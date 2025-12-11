import streamlit as st
import pandas as pd
import datetime
import numpy as np
from streamlit.web import cli as stcli
import requests
import json
from  streamlit import session_state as ss
import snowflake.connector
from streamlit_oauth import OAuth2Component
import base64
import logging
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

st.set_page_config(layout="wide")

# set up logging
# Configure the logger
log_fname = 'sms_2_way_' + datetime.datetime.now().strftime("%Y_%m_%d") + '.log'
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_fname),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

logger = logging.getLogger(__name__)




st.markdown(
    """
    <style>
    [data-testid="stElementToolbar"] {
        display: none;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("""
<style>
table th {
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)



# organize the snowflake queries here
@st.cache_data 
def snf_queries():
    # set up the snowflake credentials using Streamlit secrets
    connection_parameters = {
        'user': st.secrets["snowflake"]["user"],
        'password': st.secrets["snowflake"]["password"],
        'account': st.secrets["snowflake"]["account"],
        'role': st.secrets["snowflake"]["role"],
        'warehouse': st.secrets["snowflake"]["warehouse"],
        'database': st.secrets["snowflake"]["database"],
        'schema': st.secrets["snowflake"]["schema"],
        'insecure_mode': True  # Bypass OCSP certificate validation for Streamlit Cloud
    }

    # Use password-based authentication
    conn = snowflake.connector.connect(**connection_parameters)
    cur = conn.cursor()  
 
    # get the backlog of inbound sms
    # for now, assume a backlog of 2 weeks
    twoweeks = datetime.datetime.now() - datetime.timedelta(weeks=2)
    twoweeks = twoweeks.strftime("%Y-%m-%d")
    backlog_sql = f"""
        select 
        Null as Status,
        tph.id as touchpoint_history_id,   
        tph.id as touchpoint_history_id1,   
        c.id as case_id, 
        c.status as case_status,
        sms.sms_data:metadata.member_id::string as account_casesafe_id1,
        sms.sms_data:metadata.member_id::string as account_casesafe_id,
        sms.sms_data:metadata.client_code::string as client1,
        sms.sms_data:metadata.client_code::string as client,
        sms.sms_data:metadata.program_code::string as program1,
        sms.sms_data:metadata.program_code::string as program,
        sms.sms_data:payload:outbound:payload.body::string as message_sent1,
        sms.sms_data:payload:outbound:payload.body::string as message_sent,
        sms.sms_data:metadata.content_code::string as content_code,
        sms.sms_data:metadata.content_code::string as content_code1,
        mb.name as block_name,
        tph.touchpoint_name__c as touchpoint_name,
        sms.sms_data:payload:language::string as language,
        sms.sms_data:payload:language_written::string as language_written,
        CONVERT_TIMEZONE('UTC', 'America/New_York', sms.sms_data:metadata.created_dt::datetime) AS created_date_EST,
        sms.sms_data:metadata.created_dt::datetime as created_date,
        sms.sms_data:metadata.acknowledged = true as acknowledge_status,
        sms.sms_data:payload.Body::string as body,
        Null as outcome_code,
        null as outcome_subcode,
        null as chg_response,
        ac.firstname as account_first_name,
        ac.lastname as account_last_name, 
        c.mobile__c as phone,
        -- ac.phone ,
        coalesce(billingstreet, '') ||' '|| coalesce(billing_address_2__c, '') ||' '|| coalesce(billingcity, '') ||' '|| coalesce(billingstate, '') ||' '|| coalesce(billingpostalcode, '') as billing_address,
        county__c as county,
        ac.personbirthdate as member_dob,
        ac.member_id__c as member_id, 
        IFNULL(cs_sex__c, sex__pc) as sex,
        IFNULL(cs_gender__c, gender__pc) as gender,
        ac.primary_do_not_contact__c as do_not_contact,
        ac.primary_do_not_text__c as do_not_text
        FROM "CCP_MEMBER_INTEGRATION"."CCP_SMS_HISTORY" sms
        inner join salesforce_raw.account ac
            on sms.sms_data:metadata.member_id = ac.id
        left join salesforce_raw.member_block_touchpoint_history__c tph
            on sms.sms_data:payload.salesforce_history.body.id = tph.ssh_internal_object_link_id
        left join salesforce_raw.member_block__c mb
            on tph.member_block__c = mb.id 
        LEFT JOIN SALESFORCE_RAW.CASE c
        on touchpoint_history_id = c.touchpoint_history_id__c --account_casesafe_id = c.accountid
        where sms_data:metadata.execution = 'incoming' 
        and case_id is not null  -- uncomment for PROD
        and case_status = 'New' -- Uncomment for PROD
        and ac.test_account__c = False --  ## UCOMMENT FOR PROD
        and sms_data:metadata.created_dt > '{twoweeks}'
       -- and sms.sms_data:metadata.acknowledged = False   # UNCOMMENT FOR PROD
        order by sms_data:metadata.created_dt asc;
    """
    backlog_df = cur.execute(backlog_sql).fetch_pandas_all() 

    # do a quick mapping of language code to actual language    
    lang_map = {"en-US":"English", "ar-001":"Arabic", "es-419":"Spanish"}
    backlog_df.LANGUAGE = backlog_df.LANGUAGE.replace(lang_map)



    # also pull all the historical touchpoints for all the members in this queue
        # also pull all the historical touchpoints for all the members in this queue
    member_ids = backlog_df.ACCOUNT_CASESAFE_ID.unique()
    # deal with single member_ids
    if len(member_ids) > 1:
        member_ids = tuple(member_ids)
    else:
        member_ids =  "('" + str(member_ids[0]) + "')"
 
    history_sql = f"""
        select touchpoint_history_id, account_casesafe_id, touchpoint_name, mb.name, tph.message, tph.touchpoint_datetime, modality,touchpoint_type, outcome_code, outcome_subcode, 
        from salesforce_mart.touchpoint_history_best_result_view tph 
        inner join salesforce_raw.member_block__c mb
        on tph.member_block = mb.id
        where account_casesafe_id in {member_ids}
        and error = False
        order by account_casesafe_id, touchpoint_datetime;
    """
    history_df = cur.execute(history_sql).fetch_pandas_all()

    # and get a list of all outcome subcodes that go with inbound sms 
    subcode_sql = """
        select outcome_code__c, outcome_subcode__c, count(*) as n
        from salesforce_raw.member_block_touchpoint_history__c
        where modality__c = 'SMS'
        and outcome_code__c not like 'Outbound Call%'
        and outcome_code__c != 'Inbound SMS - Wrong Language'
        group by 1, 2
        order by 3 desc;
    """
    subcode_df = cur.execute(subcode_sql).fetch_pandas_all()

    # also pull in the list of languages here
    lang_df = pd.read_csv('languages(in).csv')

 
    return backlog_df, history_df, subcode_df, lang_df

def next_sms():
    # clears whatever session state needs to be cleared and resets everything else

    logger.info(f"CHG went to next sms from {ss.sms_idx}")

    # moves the index to the next one
    ss.sms_idx = ss.sms_idx + 1
    # checks to make sure there are still messages available
    if ss.sms_idx == len(ss.df_toshow):
        # then you are out of messages
        ss.all_done = True
    else:
        ss.all_done = False

    # resets the next step option
    # ss.next_step = None
    ss.selectbox_next_step = None
    ss.templated_responses = None
    ss.escalation_flag = False
    ss.esc_flag = False
    ss.outcome_code = None
    ss.outcome_subcode = None
    ss.subcode_dropdown = None
    ss.api_error = None
    ss.lang_update = None
    ss.pos_interaction = None
    ss.response_touse = None
    ss.other_outcome_notes = None
    ss.case_closed = False
    ss.message_sent = False
    # reprint the backlog

@st.cache_data 
def sms_api(action, member_id, message_id = None, message_text = None):
    # set up and run the sms API
    url = "https://agd9kg004g.execute-api.us-west-2.amazonaws.com/v1/sender/sms_templated_responses"  # prod
    # url = "https://pqmgc8vel6.execute-api.us-west-2.amazonaws.com/v1/sender/sms_templated_responses"  #dev

    headers = {
        'Content-Type':'application/json',
        'x-api-key':'ja2mfRpohq9hmtMri9Rp7aav9kJt6F5t9wHGHtGW'
    }


    if action == 'list':
        data = {
            "action":"list",
            "member_id":f"{member_id}"
        }
    elif (action == 'send') & (message_text == None):
        data = {
            "action": "send",
            "member_id": f"{member_id}",
            "id": f"{message_id}"
        }
        ss.case_closed = True
    elif (action == 'send') & (message_text != None):
        data = {
            "action": "send",
            "member_id":  f"{member_id}",
            "id": f"{message_id}",
            "message_source": f"{message_text}"
        }
        ss.case_closed = True

    logger.info(f"SMS/Content API payload: {data}")
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    #  print(response)

    api_output = json.loads(response.text)
    logger.info(f"SMS/Content API response: {api_output}")
    # st.write('response is: ')
    # st.write(api_output)

    return api_output


def case_account_api():
    # this calls the api to close the case and do any account updates

    # create the body for the case/account update
    api_body =   {
        "phone": f"{tmp_df.PHONE}",
        "modified_by": ss.auth,
        "case": {
            "id": f"{tmp_df.CASE_ID}",
            "outcome_code": f"{ss.outcome_code}",
            "outcome_subcode": f"{ss.outcome_subcode}", 
            "positive_interaction_notes": f"{ss.pos_interaction}",
            "other_outcome_notes": f"{ss.other_outcome_notes}"
        },
        "account": {
            "id": f"{tmp_df.ACCOUNT_CASESAFE_ID}",
            "language": f"{ss.lang_update}"
        }
    }


    # call the API to close the case 
    url = "https://agd9kg004g.execute-api.us-west-2.amazonaws.com/v1/sender/close_incoming_sms_case" # PROD
    # url = "https://pqmgc8vel6.execute-api.us-west-2.amazonaws.com/v1/sender/close_incoming_sms_case" # DEV

    headers = {
        'Content-Type':'application/json',
        'x-api-key':'ja2mfRpohq9hmtMri9Rp7aav9kJt6F5t9wHGHtGW'
    }
    logger.info(f"Case close API will run with this paylod: {api_body}")
    response = requests.post(url, headers=headers, data=json.dumps(api_body))
    
    # print(response)
    
    api_output = json.loads(response.text)
    logger.info(f"Case close API ran with this response:  {api_output}")
    # parse the response
    bulk_update = api_output['case']['bulkupdate']['Body']
    two_way = api_output['case']['remove_two_way_sms']

    # set a flag that the case is being closed
    ss.case_closed = True

    return api_output   




def account_updates():
    # this does the account updates for outcome code, subcode, etc
    ss.outcome_code = st.selectbox('Select the appropriate outcome code', list(ss.subcode_df.OUTCOME_CODE__C.unique()), index = 0) 
    subcode_list = ss.subcode_df.sort_values('OUTCOME_SUBCODE__C')
    ss.outcome_subcode = st.selectbox('Select the appropriate subcode',  [None] + list(subcode_list.OUTCOME_SUBCODE__C.unique()), key = 'subcode_dropdown') 

    # do some logic with the subcode for the account update
    if ss.outcome_subcode == 'Wrong Language':
        # provide a drop down of some languages
        # TODO - populate iwth an official list
        ss.lang_update = st.selectbox('Choose the updated language', list(lang_df.Lang.unique()))
    elif ss.outcome_code == 'Inbound SMS - Wrong Number':
        st.text_input('Update with a new phone number')

    ss.other_outcome_notes = st.text_input('Other Outcome Notes')
    ss.pos_interaction = st.text_input('Positive Interaction Notes')
    
    # # update the dataframe so that it says message done
    # backlog_idx = tmp_df['index']
    # if ss.next_step == 'Close Case w/ NO Response':
    #     ss.backlog_df.loc[backlog_idx, 'STATUS'] = 'Closed No Response'
    # else:
    #     ss.backlog_df.loc[backlog_idx, 'STATUS'] = 'Response Sent'
    # ss.backlog_df.loc[backlog_idx, 'OUTCOME_CODE'] = ss.outcome_code
    # ss.backlog_df.loc[backlog_idx, 'OUTCOME_SUBCODE'] = ss.outcome_subcode
    # # update the datafraome on top first 
    # if ss.response_touse == None:
    #     ss.backlog_df.loc[backlog_idx,'CHG_RESPONSE'] = 'Case closed, no text sent'
    # else:
    #     ss.backlog_df.loc[backlog_idx,'CHG_RESPONSE'] = ss.response_touse

def update_apptable():
    # does the table updates to make it more in sync with the Close Case API calls. Moved from account_udpates
        # update the dataframe so that it says message done
    backlog_idx = tmp_df['index']
    if ss.next_step == 'Close Case w/ NO Response':
        ss.backlog_df.loc[backlog_idx, 'STATUS'] = 'Closed No Response'
    else:
        ss.backlog_df.loc[backlog_idx, 'STATUS'] = 'Response Sent'
    ss.backlog_df.loc[backlog_idx, 'OUTCOME_CODE'] = ss.outcome_code
    ss.backlog_df.loc[backlog_idx, 'OUTCOME_SUBCODE'] = ss.outcome_subcode
    # update the datafraome on top first 
    if ss.response_touse == None:
        ss.backlog_df.loc[backlog_idx,'CHG_RESPONSE'] = 'Case closed, no text sent'
    else:
        ss.backlog_df.loc[backlog_idx,'CHG_RESPONSE'] = ss.response_touse
    
    ss.case_closed = True

##### START THE BUILD HERE ######

# Set environment variables - PROD
AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
REFRESH_TOKEN_URL = "https://oauth2.googleapis.com/token"
REVOKE_TOKEN_URL = None  # Set to None to avoid needing revocation_endpoint_auth_method
CLIENT_ID = st.secrets["oauth"]["client_id"]
CLIENT_SECRET = st.secrets["oauth"]["client_secret"]
REDIRECT_URI = st.secrets["oauth"]["redirect_uri"]
SCOPE = "openid profile email"

# ### Comment this stuff out if doing on Local Machine
if "auth" not in st.session_state:
    # create a button to start the OAuth2 flow
    oauth2 = OAuth2Component(CLIENT_ID, CLIENT_SECRET, AUTHORIZE_URL, TOKEN_URL, REFRESH_TOKEN_URL, REVOKE_TOKEN_URL)
    result = oauth2.authorize_button(
        name="Continue with Google",
        icon="https://www.google.com.tw/favicon.ico",
        redirect_uri=REDIRECT_URI,
        scope="openid email profile",
        key="google",
        extras_params={"prompt": "consent", "access_type": "offline"},
        use_container_width=True,
        pkce='S256',
    )
    
    if result:
        # st.write(result)
        # decode the id_token jwt and get the user's email address
        id_token = result["token"]["id_token"]
        # verify the signature is an optional step for security
        payload = id_token.split(".")[1]
        # add padding to the payload if needed
        payload += "=" * (-len(payload) % 4)
        payload = json.loads(base64.b64decode(payload))
        email = payload["email"]
        
        # Check if user is authorized
        allowed_users = st.secrets["authorization"]["allowed_users"]
        if email in allowed_users:
            st.session_state["auth"] = email
            st.session_state["token"] = result["token"]
            st.rerun()
        else:
            st.error(f"Access denied. User {email} is not authorized to access this application.")
            st.stop()
else:
    # Verify user is still authorized
    allowed_users = st.secrets["authorization"]["allowed_users"]
    if ss.auth not in allowed_users:
        st.error(f"Access denied. User {ss.auth} is not authorized to access this application.")
        del st.session_state["auth"]
        st.stop()
    # st.write('YOU ARE IN ELSE')
    st.write("Welcome " + ss.auth)
##### END LOCAL MACHINE COMMENT
# # comment this out if using Server
# ss.auth =  'hannah.arnson@sameskyhealth.com'
# # end server comment


#### Start getting data
# make the whole thing conditional on login

# first initialize the refresh_page session state - it starts as True for a new page
if 'refresh_page' not in ss:
    ss['refresh_page'] = True

if 'auth' not in ss:
    st.write('Please log in to continue')
else:
    # get the data
    # if this is a new page refresh, need to repull SNF data
    if ss.refresh_page == True:
        st.cache_data.clear()
    backlog_df, history_df, subcode_df, lang_df = snf_queries()

    # no longer a refresh page, set flag here
    ss.refresh_page = False
        

    # setup the session state
    if 'backlog_df' not in ss:
        ss['backlog_df'] = backlog_df

    if 'history_df' not in ss:
        ss['history_df'] = history_df

    if 'subcode_df' not in ss:
        ss['subcode_df'] = subcode_df

    if 'lang_df' not in ss:
        ss['lang_df'] = lang_df

    if 'client_code_filt' not in ss:
        ss['client_code_filt'] = None # 'All'

    if 'program_code_filt' not in ss:
        ss['program_code_filt'] = None # 'All'

    if 'lang_filt' not in ss:
        ss['lang_filt'] = None # 'All'

    if 'time_filt' not in ss:
        ss['time_filt'] = min(ss.backlog_df.CREATED_DATE)

    if 'tp_filt' not in ss:
        ss['tp_filt'] = None 

    if 'df_toshow' not in ss:
        ss['df_toshow'] = ss.backlog_df

    if 'start_button' not in ss:
        ss['start_button'] = False

    if 'sms_idx' not in ss:
        ss['sms_idx'] = 0

    if 'next_step' not in ss:
        ss['next_step'] = None

    if 'all_done' not in ss:
        ss['all_done'] = False

    if 'response_touse' not in ss:
        ss['response_touse'] = None

    if 'api_error' not in ss:
        ss['api_error'] = None

    if 'lang_update' not in ss:
        ss['lang_update'] = None
    
    if 'start_checkbox' not in ss:
        ss['start_checkbox'] = False

    if 'case_closed' not in ss:
        ss['case_closed'] = False

    if 'message_sent' not in ss:
        ss['message_sent'] = False

    # Start the build

    st.image('SameSky Health Logo Horizontal RGB.png', width = 350)
    st.title('Inbound SMS Case Queue :iphone:') 
    st.header('SMS Backlog', divider='rainbow')  

    filt1, filt2, filt3, filt4, filt5 = st.columns(5)
    with filt1: 
        cc_tmp =  st.selectbox('Client',['All']+ list(ss.backlog_df.CLIENT.unique()))
        if (ss.client_code_filt != None) and (ss.client_code_filt != cc_tmp):
            ss.start_checkbox = False
        ss.client_code_filt = cc_tmp

    with filt2:
        # dynamically filter the program codes based on the client selected 
        if ss.client_code_filt == 'All':
            programs_touse = list(ss.backlog_df.PROGRAM.unique())
        else:
            programs_touse = list(ss.backlog_df.PROGRAM[ss.backlog_df.CLIENT == ss.client_code_filt].unique())

        program_tmp = st.selectbox('Program', ['All'] + programs_touse)

        if (ss.program_code_filt != None) and (ss.program_code_filt != program_tmp):
            ss.start_checkbox = False

        ss.program_code_filt = program_tmp

    with filt3:
        # filter by touchpoint_name
        # filter by selected client and program
        if (ss.client_code_filt == 'All') & (ss.program_code_filt == 'All'):
            tps_touse =  list(ss.backlog_df.TOUCHPOINT_NAME.unique())
        elif (ss.client_code_filt != 'All') & (ss.program_code_filt == 'All'):
            tps_touse = list(ss.backlog_df.TOUCHPOINT_NAME[ss.backlog_df.CLIENT == ss.client_code_filt].unique())
        elif  (ss.client_code_filt == 'All') & (ss.program_code_filt != 'All'):
            tps_touse = list(ss.backlog_df.TOUCHPOINT_NAME[ss.backlog_df.PROGRAM == ss.program_code_filt].unique())
        else:
            tps_touse = list(ss.backlog_df.TOUCHPOINT_NAME[(ss.backlog_df.PROGRAM == ss.program_code_filt) & (ss.backlog_df.CLIENT == ss.client_code_filt)].unique())
        
        tmp_tp = st.selectbox('Touchpoint Name', ['All'] + tps_touse)
        
        if (ss.tp_filt != None) and (ss.tp_filt != tmp_tp):
            ss.start_checkbox = False 
        ss.tp_filt = tmp_tp


    with filt4:
        # filter available languages
        tmp_lang = st.selectbox('Language',['All'] + list(ss.backlog_df.LANGUAGE.unique()))
        if (ss.lang_filt != None) and (ss.lang_filt != tmp_lang):
            ss.start_checkbox = False  
        ss.lang_filt = tmp_lang
        
    with filt5:
        date_range = st.selectbox('Date Range',['Last Day', 'Last Week','Last Two Weeks']) # ['Last Two Weeks','Last Week','Last Day'])
        # map daterange to dates
        if date_range == 'Last Two Weeks':
            # tmp_date = datetime.datetime(2023, 9, 1)  ## JUST FOR DEV TESTING
            tmp_date = datetime.datetime.now() - datetime.timedelta(weeks = 2)
        elif date_range == 'Last Week':
            tmp_date = datetime.datetime.now() - datetime.timedelta(weeks = 1)
        elif date_range == 'Last Day':
            tmp_date = datetime.datetime.now() - datetime.timedelta(days = 1)
        ss.time_filt = tmp_date.strftime("%Y-%m-%d")

    # logic for filtering the df
    if ss.client_code_filt == 'All':
        client_use = ss.backlog_df.CLIENT.unique()
    else:
        client_use = [ss.client_code_filt]
    if ss.program_code_filt == 'All':
        program_use = ss.backlog_df.PROGRAM.unique()
    else:
        program_use = [ss.program_code_filt]
    if ss.tp_filt == 'All':
        tp_use = ss.backlog_df.TOUCHPOINT_NAME.unique()
    else:
        tp_use = [ss.tp_filt]
    if ss.lang_filt == 'All':
        lang_use = ss.backlog_df.LANGUAGE.unique()
    else:
        lang_use = [ss.lang_filt]
    ss.df_toshow = ss.backlog_df[(ss.backlog_df.CLIENT.isin(client_use)) & 
                                (ss.backlog_df.PROGRAM.isin(program_use)) & 
                                (ss.backlog_df.TOUCHPOINT_NAME.isin(tp_use)) & 
                                (ss.backlog_df.CREATED_DATE > ss.time_filt) &
                                (ss.backlog_df.LANGUAGE.isin(lang_use))].reset_index()

    # print the backlog dataframe
    st.dataframe(ss.df_toshow[['MEMBER_ID','STATUS','CHG_RESPONSE','CLIENT','PROGRAM','TOUCHPOINT_NAME','BODY','MESSAGE_SENT','CREATED_DATE','OUTCOME_CODE','OUTCOME_SUBCODE']], hide_index=True)

    # some logic that if the checkbox to text is unchecked, the queue counting starts over
    if ss.start_button == False:
        ss.sms_idx = 0
        ss.all_done = False 
        ss.case_closed = False
        ss.message_sent = False

    ss.start_button = st.checkbox('Check to start texting', key = 'start_checkbox')

    if (ss.start_button == True) & (ss.all_done == False):
        # now start thte texting
        st.header('Texting', divider = 'rainbow')
        st.write('Texting will start at the stop of the queue shown above')

        # need to put in a check to ensure that messages havent already been responded to
        if (ss.df_toshow.STATUS.iloc[ss.sms_idx] != None) & (ss.case_closed == False):
            # first_non_null_index = ss.backlog_df.STATUS.first_valid_index()
            first_null_index = ss.df_toshow.STATUS.isna().idxmax() if ss.df_toshow.STATUS.isna().any() else 'No New Messages'
            # if there are none, then you are done
            if first_null_index == 'No New Messages': 
                ss.all_done = True
            else:
                ss.sms_idx = first_null_index


        col1, col2, col3  = st.columns([0.3, 0.5, 0.2])
        tmp_df = ss.df_toshow.iloc[ss.sms_idx]
        
        # do a check to see if the filters are changed. If so, clear the start button
        # if (ss.client_code_filt != tmp_df.CLIENT) | (ss.program_code_filt != tmp_df.PROGRAM) | (ss.tp_filt != tmp_df.TOUCHPOINT_NAME) | (ss.lang_filt != tmp_df.LANGUAGE) :
        #     ss.start_button = False

        logger.info(f"Updating this touchpoint: {tmp_df.TOUCHPOINT_HISTORY_ID}")
        # quick Fill NA to make display eaiser
        tmp_df.fillna('None', inplace = True)
        with col1:
            # calculate the age
            today = datetime.datetime.today()

            if (tmp_df.MEMBER_DOB == 'None') |  (pd.isnull(tmp_df.MEMBER_DOB)):
                tmp_df['age'] = 'Unknown'
            else:
                tmp_df['age'] = today.year - tmp_df.MEMBER_DOB.year - ((today.month, today.day) < (tmp_df.MEMBER_DOB.month, tmp_df.MEMBER_DOB.day))
            # tmp_df['age'] = today.year - tmp_df.MEMBER_DOB.year - ((today.month, today.day) < (tmp_df.MEMBER_DOB.month, tmp_df.MEMBER_DOB.day))

            st.write('**Member Name:** ' + tmp_df.ACCOUNT_FIRST_NAME + ' ' + tmp_df.ACCOUNT_LAST_NAME)
            st.write('**Member ID:** ' + str(tmp_df.MEMBER_ID))
            st.write('**Member Age:** ' + str(tmp_df.age))
            message =  '**Message: "' + tmp_df.BODY + '"**'
            st.write(message) 
            # additioanl data
            st.write('**Date Sent:** ' + str(tmp_df.CREATED_DATE))
            st.write('**Client:** ' + tmp_df.CLIENT)
            st.write('**Program:** ' + tmp_df.PROGRAM)
            st.write('**Block Name:** ' + tmp_df.BLOCK_NAME)
            st.write('**Touchpoint Name:** ' + tmp_df.TOUCHPOINT_NAME)
            st.write('** Last Outbound Message:** "' + tmp_df.MESSAGE_SENT + '"')

            # st.dataframe(tmp_df[['CLIENT','PROGRAM','BLOCK_NAME','TOUCHPOINT_NAME','CREATED_DATE','MESSAGE_SENT']], hide_index = True) 

        with col2:
            st.write('Full Touchpoint History')
            tmp_tph = ss.history_df[ss.history_df.ACCOUNT_CASESAFE_ID == tmp_df.ACCOUNT_CASESAFE_ID]
            st.table(tmp_tph[['TOUCHPOINT_DATETIME','TOUCHPOINT_TYPE','MESSAGE']].iloc[0:10].reset_index(drop = True)) #, hide_index = True) 

        # colb1, colb2 = st.columns([0.2, 0.8])
        with col3:
            st.write('Take Action')
            ss.next_step = st.selectbox('What do you want to do?', [None,'Close Case w/ NO Response', 'Close Case & Respond'],  key = 'selectbox_next_step')

            escalation_flag =  st.checkbox('Check to view escalations data', key = 'esc_flag')
                
        if escalation_flag == True: # then show all the escalations data needed
            esc_df = tmp_df[['TOUCHPOINT_HISTORY_ID1', 'ACCOUNT_CASESAFE_ID1','CLIENT1','PROGRAM1','MESSAGE_SENT1','CONTENT_CODE1',
                'TOUCHPOINT_HISTORY_ID','ACCOUNT_CASESAFE_ID','CLIENT','PROGRAM','MESSAGE_SENT','CONTENT_CODE','CREATED_DATE_EST',
                'CREATED_DATE','ACKNOWLEDGE_STATUS','BODY','ACCOUNT_FIRST_NAME','ACCOUNT_LAST_NAME','PHONE',
                'BILLING_ADDRESS','COUNTY','MEMBER_DOB','BLOCK_NAME','TOUCHPOINT_NAME','MEMBER_ID','SEX','GENDER','DO_NOT_CONTACT','DO_NOT_TEXT']].to_frame().T
            st.write('##### Escalations Data')
            st.write('Hit shift + arrow to highlight and copy')
            st.dataframe(esc_df, hide_index=True)
            

            

        if ss.next_step == 'Close Case w/ NO Response':
            st.header('Close the case with no response needed')  
            account_updates()
            # update_apptable()
            # subcode = st.selectbox('Select the appropriate subcode', ss.subcode_df.OUTCOME_SUBCODE__C.unique())

            # # update the dataframe so that it says message skipped
            # backlog_idx = tmp_df['index']
            # ss.backlog_df.loc[backlog_idx, 'STATUS'] = 'No action needed'
            # ss.backlog_df.loc[backlog_idx, 'OUTCOME_CODE'] = 'Inbound SMS'
            # ss.backlog_df.loc[backlog_idx, 'OUTCOME_SUBCODE'] = subcode
            # # ss.df_toshow.loc[ss.sms_idx,'STATUS'] = 'No action needed'

            # ss.df_toshow.STATUS.iloc[ss.sms_idx] = 'No action needed'
            if (ss.outcome_code == 'Inbound SMS' ) & ((ss.outcome_subcode == None) | (ss.outcome_code == 'Inbound SMS - Wrong Number')):
                st.warning('Please enter in a subcode to close the case',icon="⚠️")

            if (ss.outcome_subcode != None) | (ss.outcome_code == 'Inbound SMS - Wrong Number'):    
                logger.info(f"Case updated with no response - outcome code {ss.outcome_code}, outcome subcode {ss.outcome_subcode}")
                if st.button('Close the case', on_click=update_apptable()):
                    # st.write('HEY TONY, CLOSE THE CASE!')
                    # update_apptable()
                    # account_updates() -- have the table update here
                    response = case_account_api()
                    st.write('Case closed')
                    # st.write('API says: ' + str(response))


                if st.button('Go to the next message', on_click=next_sms):
                    st.rerun()


        elif ss.next_step == 'Close Case & Respond':
            st.header('Response time!')  
            # call Tony's API to get the list of available repsonses

            member_id = tmp_df.ACCOUNT_CASESAFE_ID
            responses = sms_api('list',member_id)
            # put some error handling in here
            if 'status' in responses.keys():
                # then we have an error
                if (responses['status'] == 422) or (responses['status'] == 404):
                    #DNC
                    st.write(responses['message'])
                    # TODO - find out what to do in terms of account updates
                    ss.api_error = 'do_not_contact'
                    st.warning('Please close the case without responding')
                    # if st.button('Go to the next message', on_click=next_sms):
                    #     st.rerun()          
            else:
                ss.api_error = None


            if ss.api_error == None:
            # parse the response json
                topics = responses['collection']
                subjects = []
                message_ids = [] 
                tp_code = []
                message_name = []
                message_source = []
                for i in np.arange(0, len(topics)):
                    tmp_dict = topics[i]
                    # itterate over messages
                    for mi in np.arange(0, len(tmp_dict['messages'])):
                        subjects.append(tmp_dict['name'])
                        message_ids.append(tmp_dict['messages'][mi]['id'])
                        tp_code.append(tmp_dict['messages'][mi]['touchpoint_code'])
                        message_name.append(tmp_dict['messages'][mi]['name'])
                        message_source.append(tmp_dict['messages'][mi]['message_source'])
                # put into a dataframe
                response_df = pd.DataFrame({'subjects':subjects, 
                                            'message_ids':message_ids,
                                            'touchpoint_code':tp_code,
                                            'message_name':message_name,
                                            'message_source':message_source
                                            })
                

                # now display subjects -> message name & message source
                
                cols1, cols2, cols3 = st.columns(3)
                with cols1:
                    subject_touse = st.selectbox('Available Templated Response Subjects:', list(response_df.subjects.unique()), key = 'templated_response_subjects')
                    msgs_df = response_df[response_df.subjects == subject_touse] 
                    
                    msg_name = st.selectbox('Available Message Names', [None] + list(msgs_df.message_name.unique()), key = 'templated_response_names') 

                    if ss.templated_response_names != None:
                
                        sub_df = msgs_df[msgs_df.message_name == msg_name]
                        msg_txt = st.selectbox('Choose the message', list(sub_df.message_source), key = 'templated_messages')

        
                        ss.response_id = msgs_df.message_ids[(msgs_df.message_name == msg_name) & (msgs_df.message_source == msg_txt)].unique()[0]
                        ss.response_touse = msgs_df.message_source[msgs_df.message_ids == ss.response_id].unique()[0]

                        if ss.response_touse != None:
                            st.write('Selected Message: ' + ss.response_touse)

                        if st.checkbox('click to edit response', key = 'edit_box'):
                            ss.response_touse = st.text_area('Edit Response', ss.response_touse) 

                
                        with cols2:
                            account_updates()
                            if (ss.outcome_code == 'Inbound SMS' ) & ((ss.outcome_subcode == None) | (ss.outcome_code == 'Inbound SMS - Wrong Number')):
                            # if (ss.outcome_code == 'Inbound SMS' ) & (ss.outcome_subcode == None):
                                st.warning('Please enter in a subcode to close the case',icon="⚠️")

                        with cols3:
                            if (ss.response_touse != None) & ((ss.outcome_subcode != None) | (ss.outcome_code == 'Inbound SMS - Wrong Number')):
                                st.write('Selected Response: ' + ss.response_touse)
                                if st.button('Click to send the above response and close the case', on_click=update_apptable()):
                                    logger.info(f"Case updated with a response - outcome code {ss.outcome_code}, outcome subcode {ss.outcome_subcode}, response {ss.response_touse}")
                                    # call the send API
                                    # was the message edited? 
                                    if ss.response_touse != msgs_df.message_source[msgs_df.message_ids == ss.response_id].unique()[0]:
                                        message_text = ss.response_touse
                                    else:
                                        message_text = None
                                        
                                    sent_output = sms_api('send', member_id, ss.response_id, message_text)
                                    update_apptable()

                                    st.write('Message Sent! ' + ss.response_touse + ' :tada:')
                                    ss.message_sent = True

                                    response_caseaccount = case_account_api()
                                    st.write('Account updated! ') #  + str(response_caseaccount))

                                if ss.message_sent == True:
                                    if st.button('Go to the next message', on_click=next_sms):
                                        st.rerun()
                                

        elif (ss.start_button == True) & (ss.all_done == True):
            st.header('You have responded to all the text messages! :heavy_check_mark:')
            st.balloons()
    
    # an option to logout
    st.header('Logout', divider='rainbow')
    if st.button("Logout"):
        # clear the cache and make it rerun the snowflake queries
        st.cache_data.clear()
        del st.session_state["auth"]
        del st.session_state["token"]
