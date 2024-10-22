from datetime import datetime, timedelta
import pyodbc
import pandas as pd
import requests
import json
import os
import logging
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import numpy as np
import math
import time
import io
import argparse
import os
from dotenv import load_dotenv
import warnings
import pytz
import logging
from logging.handlers import RotatingFileHandler
from send_email_app import send_email_notification

warnings.filterwarnings("ignore")

load_dotenv(override=True)
#Web App and App registration Details
tenant_id = os.getenv('DV_TENANT_ID')
client_id = os.getenv('DV_CLIENT_ID')
client_secret = os.getenv('DV_CLIENT_SECRET')


account_name = os.getenv('CSV_ACCOUNT_NAME')
account_key = os.getenv('CSV_ACCOUNT_KEY')
container_name = os.getenv('CSV_CONTAINER_NAME')

ALARM_DIR = os.getenv('ALARM_DIR')
column_names = ['Date','Time','Acknowledgement','Alarm Type','Alarm Limit','Priority','Alarm','Alarm Value','Alarm Comment','Resolution']

log_dir = os.getenv('ALARM_LOG_DIR')
log_file_name = 'alarm_import.log'
log_path = log_dir+log_file_name
if not os.path.exists(os.path.dirname(log_path)):
    os.makedirs(os.path.dirname(log_path))
# Configure the logger
max_bytes = 10 * 1024 * 1024  # 10 MB
backup_count = 30            
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,backupCount=backup_count
        )
# Define the log format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# Remove any existing handlers
if logger.hasHandlers():
    logger.handlers.clear()
# Add the handler to the logger
logger.addHandler(handler)



def get_access_token(tenant_id,client_id,client_secret,env_url):
    access_token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    access_token_payload = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={env_url}/.default'
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    access_token_response = requests.request("POST", access_token_url, headers=headers, data=access_token_payload, verify=False)
    access_token_response_json = json.loads(access_token_response.text)
    access_token = access_token_response_json['access_token']
    return access_token


def get_source_data_from_azure(gas_date):
    gas_date_obj = datetime.strptime(gas_date,"%Y-%m-%d")
    file_name = gas_date_obj.strftime("%y%m%d10")
    blob_file_name = f"{ALARM_DIR}/{file_name}.CSV"
    source_df = get_blob_csv_file_as_df(blob_file_name)
    if source_df.empty:
        raise Exception(f"Alarm values are not present")
    return source_df

def preprocess_csv(stream, expected_cols, delimiter=','):
    lines = stream.read().decode('utf-8').splitlines()  
    processed_rows = []

    for line in lines:
        cols = line.split(delimiter)

        if len(cols) < expected_cols:
            cols += [''] * (expected_cols - len(cols))
        elif len(cols) > expected_cols:
            cols = cols[:expected_cols]
        new_row=delimiter.join(cols)
        processed_rows.append(new_row)

    processed_stream = io.StringIO("\n".join(processed_rows))
    return processed_stream

def get_blob_csv_file_as_df(blob_file_name):
    # Construct the connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # build the file name & create a blob client
    # os.path.join('readings', active_gas_date.strftime('%y%m%d') + '.csv')
    logger.info(f"blob_file_name: {blob_file_name}")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file_name)

    # check if the file exists
    if not blob_client.exists():
        msg = f'File does not exist '
        logger.error(msg)
        raise FileNotFoundError(msg)
    

    try:
        
        # Download the blob data to a stream
        download_stream = blob_client.download_blob()
        stream = BytesIO(download_stream.readall())
        stream=preprocess_csv(stream, len(column_names), delimiter=',')
        df = pd.read_csv(stream,header=None, skip_blank_lines=True,on_bad_lines='warn')
        return df
    except Exception as e:  
        logger.error(f"Error processing blob: {e}")
        
        
def insert_bulk_payload(web_api_url, access_token, table_name, singular_table_name, payloads): 
    logger.info(f"Running Bulk Insert")
    exception_counter = 0
    result={"status":"","success_count":0,"fail_count":0,"failed_records":[],"total_count":len(payloads)}
    key_to_add = "@odata.type"
    value_to_add = f"Microsoft.Dynamics.CRM.{singular_table_name}"
    payloads = {"Targets":[{**d, key_to_add: value_to_add} for d in payloads]}
    try:
        create_response  = create_bulk_record( web_api_url, access_token, table_name, payloads=payloads)
        if create_response.get("status")=="success":
                result = create_response
                return result
    except Exception as e:
        logger.error(f"Exception occured for bulk create for {table_name}: {e}")
        result = {"status":"fail"}
        return result
def create_bulk_record( web_api_url, access_token, table_name,payloads):
    logger.info(f"Running Bulk Create API call")
    url = f"{web_api_url}/{table_name}/Microsoft.Dynamics.CRM.CreateMultiple"
    headers = {
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Prefer':'return=representation'
    }
        
    try:
        response = requests.request("POST", url, headers=headers, data=json.dumps(payloads), verify=False)
        response.raise_for_status()  # Raise HTTPError for bad responses

        if response.status_code in [200,201,204]:
            response_json = response.json()
            if response_json:
                return {'status':"success",'ids':response_json.get("Ids"), "count": len(response_json.get("Ids")) }

    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        logger.error(f"JSON Decode Error: {e}")
        raise
        
    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors (non-200 status codes)
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        raise
    except requests.exceptions.RequestException as e:
        # Handle connection errors, timeouts, and general request exceptions
        logger.error(f"Request Exception: {e}")
        raise  # Re-raise the exception to propagate it further        
    except Exception as e:
        # Handle other unexpected exceptions
        logger.error(f"Unexpected Error: {e}")
        raise Exception(f"An unexpected error occurred : {e}")
    


def transform_alarm(df, gas_date):
    df = df.dropna(how='all')
    df.columns = column_names
    num_col = len(column_names)
    total_col = 10
    df['DateTime'] = df["Date"] + " " + df["Time"]
    # logger.info(df.head(30))
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%d %b %Y %H:%M:%S.%f',errors='coerce')
    df.dropna(subset=['DateTime'],inplace=True)
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df.drop(columns=['Date','Time'],inplace=True)
    # df = pd.melt(df, id_vars='TIME', var_name='Name', value_name='Value')    
    df_sorted = df.sort_values(by='DateTime')

    # Get the lowest and highest times
    lowest_time = df_sorted['DateTime'].min()
    highest_time = df_sorted['DateTime'].max()
    logger.info(f"Starts from time {lowest_time}")
    logger.info(f"Ends at time {highest_time}")
    return df, lowest_time, highest_time

def create_record( web_api_url, access_token, table_name,payload, id_col):
    url = f"{web_api_url}/{table_name}?$select={id_col}"
    logger.info(f"URL: {url}")
    headers = {
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}',
    'Prefer':'return=representation'
    }
        
    try:
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload), verify=False)
        response.raise_for_status()  # Raise HTTPError for bad responses

        if response.status_code in [200,201,204]:
            response_json = response.json()
            if response_json:
                return {'status':"success",'id':response_json.get(id_col) }

    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        logger.error(f"JSON Decode Error: {e}")
        raise
        
    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors (non-200 status codes)
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        raise
    except requests.exceptions.RequestException as e:
        # Handle connection errors, timeouts, and general request exceptions
        logger.error(f"Request Exception: {e}")
        raise  # Re-raise the exception to propagate it further        
    except Exception as e:
        # Handle other unexpected exceptions
        logger.error(f"Unexpected Error: {e}")
        raise Exception(f"An unexpected error occurred : {e}")
    

def get_table_details(web_api_url,access_token, table_name, id_col, name_col, url="" ):
    if url=="":
        url = f"{web_api_url}/{table_name}?$select={id_col},{name_col}"
    logger.info(f"URL: {url}")
    payload = {}
    headers = {
      'OData-MaxVersion': '4.0',
      'OData-Version': '4.0',
      'Content-Type': 'application/json; charset=utf-8',
      'Accept': 'application/json',
      'Authorization': f'Bearer {access_token}'
    }
    try:
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        response.raise_for_status()  # Raise HTTPError for bad responses

        if response.status_code in [200,201,204]:
            response_json = response.json()
            response_value = response_json.get("value")
            if response_value:
                return {'status':"success",'value':response_value }

    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        logger.error(f"JSON Decode Error: {e}")
        raise
        
    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors (non-200 status codes)
        logger.error(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        raise
    except requests.exceptions.RequestException as e:
        # Handle connection errors, timeouts, and general request exceptions
        logger.error(f"Request Exception: {e}")
        raise  # Re-raise the exception to propagate it further        
    except Exception as e:
        # Handle other unexpected exceptions
        logger.error(f"Unexpected Error: {e}")
        raise Exception("An unexpected error occurred")
    

def split_into_chunks(payload_list, splits=5):    
    split_list = [chunk.tolist() for chunk in np.array_split(payload_list, splits)]
    split_list = [i for i in split_list if i !=[]]
    return split_list

def insert_alarm_readings(web_api_url, access_token, env_url, payloads):#, existing_gas_days):
    singular_table_name = "gas_alarm"
    table_name="gas_alarms"
    id_col="gas_alarmid"
    exception_counter = 0
    # result={"status":"","success_count":0,"fail_count":0,"skipped_count":0,"total_count":len(payloads)}
    fail = False
    total_len=len(payloads)
    count_per_batch = 5000
    splits = math.ceil(total_len / count_per_batch)
    logger.info(f"Insert List\nSplits: {splits}; ")
    try:
        payload_batches = split_into_chunks(payloads, splits=splits)
        for idx, payload_batch in enumerate(payload_batches):
            logger.info(f"Batch Run {idx+1}\n{len(payload_batch)}")
            result_temp = insert_bulk_payload(web_api_url, access_token, table_name, singular_table_name, payloads=payload_batch)
            if result_temp["status"]=="fail":
                logger.error(f"Batch {idx+1} failed")
                fail=True
                break
            access_token = get_access_token(tenant_id,client_id,client_secret,env_url)
        if fail:
            logger.error(f"Issue in writing to dataverse in batches")
            raise Exception(f"Issue in writing to dataverse in batches")
        logger.info(f"Alarm values are successfully written to dataverse")
    except Exception as e:
        logger.error(f"Exception occured while writing to dataverse : {e}")
        raise Exception(f"Exception occured while writing to dataverse : {e}")

def insert_to_dataverse(web_api_url, access_token, env_url, gas_date, processed_df):
    payloads = []
    for index, rec in processed_df.iterrows():
        payloads.append({
                    "gas_datetime":rec['DateTime'] if not pd.isna(rec['DateTime'])  else None,
                    "gas_acknowledgement":str(rec["Acknowledgement"]) if not pd.isna(rec['Acknowledgement']) else None,
                    "gas_alarmtype":str(rec['Alarm Type']) if not pd.isna(rec['Alarm Type']) else None,
                    "gas_alarmlimit":str(rec['Alarm Limit']) if not pd.isna(rec['Alarm Limit']) else None,
                    "gas_priority":int(rec['Priority']) if not pd.isna(rec['Priority']) else None,
                    "gas_alarm":str(rec["Alarm"]) if not pd.isna(rec['Alarm']) else None,
                    "gas_alarmvalue":str(rec['Alarm Value']) if not pd.isna(rec['Alarm Value']) else None,
                    "gas_alarmcomment":str(rec['Alarm Comment']) if not pd.isna(rec['Alarm Comment']) else None,
                    "gas_resolution":str(rec['Resolution']) if not pd.isna(rec['Resolution']) else None
                            })
    logger.info(f"Total Records {len(payloads)}")
    insert_alarm_readings(web_api_url, access_token, env_url, payloads)#,existing_gas_days)

def get_table_record_ids( access_token, table_name,id_col, url):
    try:
        payload = {}
        headers = {
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0',
        'Content-Type': 'application/json; charset=utf-8',
        'Accept': 'application/json',
        'Authorization': f'Bearer {access_token}'
        }
        response = requests.request("GET", url, headers=headers, data=payload, verify=False)
        if(response.status_code==200):
            values = json.loads(response.text)['value']
            id_list=[]
            if values:
                for val in values:
                    id_list.append(val[id_col])
            return {'status':"success",'ids':id_list, 'table_name':table_name,'id_col':id_col, }
        else:
            raise Exception(f"Issue Obtaining Table Record Ids: {json.loads(response.text)}")
            # return {'status':'fail'}
    except Exception as e:
            raise Exception(f"Exception occurred while obtaining Table Record Ids: {e}")
            # return {'status':'fail'}
def find_and_del_alarm(web_api_url, access_token, lowest_time, highest_time):
    try:
        table_name="gas_alarms"
        id_col="gas_alarmid"
        date_filter = f"&$filter=gas_datetime ge {lowest_time} and gas_datetime le {highest_time}"
        url = (
            f"{web_api_url}/{table_name}?$select={id_col}"
            f"{date_filter}"
        )
        iteration = 0
        logger.info("Find and Delete Alarms")
        delete_results ={"Total Iterations":None,"Delete Status":None, "Detailed Results":{"Iteration":{}}}
        while True:
            iteration+=1
            # url = f"{web_api_url}/{table_name}?$select={id_col}&$filter=gas_GasDay/gas_date eq {gas_date}"
            record_ids_result = get_table_record_ids( access_token, table_name, id_col, url)
            if record_ids_result['status']=='success':
                record_ids=record_ids_result['ids']
                to_delete_count = len(record_ids)
                if iteration==1 and to_delete_count<=0:
                    logger.info("No Records present to delete")
                    break
                elif to_delete_count<=0:
                    logger.info("No more records to delete")
                    break
                else:
                    logger.info(f"Iteration: {iteration}")
                    logger.info(f"     Performing Delete")
                    success_batch_in_itr=0
                    logger.info(f"     Number of Records to delete: {to_delete_count}")
                    total_len=len(record_ids)
                    count_per_batch = 1000
                    splits = math.ceil(total_len / count_per_batch)
                    logger.info(f"      Batches: {splits}; ")
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]={"Records to Delete":None,"Total Batches":None,"Batch":{},"Delete Status":None}
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Records to Delete"]=to_delete_count
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Total Batches"]=splits

                    ids = split_into_chunks(record_ids, splits=splits)
                    
                    for batch, id_list in enumerate(ids):
                        delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Batch"][f"{batch}"]={"Delete Status":None}
                        logger.info(f"     Performing delete for batch: {batch+1}")
                        result = bulk_delete(web_api_url, access_token, table_name, id_list)
                        if result['status']=="success":
                            success_batch_in_itr+=1
                            delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Batch"][f"{batch}"]["Delete Status"]="success"
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Delete Status"]="success"
        delete_results["Total Iterations"]=iteration-1
        delete_results["Delete Status"]="success"
        return delete_results
    except Exception as e:
        raise Exception(f"Error in deleting alarm records {e}")
# def find_and_del_alarm(web_api_url, access_token, gas_date):
#     table_name="gas_alarms"
#     get_all_alarm_ids(web_api_url, access_token, table_name, gas_date)
#         to_delete_count = len(record_ids)
#         if to_delete_count==0:
#             alarms_present = False
#         deleted_count=0
#         delete_results=[]
#         logger.info(f"Number of Records to delete: {to_delete_count}")
#         total_len=len(record_ids)
#         count_per_batch = 1000
#         splits = math.ceil(total_len / count_per_batch)
#         logger.warning(f"Delete List\nSplits: {splits}; ")

#         ids = split_into_chunks(record_ids, splits=splits)

#         # logger.warn(ids)
#         for index, id_list in enumerate(ids):
#             logger.info(f"Performing delete for row: {index+1}")
#             result = bulk_delete(web_api_url, access_token, table_name, id_list)
#             result['row_number']=index+1
#             delete_results.append(result)
#             if result['status']=="success":
#                 deleted_count+=1 
#         undeleted = to_delete_count-deleted_count
#         if undeleted==0:
#             return {'status':'success',  "to_delete":to_delete_count, "deleted": deleted_count,'undeleted':undeleted, 'delete_results':delete_results}
#         if undeleted==to_delete_count:
#             return {'status':'fail', 'message':'None deleted', "to_delete":to_delete_count, "deleted": deleted_count,'undeleted':undeleted, 'delete_results':delete_results}
#         else:
#             return {'status':'fail','message':'Some deleted',  "to_delete":to_delete_count, "deleted": deleted_count,'undeleted':undeleted, 'delete_results':delete_results}
    
def delete_existing_alarm(web_api_url, access_token, env_url, lowest_time, highest_time):
    # del_result = find_and_del_alarm(web_api_url, access_token, gas_date)
    try:
        start_time = time.time()
        # logger.info(f"Starting Time for Delete Job {start_time}")
        del_result = find_and_del_alarm(web_api_url, access_token, lowest_time, highest_time)
        end_time = time.time()
        time_taken = end_time - start_time
        # logger.info(f"Ending Time for Delete Job {end_time}")
        logger.info(f"Time taken to run the function: {time_taken:.4f} seconds")
        logger.info(f"Successfully deleted existing alarms\n{json.dumps(del_result,indent=4)}")
        # if del_result["status"]=="success":
        #     if del_result["to_delete"]==0:
        #         logger.info(f"No records to delete for {gas_date}")
        #     else:
        #         logger.info(f"Existing alarm for {gas_date} are deleted before import - {del_result}")
        # else:
        #     raise Exception(f"Unable to delete exisiting alarm values from the table for {gas_date} - {del_result}")
    except Exception as e:
        logger.error(f"Error in deleting existing alarm : {e}")
        raise  

def delete_record(web_api_url, access_token, table_name, id):
    url = f"{web_api_url}/{table_name}({id})"
    payload = {}
    headers = {
    'If-Match':'*',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Authorization': f'Bearer {access_token}'
    }
    response = requests.request("DELETE", url, headers=headers, data=json.dumps(payload), verify=False)
    if response.status_code ==204:
        return {"status":"success",'table_name':table_name,'id':id}
    else:
        return {'status':"error",'response':response, "reason":response.reason, "error_msg":json.loads(response.text)}
    


# def bulk_delete(web_api_url, access_token):
#     logger.warn("BULK DELETING")

#     # Constants
#     BULK_DELETE_URL = f"{web_api_url}/api/data/v9.2/BulkDelete"
#     HEADERS = {
#         'Authorization': f'Bearer {access_token}',
#         'Content-Type': 'application/json; charset=utf-8',
#         'OData-Version': '4.0'
#     }

#     # Define the body of the request
#     body = {
#         "QuerySet": [
#             {
#                 "EntityName": "alarms",
#                 "DataSource": "retained",
#                 "Criteria": {
#                     "FilterOperator": "And",
#                     "Conditions": [
#                         {
#                             "AttributeName": "datetime",
#                             "Operator": "Equal",
#                             "Values": [{"Value": "10/1/2024 12:46 PM", "Type": "System.String"}]
#                         }
#                     ]
#                 }
#             }
#         ],
#         "JobName": "Bulk Delete Retained Contacts",
#         "SendEmailNotification": False,
#         "RecurrencePattern": "",
#         "StartDateTime": "2023-03-07T05:00:00Z",
#         "ToRecipients": [],
#         "CCRecipients": []
#     }

#     # Send the POST request
#     response = requests.post(BULK_DELETE_URL, headers=HEADERS, data=json.dumps(body))

#     # Check response
#     if response.status_code == 202:
#         logger.info("Bulk delete request sent successfully.")
#     else:
#         logger.error(f"Error: {response.status_code} - {response.reason} {response.text}")

def bulk_delete(web_api_url, access_token, table_name, ids):
    try:
        import uuid

        logger.info("      BULK DELETING")
        # Constants
        boundary = f"batch_{uuid.uuid4()}"  # Use a unique boundary string

        DATAVERSE_URL = web_api_url
        BATCH_URL = f'{DATAVERSE_URL}/$batch'
        HEADERS = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': f'multipart/mixed; boundary={boundary} ',
            'OData-Version': '4.0'
        }

        # List of IDs to delete
        ids_to_delete = ids

        # Create the batch request body
        batch_body = f"--{boundary}\n"

        for entity_id in ids_to_delete:
            batch_body += (
                f"Content-Type: application/http\n"
                f"Content-Transfer-Encoding: binary\n\n"
                f"DELETE {DATAVERSE_URL}/{table_name}({entity_id}) HTTP/1.1\n"
                f"Accept: application/json\n\n"
                f"\n"
            )
            batch_body += f"--{boundary}\n"  # Separate each request with the boundary
        # batch_body += f"--{boundary}--"  
        # logger.info(batch_body)
        # Send the batch request
        response = requests.post(BATCH_URL, headers=HEADERS, data=batch_body, verify=False)

        # Check response
        if response.status_code in [200,202,204]:
            logger.info("      Batch delete request sent successfully.")
            return {"status":"success"}
        else:
            raise Exception(f"Error in deleting alarms in bulk: {response.status_code} - {response.reason} ")
    except Exception as e:
        raise Exception(f"Exception occured while deleting alarms in bulk: {e}")

def alarm_import(gas_date:str, env:str):
    logger.info("***************START*********************")
    eastern = pytz.timezone('US/Eastern')
    job_start_time_est = datetime.now(eastern)
    logger.info(f"Starting the scan and upload job at {job_start_time_est}...")
    if gas_date=='auto':
        gas_date = job_start_time_est - timedelta(days=1)
        gas_date = gas_date.strftime('%Y-%m-%d')
    try:
        if env=="dev":
            env_url=os.getenv("DV_ENV_URL")
        elif env=="stage":
            env_url=os.getenv("DV_STAGE_ENV_URL")
        web_api_url = f"{env_url}/api/data/v9.2"
        access_token = get_access_token(tenant_id,client_id,client_secret,env_url)
        logger.info(f"ALARM IMPORT for {gas_date}")
        logger.info(f"Getting source data for {gas_date}")
        source_df = get_source_data_from_azure(gas_date)
        logger.info(f"Transforming data")
        processed_df, lowest_time, highest_time = transform_alarm(source_df, gas_date)
        delete_existing_alarm(web_api_url, access_token, env_url, lowest_time, highest_time)
        logger.info(f"Writing data to dataverse")
        insert_to_dataverse(web_api_url, access_token, env_url, gas_date, processed_df)
        logger.info(f"Alarm Import Successfull")
        logger.info(f"IMPORT FOR {gas_date} SUCCESSFULL")
        print(f"IMPORT FOR {gas_date} SUCCESSFULL")
    except Exception as e:
        logger.error(f"IMPORT FOR {gas_date} UNSUCCESSFULL {e}")
        print(f"IMPORT FOR {gas_date} UNSUCCESSFULL {e}")
        subject="Error: Issue in importing Alarm Data to Azure Storage"
        body=f"Alarm Import Job Failed \n\nJob Start Time: \n{job_start_time_est} \n\nError:\n\n {e}\n"
        send_email_notification(subject, body, logger)
    finally:
        logger.info("***************END*********************")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Alarm Import")
    parser.add_argument('--gas_date', type=str, help='The date you want to import in yyyy-mm-dd format; If left empty, program will import yesterday\'s alarms',default='auto')
    parser.add_argument('--env', choices=['dev', 'stage'], help='Environment', default='dev')
    args = parser.parse_args()
    alarm_import(args.gas_date, args.env)