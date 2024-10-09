from datetime import datetime, timedelta
import pyodbc
import pandas as pd
import requests
import json
import os
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import numpy as np
import math
import time
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

FLOW_DIR = os.getenv('FLOW_DIR')
# FLOW_FILE = os.getenv('FLOW_FILE')
log_dir = os.getenv('FLOW_LOG_DIR')
log_file_name = 'flow_import.log'
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
    file_name = gas_date_obj.strftime("%y%m%d")
    blob_file_name = f"{FLOW_DIR}/{file_name}.csv"
    source_df = get_blob_csv_file_as_df(blob_file_name)
    if source_df.empty:
        raise Exception(f"Flow values are not present")
    # logger.info(f"source_df: {source_df.head(1)}")
    return source_df

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
        df = pd.read_csv(stream)
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
    


def transform_flow(df, gas_date):
    df = df.dropna(axis=1, how='all')
    logger.info(f"Total Columns {len(df.columns)}")
    logger.info(f"Total Rows {df.shape[0]}")
    df['TIME'] = gas_date + " " + df["TIME"]
    df['TIME'] = pd.to_datetime(df['TIME'], format='%Y-%m-%d %I:%M:%S %p').dt.tz_localize('UTC')
    start_time = pd.to_datetime('2024-10-02T00:00:00Z')
    end_time = pd.to_datetime('2024-10-02T10:00:00Z')
    mask = (df['TIME'] >= start_time) & (df['TIME'] < end_time)
    df.loc[mask, 'TIME'] += pd.Timedelta(days=1)
    df['TIME'] = df['TIME'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    df = pd.melt(df, id_vars='TIME', var_name='Name', value_name='Value')   
    df_sorted = df.sort_values(by='TIME')
    lowest_time = df_sorted['TIME'].min()
    highest_time = df_sorted['TIME'].max() 
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

def insert_flow_readings(web_api_url, access_token, env_url, payloads):#, existing_gas_days):
    singular_table_name = "gas_flow"
    table_name="gas_flows"
    id_col="gas_flowid"
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
        logger.info(f"Flow values are successfully written to dataverse")
        if fail:
            logger.error(f"Issue in writing to dataverse in batches")
            raise Exception(f"Issue in writing to dataverse in batches")
    except Exception as e:
        logger.error(f"Exception occured while writing to dataverse : {e}")
        raise Exception(f"Exception occured while writing to dataverse : {e}")

def insert_to_dataverse(web_api_url, access_token, env_url, gas_date, processed_df):
    payloads = []
    for index, rec in processed_df.iterrows():
        payloads.append({
                    "gas_datetime":rec['TIME'],
                    "gas_name":rec["Name"],
                    "gas_value":float(rec['Value'])
    })
    logger.info(f"Number of Records: {len(payloads)}")
    insert_flow_readings(web_api_url, access_token, env_url, payloads)#,existing_gas_days)

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
def find_and_del_flow(web_api_url, access_token, lowest_time, highest_time):
    try:
        table_name="gas_flows"
        id_col="gas_flowid"
        date_filter = f"&$filter=gas_datetime ge {lowest_time} and gas_datetime le {highest_time}"
        url = (
            f"{web_api_url}/{table_name}?$select={id_col}"
            f"{date_filter}"
        )
        iteration = 0
        logger.info("Find and Delete Flows")
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
                    logger.info(f"     Batches: {splits}; ")
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
        raise Exception(f"Error in deleting flow records {e}")
# def find_and_del_flow(web_api_url, access_token, gas_date):
#     table_name="gas_flows"
#     get_all_flow_ids(web_api_url, access_token, table_name, gas_date)
#         to_delete_count = len(record_ids)
#         if to_delete_count==0:
#             flows_present = False
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
    
def delete_existing_flow(web_api_url, access_token, env_url, lowest_time, highest_time):
    # del_result = find_and_del_flow(web_api_url, access_token, gas_date)
    try:
        start_time = time.time()
        # logger.info(f"Starting Time for Delete Job {start_time}")
        del_result = find_and_del_flow(web_api_url, access_token, lowest_time, highest_time)
        end_time = time.time()
        time_taken = end_time - start_time
        # logger.info(f"Ending Time for Delete Job {end_time}")
        logger.info(f"Time taken to run the function: {time_taken:.4f} seconds")
        logger.info(f"Successfully deleted existing flows\n{json.dumps(del_result,indent=4)}")
        # if del_result["status"]=="success":
        #     if del_result["to_delete"]==0:
        #         logger.info(f"No records to delete for {gas_date}")
        #     else:
        #         logger.info(f"Existing flow for {gas_date} are deleted before import - {del_result}")
        # else:
        #     raise Exception(f"Unable to delete exisiting flow values from the table for {gas_date} - {del_result}")
    except Exception as e:
        logger.error(f"Error in deleting existing flow for : {e}")
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
#                 "EntityName": "flows",
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
            raise Exception(f"Error in deleting flows in bulk: {response.status_code} - {response.reason} ")
    except Exception as e:
        raise Exception(f"Exception occured while deleting flows in bulk: {e}")

def flow_import(gas_date:str, env:str):

    try:
        logger.info("***************START*********************")
        eastern = pytz.timezone('US/Eastern')
        job_start_time_est = datetime.now(eastern)
        logger.info(f"Starting the scan and upload job at {job_start_time_est}...")
        if gas_date=='auto':
            gas_date = job_start_time_est - timedelta(days=1)
            gas_date = gas_date.strftime('%Y-%m-%d')
        if env=="dev":
            env_url=os.getenv("DV_ENV_URL")
        elif env=="stage":
            env_url=os.getenv("DV_STAGE_ENV_URL")
        web_api_url = f"{env_url}/api/data/v9.2"
        access_token = get_access_token(tenant_id,client_id,client_secret,env_url)
        logger.info(f"FLOW IMPORT for {gas_date}")
        logger.info(f"Getting source data for {gas_date}")
        source_df = get_source_data_from_azure(gas_date)
        logger.info(f"Transforming data")
        processed_df, lowest_time, highest_time = transform_flow(source_df, gas_date)
        delete_existing_flow(web_api_url, access_token, env_url, lowest_time, highest_time)
        logger.info(f"Writing data to dataverse")
        insert_to_dataverse(web_api_url, access_token, env_url, gas_date, processed_df)    
        logger.info(f"Flow Import Successfull")            
        logger.info(f"IMPORT FOR {gas_date} SUCCESSFULL")
        print(f"IMPORT FOR {gas_date} SUCCESSFULL")
    except Exception as e:
        logger.error(f"IMPORT FOR {gas_date} UNSUCCESSFULL {e}")
        print(f"IMPORT FOR {gas_date} UNSUCCESSFULL {e}")
        subject="Error: Issue in importing Flow Data to Azure Storage"
        body=f"Flow Import Job Failed \n\nJob Start Time: \n{job_start_time_est} \n\nError: \n"
        send_email_notification(subject, body, logger)
    finally:
        logger.info("***************END*********************")


if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Flow Import")
    parser.add_argument('--gas_date', type=str, help='The date you want to import in yyyy-mm-dd format; If left empty, program will import yesterday\'s flow',default='auto')
    parser.add_argument('--env', choices=['dev', 'stage'], help='Environment', default='dev')
    args = parser.parse_args()
    flow_import(args.gas_date, args.env)
