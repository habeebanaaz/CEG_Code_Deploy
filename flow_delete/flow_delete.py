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
import argparse
import os
from dotenv import load_dotenv
import warnings
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

def get_access_token(tenant_id,client_id,client_secret,env_url):
    access_token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    access_token_payload = f'grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={env_url}/.default'
    headers = {
      'Content-Type': 'application/x-www-form-urlencoded'
    }

    access_token_response = requests.request("POST", access_token_url, headers=headers, data=access_token_payload,verify=False)
    access_token_response_json = json.loads(access_token_response.text)
    access_token = access_token_response_json['access_token']
    return access_token   

def split_into_chunks(payload_list, splits=5):    
    split_list = [chunk.tolist() for chunk in np.array_split(payload_list, splits)]
    split_list = [i for i in split_list if i !=[]]
    return split_list


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
            # print(json.dumps(values,indent=4))
            id_list=[]
            if values:
                for val in values:
                    id_list.append(val[id_col])
            return {'status':"success",'ids':id_list, 'table_name':table_name,'id_col':id_col, }
        else:
            raise(f"Issue Obtaining Table Record Ids: {json.loads(response.text)}")
            # return {'status':'fail'}
    except Exception as e:
            raise(f"Exception occurred while obtaining Table Record Ids: {e}")
            # return {'status':'fail'}
def find_and_del_flow(web_api_url, access_token, gas_date):
    try:
        table_name="gas_flows"
        id_col="gas_flowid"
        date_filter = f"&$filter=gas_datetime ge {gas_date}T00:00:00Z and gas_datetime le {gas_date}T23:59:59Z"

        url = (
            f"{web_api_url}/{table_name}?$select={id_col}"
            f"{date_filter}"
        )
        iteration = 0
        print("Find and Delete Flows")
        delete_results ={"Total Iterations":None,"Delete Status":None, "Detailed Results":{"Iteration":{}}}
        while True:
            iteration+=1
            # url = f"{web_api_url}/{table_name}?$select={id_col}&$filter=gas_GasDay/gas_date eq {gas_date}"
            record_ids_result = get_table_record_ids( access_token, table_name, id_col, url)
            if record_ids_result['status']=='success':
                record_ids=record_ids_result['ids']
                to_delete_count = len(record_ids)
                if iteration==1 and to_delete_count<=0:
                    print("No Records present to delete")
                    break
                elif to_delete_count<=0:
                    print("No more records to delete")
                    break
                else:
                    print(f"Iteration: {iteration}")
                    print(f"     Performing Delete")
                    success_batch_in_itr=0
                    print(f"     Number of Records to delete: {to_delete_count}")
                    total_len=len(record_ids)
                    count_per_batch = 1000
                    splits = math.ceil(total_len / count_per_batch)
                    print(f"     Batches: {splits}; ")
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]={"Records to Delete":None,"Total Batches":None,"Batch":{},"Delete Status":None}
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Records to Delete"]=to_delete_count
                    delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Total Batches"]=splits

                    ids = split_into_chunks(record_ids, splits=splits)
                    
                    for batch, id_list in enumerate(ids):
                        delete_results["Detailed Results"]["Iteration"][f"{iteration}"]["Batch"][f"{batch}"]={"Delete Status":None}
                        print(f"     Performing delete for batch: {batch+1}")
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

def delete_existing_flow(web_api_url, access_token, env_url, gas_date):
    # del_result = find_and_del_flow(web_api_url, access_token, gas_date)
    try:
        start_time = time.time()
        # print(f"Starting Time for Delete Job {start_time}")
        del_result = find_and_del_flow(web_api_url, access_token, gas_date)
        end_time = time.time()
        time_taken = end_time - start_time
        # print(f"Ending Time for Delete Job {end_time}")
        print(f"Time taken to run: {time_taken:.4f} seconds")
        print(f"Successfully deleted existing flows\n{json.dumps(del_result,indent=4)}")
        # if del_result["status"]=="success":
        #     if del_result["to_delete"]==0:
        #         print(f"No records to delete for {gas_date}")
        #     else:
        #         print(f"Existing flow for {gas_date} are deleted before import - {del_result}")
        # else:
        #     raise Exception(f"Unable to delete exisiting flow values from the table for {gas_date} - {del_result}")
    except Exception as e:
        print(f"Error in deleting existing flow for {gas_date} : {e}")
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
    

def bulk_delete(web_api_url, access_token, table_name, ids):
    try:
        import uuid

        print("      BULK DELETING")
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
        # print(batch_body)
        # Send the batch request
        response = requests.post(BATCH_URL, headers=HEADERS, data=batch_body, verify=False)

        # Check response
        if response.status_code in [200,202,204]:
            print("      Batch delete request sent successfully.")
            return {"status":"success"}
        else:
            raise Exception(f"Error in deleting flows in bulk: {response.status_code} - {response.reason} ")
    except Exception as e:
        raise Exception(f"Exception occured while deleting flows in bulk: {e}")

def flow_delete(gas_date:str, env:str):
    try:
        if env=="dev":
            env_url=os.getenv("DV_ENV_URL")
        elif env=="stage":
            env_url=os.getenv("DV_STAGE_ENV_URL")
        web_api_url = f"{env_url}/api/data/v9.2"
        access_token = get_access_token(tenant_id,client_id,client_secret,env_url)
        print(f"FLOW IMPORT for {gas_date}")
        delete_existing_flow(web_api_url, access_token, env_url, gas_date)
        print(f"DELETE FOR {gas_date} SUCCESSFULL")
    except Exception as e:
        print(f"DELETE FOR {gas_date} UNSUCCESSFULL {e}")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Flow Import")
    parser.add_argument('gas_date', type=str, help='The date you want to reimport in yyyy-mm-dd format')
    parser.add_argument('env', choices=['dev', 'stage'], help='Environment', default='dev')
    args = parser.parse_args()
    flow_delete(args.gas_date, args.env)
