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

PERRYK_ENTITY = os.getenv('PERRYK_UPLOAD_ENTITY')
PERRYK_METER = os.getenv('PERRYK_UPLOAD_METER')
PERRYK_FILE_PREFIX = os.getenv('PERRYK_UPLOAD_FILE_PREFIX')
PERRYK_UPLOAD_DIR = os.getenv('PERRYK_UPLOAD_DIR')

account_name = os.getenv('CSV_ACCOUNT_NAME')
account_key = os.getenv('CSV_ACCOUNT_KEY')
container_name = os.getenv('CSV_CONTAINER_NAME')

log_dir = os.getenv('PERRYK_UPLOAD_LOG_DIR')
log_file_name = 'perryK_upload.log'
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
 
def transform_flow(df):
    df.rename(columns={'Gas Day': 'EndDateTime','Hour':'HR','MCF':'Consump'},inplace=True)
    df['Meter'] = PERRYK_METER
    df['UOM'] = ""
    df["Blank"] = ""
    df["Reading"] = ""
    df["Blank1"] = ""
    df["RUID"] = ""
    df["RUInput"] = ""
    df = df[['EndDateTime','HR','Meter','UOM','Consump','Blank','Reading','Blank1','RUID','RUInput']]
    df.loc[df['HR'] == 24, 'HR'] = 0
    df.loc[df['HR'] <= 10, 'EndDateTime'] += timedelta(days=1)
    df['HR'] = df['HR'].astype(str).str.zfill(2) + ":00:00" # Ensure all hours are two digits
    df['EndDateTime'] = df['EndDateTime'].dt.strftime("%m-%d-%Y") + " " + df['HR']
    return df    

def get_table_details(web_api_url,access_token, table_name, id_col, name_col, url="" ):
    if url=="":
        url = f"{web_api_url}/{table_name}?$select={id_col},{name_col}"
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
    
def get_seq_from_hour(hour):
    if 1 <= hour <= 24:
        return hour + 14 if hour <= 10 else hour - 10
    return None

def get_hour_from_seq(seq):
    if 1 <= seq <= 24:
        return seq + 10 if seq <=14 else seq - 14
    return None

def get_flow_values(web_api_url, access_token, gas_date, entity):
    table_name="gas_gasdayreadings"
    id_col="gas_gasdayreadingid"
    name_col="gas_gasdayreadingid"
    url = f"{web_api_url}/{table_name}?$select=gas_mcf,gas_readinghour&$expand=gas_GasDate&$filter=gas_Entity/gas_name eq '{entity}' and gas_GasDate/gas_date eq {gas_date}"# and gas_readinghour eq {hour}"
    # url = f"{web_api_url}/{table_name}?$select=gas_gasdate,gas_difference&$expand=gas_Meter($select=gas_meternumber;$expand=gas_BTUZone($select=gas_name))&$filter=gas_gasdate ge {start_date} and gas_gasdate le {end_date} and gas_Meter/gas_meternumber eq '{meter_number}'"
    try:
        hour_df = pd.DataFrame({'Seq':range(1,25),'Gas Day':gas_date})
        hour_df['Hour'] = hour_df['Seq'].apply(get_hour_from_seq)
        mcf_df = pd.DataFrame(columns=[ 'MCF','Hour'])
        mcf_list=[]
        response_dict = get_table_details(web_api_url, access_token, table_name, id_col, name_col, url=url)
        if response_dict is not None:
            status = response_dict["status"]
            resp_list = response_dict["value"]
            if status=="success" and len(resp_list)>0:
                for resp_item in resp_list:
                    mcf_list.append({'MCF':resp_item['gas_mcf'],'Hour':resp_item['gas_readinghour']})
                mcf_df = pd.concat([mcf_df, pd.DataFrame(mcf_list)], ignore_index=True)
                mcf_df = pd.merge(hour_df, mcf_df, on="Hour", how="left")
                mcf_df['Gas Day'] = pd.to_datetime(mcf_df['Gas Day'])
                mcf_df.sort_values(by='Seq', ascending=True, inplace=True)
                mcf_df = mcf_df.reset_index(drop=True)
                return mcf_df
        else:
            error_message = f"Issue retrieveing records for {entity} for {gas_date}"
            logger.error(error_message)
            raise Exception(error_message)
    except Exception as e:
            error_message = f"Error occured in getting existing MCF records for {entity}: {e}"
            logger.error(error_message)
            raise Exception(error_message)


def get_raw_data(web_api_url, access_token, gas_date):
    perryK_df = get_flow_values(web_api_url, access_token, gas_date, PERRYK_ENTITY)
    return perryK_df

def write_to_network_dir(df,filename):
    try:
        if not os.path.exists(PERRYK_UPLOAD_DIR):
            os.makedirs(PERRYK_UPLOAD_DIR)
            logger.info(f"Directory '{PERRYK_UPLOAD_DIR}' created.")
        else:
            logger.info(f"Directory '{PERRYK_UPLOAD_DIR}' already exists.")

        file_path = PERRYK_UPLOAD_DIR+filename
        df.to_csv(file_path, index=False)
    except Exception as e:
        logger.error(f"Unable to upload perryK file {filename} to network directory")
        raise

def perryK_upload(gas_date:str, hour:str, env:str):
    try:
        logger.info("***************START*********************")
        eastern = pytz.timezone('US/Eastern')
        job_start_time_est = datetime.now(eastern) #-  timedelta(days=7)
        logger.info(f"Starting the scan and upload job at {job_start_time_est}...")
        if gas_date=='auto':
            gas_date = job_start_time_est #- timedelta(days=1)
            gas_date = gas_date.strftime('%Y-%m-%d')
        if hour=='auto':
            hour = job_start_time_est.strftime('%H')
        hour = int(hour)
        logger.info(f"Date {gas_date}; Hour {hour}")
        if hour<=10:
            gas_date = datetime.strptime(gas_date,'%Y-%m-%d')
            gas_date = gas_date - timedelta(days=1)
            gas_date = gas_date.strftime('%Y-%m-%d')
        logger.info(f"Gas Date {gas_date}")
        file_name = PERRYK_FILE_PREFIX+datetime.strptime(gas_date,"%Y-%m-%d").strftime("%m%d%y")+'.csv'   
        logger.info(f"File Name {file_name}")
        if env=="dev":
            env_url=os.getenv("DV_ENV_URL")
        elif env=="stage":
            env_url=os.getenv("DV_STAGE_ENV_URL")
        web_api_url = f"{env_url}/api/data/v9.2"
        access_token = get_access_token(tenant_id,client_id,client_secret,env_url)
        logger.info(f"PerryK Upload for {gas_date}")
        logger.info(f"Getting data from dataverse")
        source_df = get_raw_data(web_api_url, access_token, gas_date)
        logger.info(f"Transforming data")
        processed_df = transform_flow(source_df)
        logger.info(f"Writing data to network directory")
        write_to_network_dir(processed_df,file_name)    
        logger.info(f"PerryK Upload Successfull")            
        logger.info(f"UPLOAD FOR {gas_date} SUCCESSFULL")
        print(f"UPLOAD FOR {gas_date} SUCCESSFULL")
    except Exception as e:
        logger.error(f"UPLOAD FOR {gas_date} UNSUCCESSFULL \n{e}")
        print(f"UPLOAD FOR {gas_date} UNSUCCESSFULL \n{e}")
        subject="Error: Issue in uploading perryK Data to network directory"
        body=f"PerryK Upload Job Failed \n\nJob Start Time: \n{job_start_time_est} \n\nError:\n{e}\n"
        send_email_notification(subject, body, logger)
    finally:
        logger.info("***************END*********************")
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="PerryK Upload")
    parser.add_argument('--date', type=str, help='The date you want to upload in yyyy-mm-dd format; If left empty, program will import today\'s perryK data',default='auto')
    parser.add_argument('--hour', type=str, help='The hour you want to upload as a number; If left empty, program will import current hour\'s perryK data',default='auto')
    parser.add_argument('--env', choices=['dev', 'stage'], help='Environment', default='dev')
    args = parser.parse_args()
    perryK_upload(args.date, args.hour,args.env)
