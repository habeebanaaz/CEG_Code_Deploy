import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime, timezone
from azure.core.exceptions import ResourceNotFoundError
import pytz
import logging
import json
from send_email_app import send_email_notification
from scada_filter_files import filtered_files

# Configure logging
logging.basicConfig(filename='c:/Users/v91217/Downloads/Upload_Test/info.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load configuration from JSON file
logging.info('Loading configuration from JSON file...')
with open('c:/Users/v91217/Downloads/Upload_Test/config.json') as config_file:
    config = json.load(config_file)
logging.info('Configuration loaded successfully.')

# Load Azure Data Lake credentials from configuration
azure_storage_config = config['azure_storage']

# Configuration
connection_string = azure_storage_config['connection_string']
container_name= azure_storage_config['scada_container_name'] 
local_directory = azure_storage_config['scada_local_directory']


def blob_exists(blob_client):
    """Check if a blob exists in Azure Storage."""
    try:
        blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False
    
def convert_utc_to_et(utc_dt):
    """Convert a UTC datetime object to Eastern Time (ET)."""
    eastern = pytz.timezone('America/New_York')
    return utc_dt.astimezone(eastern)

def upload_files_to_blob(local_directory, container_name):
 try:
    # Create BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    logging.info('Blob Service Client created...')
    # Get container client
    container_client = blob_service_client.get_container_client(container_name)
    logging.info('Retrieving Container Client... ')
    filtered_files_list = filtered_files(local_directory)
    # Walk through the local directory
    for root, _, files in os.walk(local_directory):
        for file in files:
         if(file in filtered_files_list):
            print("This file is today's or future's date") 
            if file.endswith('.csv') or file.endswith('.xls') or file.endswith('.xlsx'):
              file_path = os.path.join(root, file)
              blob_client = container_client.get_blob_client(file) 
              #Last modified timestamp of Local file
              timestamp = os.path.getmtime(file_path)
              last_modified_local = convert_utc_to_et(datetime.fromtimestamp(timestamp, tz=timezone.utc))
              if not blob_exists(blob_client):
                # Upload the file
                print(f"Uploading {file_path} to {container_name}/{file}")
                logging.info(f'Uploading: {file_path} to {container_name}/{file}')
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                print(f"{file_path} uploaded successfully.")
                logging.info(f'{file_path} uploaded successfully')
              else:
                #Last modified timestamp of Blob
                blob_properties = blob_client.get_blob_properties()
                last_modified_blob = convert_utc_to_et(blob_properties['last_modified'])
                if((last_modified_local > last_modified_blob)):
                
                 print(f"Uploading {file_path} to {container_name}/{file}")
                 logging.info(f'Uploading: {file_path} to {container_name}/{file}')
                 # Upload the file
                 with open(file_path, "rb") as data:
                  blob_client.upload_blob(data, overwrite=True)
                
                 print(f"{file_path} uploaded successfully.")
                 logging.info(f'{file_path} uploaded successfully')
                else:
                 print("File is not modified")
                 logging.info(f'{file_path} is not modified!')
         else:
             print("This file is not today's or future's date")
 except Exception as e:

    subject="Error: Issue in uploading CEG SCADA File to Azure Storage"
    body=f"SCADA Upload Job Failed: {e}"
    send_email_notification(subject, body)
    
                
if __name__ == "__main__":

 try:
        upload_files_to_blob(local_directory, container_name)
 except Exception as e:
         send_email_notification("Error Details", e)
 