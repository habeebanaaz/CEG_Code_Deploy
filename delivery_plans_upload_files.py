import os
from datetime import datetime,timezone
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
import pytz
import json
import logging

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

# Function to filter files based on last modified time
def filter_files(parent_folder, prefix='DeliveryPlan', suffix='Exelon'):
    # Get today's date
    today = convert_utc_to_et(datetime.now())
    current_year = today.year
    current_month = today.month

    filtered_files = []

    # Check if the specified parent folder exists
    if os.path.isdir(parent_folder):
        for year_folder in os.listdir(parent_folder):
            year_path = os.path.join(parent_folder, year_folder)

            # Check if it's a directory for a year and matches the format 'FYxxxx'
            if os.path.isdir(year_path) and year_folder.startswith('FY') and len(year_folder) == 6:
                year = int(year_folder[2:])  # Extract year from 'FYxxxx'

                for month_folder in os.listdir(year_path):
                    month_path = os.path.join(year_path, month_folder)

                    # Check if it's a directory for a month
                    if os.path.isdir(month_path):
                        # Extract month and year from the folder name (e.g., 'Oct2024')
                        try:
                            month = datetime.strptime(month_folder, "%b%Y").month  # e.g., 'Oct2024'

                            # Get all files in the month folder
                            for file in os.listdir(month_path):
                                file_path = os.path.join(month_path, file)

                                # Check if the file starts with the specified prefix
                                if os.path.isfile(file_path) and file.startswith(prefix) and not file.endswith(suffix):
                                     # Get the last modified time of the file
                                     mod_time = convert_utc_to_et(datetime.fromtimestamp(os.path.getmtime(file_path)))

                                     # Check if the modification date is this month or in future months
                                     if (mod_time.year == year and mod_time.month >= month) or (mod_time.year > year):
                                        filtered_files.append(file_path)
                        except ValueError:
                            print(f"Skipping folder '{month_folder}': Invalid format.")

    return filtered_files


# Function to upload files to Azure Blob Storage
def upload_files_to_blob(blob_service_client, container_name, local_files):
    container_client = blob_service_client.get_container_client(container_name)

    for local_file in local_files:
        file_name = os.path.basename(local_file)
        blob_client = container_client.get_blob_client(file_name)
        timestamp = os.path.getmtime(local_file)
        last_modified_local = convert_utc_to_et(datetime.fromtimestamp(timestamp, tz=timezone.utc))

        # Check if the blob already exists
        if not blob_exists(blob_client):
            # Blob does not exist, upload as a new blob
            print(f"Uploading new file: {file_name}")
            with open(local_file, "rb") as data:
                blob_client.upload_blob(data)
        else:
           #Last modified timestamp of Blob
            blob_properties = blob_client.get_blob_properties()
            last_modified_blob = convert_utc_to_et(blob_properties['last_modified'])
            # Compare last modified time
            if((last_modified_local > last_modified_blob)):
                print(f"Uploading modified file: {file_name}")
                logging.info(f"Uploading modified file: {file_name}")
                with open(local_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
            else:
                print(f"File '{file_name}' not modified, skipping upload.")
                logging.info(f"File '{file_name}' not modified, skipping upload.")


# Main execution
if __name__ == "__main__":
   
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
   parent_folder = azure_storage_config['delivery_plans_local_directory']  # Replace with your actual path
   container_name = azure_storage_config['delivery_plans_container_name']        # Replace with your container name

   # Filter files
   filtered_files = filter_files(parent_folder)
  
   # Create BlobServiceClient
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)

   # Upload files to Azure Blob Storage
   upload_files_to_blob(blob_service_client, container_name, filtered_files)

   print("Upload process completed.")
   logging.info("Upload process completed.")
