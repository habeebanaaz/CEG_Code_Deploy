from email import errors
import os
import time
import schedule
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from send_email_app import send_email_notification 
import pyodbc
import pytz
from datetime import datetime, timedelta
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions

load_dotenv()
account_name = os.getenv('CSV_ACCOUNT_NAME')
account_key = os.getenv('CSV_ACCOUNT_KEY')
file_system_name = os.getenv('CSV_CONTAINER_NAME')
container_name = os.getenv('CSV_CONTAINER_NAME')

AMR_DIR = os.getenv('AMR_DIR')
AMR_FILE = os.getenv('AMR_FILE')
# SRC_AMR_DIR = 'C:/Users/v91147/Downloads/Migrate/CEG_etl_data_migration/amr_import/'
SRC_AMR_DIR = os.getenv('SRC_AMR_DIR')
SRC_FILE_NAME='amr_data.csv'
src_file_path = SRC_AMR_DIR+SRC_FILE_NAME
directory_name=AMR_DIR


# Azure Data Lake credentials
# log_dir = 'C:/Users/v91147/Downloads/Migrate/CEG_etl_data_migration/amr_import/amr_log/'
log_dir = os.getenv('AMR_LOG_DIR')
log_file_name = 'amr_upload.log'
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



# Initialize the Data Lake service client
service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net", credential=account_key)
file_system_client = service_client.get_file_system_client(file_system=file_system_name)
directory_client = file_system_client.get_directory_client(directory_name)

# Directory to scan for CSV files
# network_drive_path = 'C:/Users/PriyaG/Desktop/ceg/UploadAgent/CEG Scada/'

def get_db_connection():
    try:
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_DATABASE')
        username = os.getenv('DB_USERNAME')
        # if username:
        #     username = r'%s' % username
        #     logger.error(username)
            
        password = os.getenv('DB_PASSWORD')
        # connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        # print(connection_string)       
        connection_string = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )

        return pyodbc.connect(connection_string)
    except Exception as e:
        print(f"Ex[ {e}]")
        raise
def get_reading():
    pass
def get_source_data(today):
    conn = get_db_connection()
    cursor = conn.cursor()
    logger.info(f"Date: {today}")
    # upcoming_gas_date = gas_date + timedelta(days=1)
    act_today = today
    today = datetime.strftime(today,format="%Y-%m-01 00:00:00")
    logger.info(f"Month: {today}")
    query =f"""
           WITH filtered_dr AS (
            SELECT *
            FROM dbo.Daily_Reading AS dr
            WHERE dr.Date_Time = '{today}'
        )
        SELECT fdr.*, ip.Input_Desc, ms.Address2
        FROM filtered_dr AS fdr
        INNER JOIN dbo.RU_Input AS ip 
            ON fdr.RU_ID = ip.RU_ID 
            AND fdr.RU_Input_Num = ip.RU_Input_Num
        INNER JOIN dbo.RU_Master2 AS ms 
            ON fdr.RU_ID = ms.RU_ID
            WHERE ms.Address2 IN ('STAT',
        'WGC','BASIC')

        """
    cursor.execute(query)
    # Fetch all rows from the result set
    rows = cursor.fetchall()
    
    # Convert the result set to a pandas DataFrame
    columns = [column[0] for column in cursor.description]
    source_df = pd.DataFrame.from_records(rows, columns=columns)
    source_df.to_csv(src_file_path,index=False)
    if source_df.empty:
        error_message = f"Readings are not present for {act_today}"
        logger.error(error_message)
        # raise Exception(error_message)
    return source_df

def upload_file_to_datalake(file_path, file_name):
    try:
        file_client = directory_client.get_file_client(file_name)
        with open(file_path, 'rb') as file:
            file_client.upload_data(file, overwrite=True)
        logger.info(f"Uploaded {file_name} to Azure Data Lake. Source {file_path}")
    except Exception as e:
        error_message = f"Failed to upload {file_name}: {e}"
        logger.error(error_message)
        raise Exception(error_message)
    
def upload_blob(file_path):
    logging.info('Parsing CSV started')

    # Construct the connection string
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
    
    try:
        # Create the BlobServiceClient object
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Build the file name & create a blob client
        blob_file_name = f"{AMR_DIR}/{AMR_FILE}"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file_name)

        # Upload the byte string to the blob
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
    except Exception as e:
        logging.error(f"Exception occurred: {e}")
        raise

def scan_and_upload(job_start_time_est):
    try:
        get_source_data(job_start_time_est)
        try:
            upload_blob(src_file_path)
            # uploads_file_to_datalake(src_file_path, AMR_FILE)
        except Exception as e:
            error_message = f"Issue in uploading files to Azure Storage: \n\n{e}"
            logger.error(error_message)
            raise Exception(error_message)
    except Exception as e: 
        error_message = f"Failed to scan and upload: \n\n{e}"
        logger.error(error_message)
        raise Exception(error_message)

def job():
    logger.info("***************START*********************")

    # job_start_timestamp = time.time()
    # Set the timezone to Eastern Time (EST)
    eastern = pytz.timezone('US/Eastern')
    # Get the current time in Eastern Time (EST)
    job_start_time_est = datetime.now(eastern)
    # subject="Error: Issue in uploading AMR Data to Azure Storage"
    # body=f"AMR Upload Job Failed \n\nJob Start Time: \n{job_start_time_est} \n\nError: \n"
    # send_email_notification(subject, body, logger)
    # return
    # job_start_time_est = job_start_time_est - timedelta(days=71)
    logger.info(f"Starting the scan and upload job at {job_start_time_est}...")
    scan_and_upload(job_start_time_est)
    try:
        logger.info("Job completed.")
        # raise Exception("Email test")
    except Exception as e:
        logger.error("Job Failed.")
        logger.error(f"Error occured during job: {e}")
        subject="Error: Issue in uploading AMR Data to Azure Storage"
        body=f"AMR Upload Job Failed \n\nJob Start Time: \n{job_start_time_est} \n\nError: \n{e}"
        send_email_notification(subject, body, logger)
    finally:
        logger.info("***************END*********************")
 
if __name__ == "__main__":
    job()


# # Schedule the job every hour
# schedule.every(1).minutes.do(job)
# # schedule.every(1).minute.do(job)
# # schedule.every().hour.at("05:00").do(job)

# # schedule.every(1).hour.do(job)

# if __name__ == "__main__":
#     print("Scheduler started. Press Ctrl+C to exit.")
#     while True:
#         schedule.run_pending()  # Check for due jobs and run them
#         time.sleep(1)  # Sleep for a short duration to avoid busy-waiting
