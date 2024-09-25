import os
from datetime import datetime, timezone
import pytz
import json


with open('c:/Users/v91217/Downloads/Upload_Test/config.json') as config_file:
    config = json.load(config_file)

# Load Azure Data Lake credentials from configuration
azure_storage_config = config['azure_storage']


def convert_utc_to_et(utc_dt):
    """Convert a UTC datetime object to Eastern Time (ET)."""
    eastern = pytz.timezone('America/New_York')
    return utc_dt.astimezone(eastern)

def filtered_files(directory):
 # Get today's date in UTC
 today = convert_utc_to_et(datetime.now(timezone.utc)).date()

 # Initialize a list to hold the filtered files
 filtered_files = []

 # Iterate through the files in the directory
 for filename in os.listdir(directory):
    # Get the full path of the file
    file_path = os.path.join(directory, filename)
    
    # Ensure it's a file (not a directory)
    if os.path.isfile(file_path):
        # Get the modification time and convert it to a timezone-aware datetime
        mod_time = os.path.getmtime(file_path)
        mod_date = convert_utc_to_et(datetime.fromtimestamp(mod_time, timezone.utc)).date()
        
        # Check if the modification date is today or in the future
        if mod_date >= today:
            filtered_files.append(filename)

 return filtered_files

if __name__ == "__main__":
   # Define the directory you want to filter files from
   directory = azure_storage_config['UGS_local_directory']
   filtered_files = filtered_files(directory)

   # Print the filtered list of files
   print("Files with today's or future dates:")
   for file in filtered_files:
    print(file)