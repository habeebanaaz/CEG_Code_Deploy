# PerryK Upload SCRIPT EXECUTION COMMANDS

### 1. Open Command Prompt 
Navigate to the script directory:
```
cd C:\Users\v91147\Downloads\Upload Jobs\
```

### 2. Activate the Virtual Environment
Run the following command to activate the virtual environment:
```
.\venv\Scripts\Activate
```

### 3. Change Directory to PerryK Upload
Navigate to the `perryK_upload` directory:
```
cd perryK_upload
```

### 4. Execute the Script
Run the script with the required arguments:
```
py perryK_upload.py --date <date> --hour <hour> --env <env>
```

**Positional Arguments:**
  --date  The date you want to upload in yyyy-mm-dd format; If left empty, program will import today's perryK data
  --hour      The hour you want to upload as a number; If left empty, program will import current hour's perryK data
  --env {dev,stage}    Environment

**Example:**
```
python perryK_upload.py --date 2024-10-01 --hour 14 --env dev
```

If the program executes successfully, you will see the following message in the output and in log file (under ./perryK_log/perryK_upload.log):
```
UPLOAD FOR <date> SUCCESSFUL
```

If the execution fails, the output and log file message (under ./perryK_log/perryK_upload.log) will be:
```
UPLOAD FOR <date> UNSUCCESSFUL. 
```
Please check your arguments and try again.