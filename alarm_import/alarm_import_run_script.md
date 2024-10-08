# ALARM Import SCRIPT EXECUTION COMMANDS

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

### 3. Change Directory to Alarm Import
Navigate to the `alarm_import` directory:
```
cd alarm_import
```

### 4. Execute the Script
Run the script with the required arguments:
```
py alarm_import.py --gas_date <gas_date> --env <env>
```

**Positional Arguments:**
- `gas_date`: The date to be imported, formatted as `yyyy-mm-dd`.
- `env`: The target environment, which can be either `{dev, stage}`.

**Example:**
```
python alarm_import.py --gas_date 2024-10-01 --env dev
```

If the program executes successfully, you will see the following message in the output and in log file (under ./alarm_log/alarm_import.log):
```
IMPORT FOR <gas_date> SUCCESSFUL
```

If the execution fails, the output and log file message (under ./alarm_log/alarm_import.log) will be:
```
IMPORT FOR <gas_date> UNSUCCESSFUL. 
```
Please check your arguments and try again.