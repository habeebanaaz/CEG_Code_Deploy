# FLOW Import SCRIPT EXECUTION COMMANDS

### 1. Open Command Prompt 
Navigate to the script directory:
```
cd C:\Users\v91147\Downloads\Upload Jobs\
```

### 2. Activate the Virtual Environment
Run the following command to activate the virtual environment:
```
.env\Scripts\Activate
```

### 3. Change Directory to Flow Import
Navigate to the `flow_import` directory:
```
cd flow_import
```

### 4. Execute the Script
Run the script with the required arguments:
```
py flow_import.py --gas_date <gas_date> --env <env>
```

**Positional Arguments:**
- `gas_date`: The date to be imported, formatted as `yyyy-mm-dd`.
- `env`: The target environment, which can be either `{dev, stage}`.

**Example:**
```
python script.py --gas_date 2024-10-01 --env dev
```

If the program executes successfully, you will see the following message in the log file:
```
IMPORT FOR <gas_date> SUCCESSFUL
```

If the execution fails, the log file message will be:
```
IMPORT FOR <gas_date> UNSUCCESSFUL. 
```
Please check your arguments and try again.