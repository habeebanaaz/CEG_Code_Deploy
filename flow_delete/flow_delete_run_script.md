# FLOW DELETE SCRIPT EXECUTION COMMANDS

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

### 3. Change Directory to Flow Delete
Navigate to the `flow_delete` directory:
```
cd flow_delete
```

### 4. Execute the Script
Run the script with the required arguments:
```
py flow_delete.py <gas_date> <env>
```

**Positional Arguments:**
- `gas_date`: The date to be reimported, formatted as `yyyy-mm-dd`.
- `env`: The target environment, which can be either `{dev, stage}`.

**Example:**
```
py flow_delete.py 2024-10-01 dev
```

If the program executes successfully, you will see the message:
```
DELETE FOR <gas_date> SUCCESSFUL
```

If the execution fails, the message will be:
```
DELETE FOR <gas_date> UNSUCCESSFUL. 
```
Please check your arguments and try again.