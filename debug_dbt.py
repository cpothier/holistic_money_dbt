#!/usr/bin/env python3
import os
import subprocess
import json

# Use environment variables for the client and project
os.environ['DBT_CLIENT_DATASET'] = 'austin_lifestyler'
os.environ['DBT_BIGQUERY_PROJECT'] = 'holistic-money'

# Clean URL
url = "https://docs.google.com/spreadsheets/d/1SHBAJZ9xect4kwLrGTeH2GZilLYLmbQQQDwUo-BBZI4"
# Don't escape the exclamation mark as BigQuery doesn't accept it
sheet_range = "Budget Summary!A4:AS69"

print(f"Running dbt with URL: {url}")

# Get absolute path for profiles directory
home_dir = os.path.expanduser("~")
profiles_dir = os.path.join(home_dir, ".dbt")

# Build the dbt command
cmd = [
    'dbt', 'run-operation', 
    'create_external_budget_table',
    '--args', json.dumps({
        'budget_sheet_url': url,
        'sheet_range': sheet_range
    }),
    '--profiles-dir', profiles_dir,
    '--target', 'service_account',
    '--debug'  # Add debug flag to get more information
]

# Run the command
print(f"Command: {' '.join(cmd)}")
try:
    # Use Popen to capture real-time output
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    # Print output in real-time
    while True:
        stdout_line = process.stdout.readline()
        if stdout_line:
            print(f"STDOUT: {stdout_line.strip()}")
        
        stderr_line = process.stderr.readline()
        if stderr_line:
            print(f"STDERR: {stderr_line.strip()}")
            
        if not stdout_line and not stderr_line and process.poll() is not None:
            break
    
    # Get the return code
    return_code = process.poll()
    print(f"Command finished with return code: {return_code}")
    
except Exception as e:
    print(f"Error running command: {str(e)}")
