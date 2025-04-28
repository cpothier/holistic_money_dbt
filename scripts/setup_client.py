#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import configparser
import tempfile
import json
import logging
import re

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('setup_client.log'),
        logging.StreamHandler()
    ]
)

def clean_sheet_url(url):
    """Clean Google Sheets URL to the format required by BigQuery"""
    # Remove any parameters or fragments
    base_url = url.split('?')[0].split('#')[0]
    # Remove /edit if present
    base_url = base_url.replace('/edit', '')
    return base_url

def run_command(cmd, env=None, check=True):
    """Run a command and log its output"""
    logging.info(f"Running command: {' '.join(cmd)}")
    try:
        # Run with output visible in real-time
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        stdout_data = []
        stderr_data = []
        
        # Print output in real-time
        while True:
            stdout_line = process.stdout.readline()
            if stdout_line:
                line = stdout_line.strip()
                logging.info(f"STDOUT: {line}")
                stdout_data.append(line)
                print(line)  # Show output in real-time
                
            stderr_line = process.stderr.readline()
            if stderr_line:
                line = stderr_line.strip()
                logging.error(f"STDERR: {line}")
                stderr_data.append(line)
                print(f"ERROR: {line}")  # Show errors in real-time
                
            if not stdout_line and not stderr_line and process.poll() is not None:
                break
        
        # Get the return code
        return_code = process.poll()
        if return_code != 0 and check:
            error_msg = f"Command failed with exit code {return_code}\nCommand: {' '.join(cmd)}"
            logging.error(error_msg)
            raise subprocess.CalledProcessError(
                return_code, cmd, 
                output='\n'.join(stdout_data), 
                stderr='\n'.join(stderr_data)
            )
            
        return process
        
    except Exception as e:
        logging.error(f"Error running command: {str(e)}")
        raise

def create_debug_script(budget_sheet_url, sheet_range, client, project):
    """Create a debug script that we know works"""
    # Don't escape the exclamation mark - BigQuery doesn't like it
    # Just use the sheet range as is
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.dirname(script_dir)
    debug_script_path = os.path.join(dbt_dir, "debug_dbt.py")
    
    with open(debug_script_path, "w") as f:
        f.write(f'''#!/usr/bin/env python3
import os
import subprocess
import json

# Use environment variables for the client and project
os.environ['DBT_CLIENT_DATASET'] = '{client}'
os.environ['DBT_BIGQUERY_PROJECT'] = '{project}'

# Clean URL
url = "{budget_sheet_url}"
# Don't escape the exclamation mark as BigQuery doesn't accept it
sheet_range = "{sheet_range}"

print(f"Running dbt with URL: {{url}}")

# Get absolute path for profiles directory
home_dir = os.path.expanduser("~")
profiles_dir = os.path.join(home_dir, ".dbt")

# Build the dbt command
cmd = [
    'dbt', 'run-operation', 
    'create_external_budget_table',
    '--args', json.dumps({{
        'budget_sheet_url': url,
        'sheet_range': sheet_range
    }}),
    '--profiles-dir', profiles_dir,
    '--target', 'service_account',
    '--debug'  # Add debug flag to get more information
]

# Run the command
print(f"Command: {{' '.join(cmd)}}")
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
            print(f"STDOUT: {{stdout_line.strip()}}")
        
        stderr_line = process.stderr.readline()
        if stderr_line:
            print(f"STDERR: {{stderr_line.strip()}}")
            
        if not stdout_line and not stderr_line and process.poll() is not None:
            break
    
    # Get the return code
    return_code = process.poll()
    print(f"Command finished with return code: {{return_code}}")
    
except Exception as e:
    print(f"Error running command: {{str(e)}}")
''')
    
    # Make the script executable
    os.chmod(debug_script_path, 0o755)
    return debug_script_path

def setup_dbt_profile():
    """Set up the dbt profile with the correct service account path"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.dirname(script_dir)
    profile_update_script = os.path.join(dbt_dir, "update_profile.py")
    
    if not os.path.exists(profile_update_script):
        # Create the update_profile.py script if it doesn't exist
        logging.info("Creating profile update script")
        with open(profile_update_script, "w") as f:
            f.write('''#!/usr/bin/env python3
import os
import sys

# Create the profiles directory
home_dir = os.path.expanduser("~")
profiles_dir = os.path.join(home_dir, ".dbt")
os.makedirs(profiles_dir, exist_ok=True)

# Get absolute path to project root and credentials
current_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(current_dir, "credentials", "service-account.json")

# Create the profiles.yml content
profiles_content = f"""holistic_money_dw:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: "holistic-money"
      dataset: "default_client"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      
    service_account:
      type: bigquery
      method: service-account
      project: "holistic-money"
      dataset: "{{{{ env_var('DBT_CLIENT_DATASET', 'default_client') }}}}"
      keyfile: "{creds_path}"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
"""

# Write the profiles.yml file
profiles_file = os.path.join(profiles_dir, "profiles.yml")
with open(profiles_file, "w") as f:
    f.write(profiles_content)

print(f"Updated dbt profile at {profiles_file}")
print(f"Service account keyfile path: {creds_path}")''')
    
        # Make the script executable
        os.chmod(profile_update_script, 0o755)
    
    # Run the profile update script
    logging.info("Updating dbt profile with correct service account path")
    run_command(["python3", profile_update_script])

def main():
    """
    Setup a new client data warehouse using dbt Core
    """
    parser = argparse.ArgumentParser(description='Setup a dbt warehouse for a specific client')
    parser.add_argument('--client', required=True, help='Client name (dataset in BigQuery)')
    parser.add_argument('--project', required=True, help='GCP project ID')
    parser.add_argument('--budget-sheet-url', required=True, help='URL to Google Sheet with budget data')
    parser.add_argument('--sheet-range', required=True, help='Range in Google Sheet (e.g., Budget Summary!A4:AS69)')
    parser.add_argument('--profile-dir', help='Path to dbt profiles directory', default='~/.dbt')
    parser.add_argument('--dbt-target', help='dbt target to use', default='service_account')
    parser.add_argument('--dry-run', action='store_true', help='Show commands without executing')
    
    args = parser.parse_args()
    
    # Get absolute paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dbt_dir = os.path.dirname(script_dir)
    profile_dir = os.path.expanduser(args.profile_dir)
    staging_file = os.path.join(dbt_dir, 'models', 'staging', 'stg_budget_template.sql')
    
    # Clean the Google Sheets URL
    clean_url = clean_sheet_url(args.budget_sheet_url)
    logging.info(f"Cleaned sheet URL: {clean_url}")
    
    # Log initial setup
    logging.info(f"Setting up client {args.client} in project {args.project}")
    logging.info(f"Working directory: {dbt_dir}")
    logging.info(f"Profile directory: {profile_dir}")
    
    # Set up the dbt profile
    setup_dbt_profile()
    
    # Change to the dbt project directory
    os.chdir(dbt_dir)
    
    # Create an environment variables dict for dbt
    env = os.environ.copy()
    env['DBT_CLIENT_DATASET'] = args.client
    env['DBT_BIGQUERY_PROJECT'] = args.project
    
    # Step 1: Create external Google Sheets table using the debug script that we know works
    logging.info(f"Setting up budget template for client {args.client} using debug script...")
    debug_script = create_debug_script(clean_url, args.sheet_range, args.client, args.project)
    
    if not args.dry_run:
        try:
            run_command(["python3", debug_script])
        except subprocess.CalledProcessError as e:
            logging.error("Failed to create external budget table")
            sys.exit(1)
    
    # Step 2: Enable the staging model
    logging.info("Enabling staging model...")
    if os.path.exists(staging_file):
        with open(staging_file, 'r') as f:
            content = f.read()
        with open(staging_file, 'w') as f:
            f.write(content.replace('enabled=false', 'enabled=true'))
        logging.info("Staging model enabled successfully")
    else:
        logging.warning(f"Could not find staging file at {staging_file}")
    
    # Step 3: Run dbt models
    logging.info(f"Running dbt models for client {args.client}...")
    dbt_run_cmd = [
        'dbt', 'run',
        '--profiles-dir', profile_dir,
        '--target', args.dbt_target
    ]
    
    if not args.dry_run:
        try:
            run_command(dbt_run_cmd, env=env)
        except subprocess.CalledProcessError as e:
            logging.error("Failed to run dbt models")
            sys.exit(1)
    
    logging.info(f"Client {args.client} setup complete!")

if __name__ == "__main__":
    main() 