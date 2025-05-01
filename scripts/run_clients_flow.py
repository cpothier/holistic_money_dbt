from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect_github.repository import GitHubRepository
from prefect_gcp.credentials import GcpCredentials
from prefect_shell import ShellOperation
import os
import subprocess
import shutil
import tempfile
import json
import yaml
from typing import List
from datetime import timedelta
from pathlib import Path

# Load the GitHub repository block for deployment
github_repository_block = GitHubRepository.load("holistic-money-dbt")

@task
def check_dbt_installed():
    """Check if dbt is installed and accessible."""
    logger = get_run_logger()
    dbt_path = shutil.which("dbt")
    
    if not dbt_path:
        raise RuntimeError("dbt executable not found in PATH. Please install dbt.")
    
    logger.info(f"Found dbt at: {dbt_path}")
    
    # Run dbt --version to verify installation
    try:
        result = subprocess.run(["dbt", "--version"], capture_output=True, text=True)
        logger.info(f"dbt version info: {result.stdout.strip()}")
        return dbt_path
    except Exception as e:
        logger.error(f"Error verifying dbt installation: {str(e)}")
        raise

@task(
    retries=2,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    persist_result=False
)
def process_client(client: str, gcp_project: str, dbt_project_dir: str, dbt_path: str) -> None:
    """Process client using dbt via prefect_shell with a dynamic profiles.yml."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    temp_creds_file = None
    temp_profiles_dir = None

    try:
        # Load the GCP credentials block
        logger.info("Loading GCP credentials from block 'holistic-money-credentials'...")
        gcp_credentials_block = GcpCredentials.load("holistic-money-credentials")
        service_account_info = gcp_credentials_block.service_account_info.get_secret_value()

        # Create a temporary file to store the credentials JSON
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f_creds:
            f_creds.write(json.dumps(service_account_info))
            temp_creds_file = f_creds.name
        logger.info(f"GCP credentials written to temporary file: {temp_creds_file}")

        # Define the standard profiles.yml content using the temp creds file path
        profiles_content = {
            "holistic_money_dw": { # Matches the profile name in dbt_project.yml
                "target": "service_account",
                "outputs": {
                    "service_account": {
                        "type": "bigquery",
                        "method": "service-account",
                        "project": gcp_project,
                        "dataset": client,
                        "keyfile": temp_creds_file, # Use the temp creds file path directly
                        "threads": 4,
                        "timeout_seconds": 300,
                        "location": "US",
                        "priority": "interactive"
                    }
                }
            }
        }

        # Create a temporary directory to store profiles.yml
        temp_profiles_dir = tempfile.mkdtemp(prefix="dbt_profiles_")
        profiles_file_path = os.path.join(temp_profiles_dir, "profiles.yml")
        
        # Write profiles.yml file with the proper name
        with open(profiles_file_path, "w") as f_profiles:
            yaml.safe_dump(profiles_content, f_profiles, default_flow_style=False)
            
        logger.info(f"Created profiles.yml at: {profiles_file_path}")
        logger.info(f"Using profiles directory: {temp_profiles_dir}")
        
        # Debug: Print file contents and verify it exists
        logger.info(f"Verifying profiles.yml exists: {os.path.exists(profiles_file_path)}")
        with open(profiles_file_path, "r") as f:
            logger.info(f"Profiles.yml content preview: {f.read()[:500]}")
        
        # List directory contents to verify
        logger.info(f"Directory contents of {temp_profiles_dir}: {os.listdir(temp_profiles_dir)}")
        
        # Construct the shell command for ShellOperation
        command = f'{dbt_path} run --project-dir "{dbt_project_dir}" --profiles-dir "{temp_profiles_dir}" --target service_account --debug'
        logger.info(f"Executing command: {command}")

        # Run the command using ShellOperation
        shell_op = ShellOperation(
            commands=[command],
            return_all=True,
            stream_output=True,
            env={
                "DBT_BIGQUERY_PROJECT": gcp_project,   # already set
                "DBT_CLIENT_DATASET": client,          # 👈 add this
            }
        )
        result = shell_op.run()
        logger.info(f"Shell operation output:\n{result}")

        logger.info(f"Successfully completed processing for {client}")
        return result

    except Exception as e:
        # Log the full exception details
        logger.error(f"Error processing client {client}", exc_info=True)
        raise
    finally:
        # Clean up the temporary files
        if temp_creds_file and os.path.exists(temp_creds_file):
            logger.info(f"Cleaning up temporary credentials file: {temp_creds_file}")
            os.remove(temp_creds_file)
        if temp_profiles_dir and os.path.exists(temp_profiles_dir):
            logger.info(f"Cleaning up temporary profiles directory: {temp_profiles_dir}")
            shutil.rmtree(temp_profiles_dir)

@flow(
    name="Process All Clients",
    description="Process all clients using dbt",
    version="1.1.0",
    retries=1,
    retry_delay_seconds=300
)
def process_all_clients(
    clients: List[str] = [
        "golden_hour",
        "austin_lifestyler",
        "bb_design",
        "child_life_on_call",
        "western_holistic_med"
    ],
    gcp_project: str = "holistic-money",
) -> None:
    """Process all clients using dbt."""
    logger = get_run_logger()
    logger.info(f"Starting flow to process {len(clients)} clients")
    
    # Check dbt installation first
    dbt_path = check_dbt_installed()
    
    # Get the absolute path to the dbt project directory (contains dbt_project.yml)
    script_dir = Path(__file__).parent.absolute()
    dbt_project_dir = str(script_dir.parent)
    logger.info(f"Using dbt project directory: {dbt_project_dir}")
    
    # Process clients sequentially
    for client in clients:
        try:
            # Pass only needed params - profiles_dir removed
            process_client(client, gcp_project, dbt_project_dir, dbt_path)
        except Exception as e:
            logger.error(f"Failed to process client {client}: {str(e)}")
            # Continue processing other clients despite failure
            continue
    
    logger.info("Completed processing all clients")

if __name__ == "__main__":
    process_all_clients() 