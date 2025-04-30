from prefect import flow, task, get_run_logger
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from prefect_github.repository import GitHubRepository
from prefect_gcp.credentials import GcpCredentials
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
    """Process a single client using dbt, authenticating via dbt_cli_profile passed to DbtCoreOperation."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    temp_creds_file = None
    # profiles_content renamed to dbt_cli_profile_data
    dbt_cli_profile_data = None

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

        # Define the profile data structure expected by dbt_cli_profile
        dbt_cli_profile_data = {
            "name": "holistic_money_dw", # Profile name (must match dbt_project.yml)
            "target": "service_account",   # Default target for this profile
            "target_configs": {           # Corresponds to 'outputs' in profiles.yml
                "service_account": {      # The actual target configuration
                    "type": "bigquery",
                    "method": "service-account",
                    "project": gcp_project,
                    "dataset": client,
                    "keyfile": temp_creds_file, 
                    "threads": 4,
                    "timeout_seconds": 300,
                    "location": "US",
                    "priority": "interactive"
                }
            }
        }
        logger.info("Defined dbt_cli_profile data structure.")

        # Create the operation passing the structured profile dictionary
        dbt_op = DbtCoreOperation(
            commands=["dbt run"], 
            project_dir=dbt_project_dir,
            dbt_cli_profile=dbt_cli_profile_data, # Pass the structured dictionary
            dbt_executable_path=dbt_path,
            overwrite_profiles=False 
        )
        
        # Run the operation
        logger.info(f"Executing dbt run for client {client} using dbt_cli_profile arg...")
        result = dbt_op.run()
        
        logger.info(f"Successfully completed processing for {client}")
        return result
    except Exception as e:
        logger.error(f"Error processing client {client}: {str(e)}")
        raise
    finally:
        # Clean up ONLY the temporary credentials file
        if temp_creds_file and os.path.exists(temp_creds_file):
            logger.info(f"Cleaning up temporary credentials file: {temp_creds_file}")
            os.remove(temp_creds_file)
        # Remove cleanup for temp_profiles_file

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