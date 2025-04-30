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
    """Process client using dbt with dynamically generated profiles.yml file specified via profiles_path."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    temp_creds_file = None
    temp_profiles_file = None
    profiles_content = None

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

        # Create a temporary file to store the dynamic profiles.yml
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".yml") as f_profiles:
            yaml.dump(profiles_content, f_profiles)
            temp_profiles_file = f_profiles.name
        logger.info(f"Dynamic profiles.yml written to temporary file: {temp_profiles_file}")
        
        # Create the operation pointing to the dynamic profile file via profiles_path
        dbt_op = DbtCoreOperation(
            commands=["dbt run"], 
            project_dir=dbt_project_dir,
            profiles_path=temp_profiles_file, # Use profiles_path with the file
            target="service_account", # Specify target to use within the profile
            dbt_executable_path=dbt_path,
            overwrite_profiles=False # Explicitly False
        )
        
        # Run the operation
        logger.info(f"Executing dbt run for client {client} using profiles_path='{temp_profiles_file}'...")
        result = dbt_op.run()
        
        logger.info(f"Successfully completed processing for {client}")
        return result
    except Exception as e:
        logger.error(f"Error processing client {client}: {str(e)}")
        raise
    finally:
        # Clean up the temporary files
        if temp_creds_file and os.path.exists(temp_creds_file):
            logger.info(f"Cleaning up temporary credentials file: {temp_creds_file}")
            os.remove(temp_creds_file)
        if temp_profiles_file and os.path.exists(temp_profiles_file):
            logger.info(f"Cleaning up temporary profiles file: {temp_profiles_file}")
            os.remove(temp_profiles_file)

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