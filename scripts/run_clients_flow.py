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
def process_client(client: str, gcp_project: str, dbt_project_dir: str, profiles_dir: str, dbt_path: str) -> None:
    """Process a single client using dbt, authenticating via GcpCredentials block."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    gcp_credentials_block = None
    temp_creds_file = None

    try:
        # Load the GCP credentials block
        logger.info("Loading GCP credentials from block 'holistic-money-credentials'...")
        gcp_credentials_block = GcpCredentials.load("holistic-money-credentials")
        service_account_info = gcp_credentials_block.service_account_info.get_secret_value()

        # Create a temporary file to store the credentials
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            # Convert the dict to a JSON string before writing
            f.write(json.dumps(service_account_info))
            temp_creds_file = f.name
        logger.info(f"GCP credentials written to temporary file: {temp_creds_file}")

        # Prepare environment variables for dbt subprocess
        dbt_env = os.environ.copy()
        dbt_env["GOOGLE_APPLICATION_CREDENTIALS"] = temp_creds_file
        dbt_env["DBT_CLIENT_DATASET"] = client
        dbt_env["DBT_BIGQUERY_PROJECT"] = gcp_project
        
        # Create the operation with executable path and explicit environment
        dbt_op = DbtCoreOperation(
            commands=["dbt run --target service_account"], 
            project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
            dbt_executable_path=dbt_path,
            overwrite_profiles=False,
            env=dbt_env # Pass the modified environment
        )
        
        # Run the operation
        logger.info(f"Executing dbt run for client {client} with explicit env...")
        result = dbt_op.run()
        
        logger.info(f"Successfully completed processing for {client}")
        return result
    except Exception as e:
        logger.error(f"Error processing client {client}: {str(e)}")
        raise
    finally:
        # Clean up the temporary credentials file
        if temp_creds_file and os.path.exists(temp_creds_file):
            logger.info(f"Cleaning up temporary credentials file: {temp_creds_file}")
            os.remove(temp_creds_file)
        # Unset the env var? Maybe not necessary as the process ends.
        # if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        #     del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

@flow(
    name="Process All Clients",
    description="Process all clients using dbt",
    version="1.0.0",
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
    
    # Get the absolute path to the dbt project directory
    # This directory contains profiles.yml
    script_dir = Path(__file__).parent.absolute()
    dbt_project_dir = str(script_dir.parent)
    logger.info(f"Using dbt project directory (and profiles_dir): {dbt_project_dir}")
    
    # Process clients sequentially
    for client in clients:
        try:
            # Pass dbt_project_dir as the profiles_dir
            process_client(client, gcp_project, dbt_project_dir, dbt_project_dir, dbt_path)
        except Exception as e:
            logger.error(f"Failed to process client {client}: {str(e)}")
            # Continue processing other clients despite failure
            continue
    
    logger.info("Completed processing all clients")

if __name__ == "__main__":
    process_all_clients() 