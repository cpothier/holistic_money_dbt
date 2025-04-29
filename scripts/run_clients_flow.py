from prefect import flow, task, get_run_logger
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
from prefect_github.repository import GitHubRepository
import os
import subprocess
import shutil
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
    """Process a single client using dbt."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    try:
        # Set environment variables
        os.environ["DBT_CLIENT_DATASET"] = client
        os.environ["DBT_BIGQUERY_PROJECT"] = gcp_project
        
        # Get service account credentials from secret block
        try:
            service_account_secret = Secret.load("bigquery-credentials")
            service_account_json = service_account_secret.get()
            
            # Write credentials to a temporary file
            import tempfile
            import json
            
            temp_creds_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
            try:
                with open(temp_creds_file.name, 'w') as f:
                    json.dump(json.loads(service_account_json), f)
                
                logger.info(f"Temporary credentials file created at {temp_creds_file.name}")
                
                # Create the operation with executable path - use full 'dbt run' command
                dbt_op = DbtCoreOperation(
                    commands=["dbt run"],
                    project_dir=dbt_project_dir,
                    profiles_dir=profiles_dir,
                    dbt_executable_path=dbt_path,
                    dbt_cli_profile={
                        "name": "holistic_money_dw",
                        "target": "service_account",
                        "target_configs": {
                            "type": "bigquery",
                            "method": "service-account",
                            "project": "{{ env_var('DBT_BIGQUERY_PROJECT') }}",
                            "dataset": "{{ env_var('DBT_CLIENT_DATASET') }}",
                            "schema": "{{ env_var('DBT_CLIENT_DATASET') }}",
                            "threads": 4,
                            "keyfile": temp_creds_file.name,
                            "timeout_seconds": 300
                        },
                        "config": {
                            "send_anonymous_usage_stats": False
                        }
                    }
                )
                
                # Run the operation
                logger.info(f"Executing dbt run for client {client}...")
                result = dbt_op.run()
                
                logger.info(f"Successfully completed processing for {client}")
                return result
            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_creds_file.name)
                    logger.info("Temporary credentials file removed")
                except Exception as e:
                    logger.warning(f"Failed to remove temporary credentials file: {str(e)}")
        except Exception as e:
            logger.error(f"Error with credentials: {str(e)}")
            raise
    except Exception as e:
        logger.error(f"Error processing client {client}: {str(e)}")
        raise

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
    profiles_dir: str = "~/.dbt"
) -> None:
    """Process all clients using dbt."""
    logger = get_run_logger()
    logger.info(f"Starting flow to process {len(clients)} clients")
    
    # Check dbt installation first
    dbt_path = check_dbt_installed()
    
    # Get the absolute path to the dbt project directory
    script_dir = Path(__file__).parent.absolute()
    dbt_project_dir = str(script_dir.parent)
    logger.info(f"Using dbt project directory: {dbt_project_dir}")
    
    # Process clients sequentially
    for client in clients:
        try:
            process_client(client, gcp_project, dbt_project_dir, profiles_dir, dbt_path)
        except Exception as e:
            logger.error(f"Failed to process client {client}: {str(e)}")
            # Continue processing other clients despite failure
            continue
    
    logger.info("Completed processing all clients")

if __name__ == "__main__":
    process_all_clients() 