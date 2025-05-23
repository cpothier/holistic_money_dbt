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
        
        # Create the operation with executable path - use full 'dbt run' command
        dbt_op = DbtCoreOperation(
            commands=["dbt run"],
            project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
            dbt_executable_path=dbt_path,
            overwrite_profiles=False
        )
        
        # Run the operation
        logger.info(f"Executing dbt run for client {client}...")
        result = dbt_op.run()
        
        logger.info(f"Successfully completed processing for {client}")
        return result
    except Exception as e:
        logger.error(f"Error processing client {client}: {str(e)}")
        raise

@flow(
    name="Test Single Client",
    description="Test process for a single client using dbt",
    version="1.0.0",
    retries=1,
    retry_delay_seconds=300
)
def test_single_client(
    client: str = "golden_hour",
    gcp_project: str = "holistic-money",
    profiles_dir: str = "~/.dbt"
) -> None:
    """Process a single client using dbt for testing."""
    logger = get_run_logger()
    logger.info(f"Starting test flow for client: {client}")
    
    # Check dbt installation first
    dbt_path = check_dbt_installed()
    
    # Get the absolute path to the dbt project directory
    script_dir = Path(__file__).parent.absolute()
    dbt_project_dir = str(script_dir.parent)
    logger.info(f"Using dbt project directory: {dbt_project_dir}")
    
    # Process the client
    try:
        process_client(client, gcp_project, dbt_project_dir, profiles_dir, dbt_path)
        logger.info("Test completed successfully")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise
    
if __name__ == "__main__":
    test_single_client() 