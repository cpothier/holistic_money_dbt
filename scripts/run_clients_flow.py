from prefect import flow, task, get_run_logger
from prefect_shell import shell_run_command
from prefect.blocks.system import Secret
from prefect.tasks import task_input_hash
import os
from typing import List
from datetime import timedelta
import asyncio

@task(
    retries=2,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    persist_result=False
)
async def process_client(client: str, gcp_project: str, profiles_dir: str) -> None:
    """Process a single client using dbt."""
    logger = get_run_logger()
    logger.info(f"Starting processing for client: {client}")
    
    try:
        # Set environment variables
        os.environ["DBT_CLIENT_DATASET"] = client
        os.environ["DBT_BIGQUERY_PROJECT"] = gcp_project
        
        # Run dbt models for this client
        result = await shell_run_command(
            command=f"dbt run --profiles-dir {profiles_dir} --target service_account",
            return_all=True,
            stream_level=20  # INFO level logging
        )
        logger.info(f"Successfully completed processing for {client}")
        return result
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
async def process_all_clients(
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
    
    for client in clients:
        await process_client(client, gcp_project, profiles_dir)
    
    logger.info("Completed processing all clients")

if __name__ == "__main__":
    asyncio.run(process_all_clients()) 