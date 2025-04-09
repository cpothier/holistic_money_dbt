#!/bin/bash

# Check if client parameter is provided
if [ -z "$1" ]; then
    echo "Error: Client name parameter is required"
    echo "Usage: ./refresh_client.sh CLIENT_NAME [PROJECT_ID]"
    exit 1
fi

# Set client name from parameter
CLIENT_NAME=$1

# Set project ID from parameter or use default
PROJECT_ID=${2:-"holistic-money"}

echo "Refreshing data warehouse for client: $CLIENT_NAME in project: $PROJECT_ID"

# Change to the dbt project directory (assuming script is run from scripts/ directory)
cd "$(dirname "$0")/.." || exit

# Create environment variables for dbt
export DBT_CLIENT_DATASET=$CLIENT_NAME
export DBT_BIGQUERY_PROJECT=$PROJECT_ID

# Run dbt models for this client
dbt run --profiles-dir $HOME/.dbt --target service_account

echo "Data warehouse refresh completed for $CLIENT_NAME in project $PROJECT_ID" 