#!/bin/bash

# List of clients
CLIENTS=(
  "golden_hour"
  "austin_lifestyler" 
  "bb_design"
  "radical_transformation"
  "western_holistic_med"
)

# GCP Project
GCP_PROJECT="holistic_money"  # Replace with your actual GCP project ID

# dbt profiles directory
PROFILES_DIR="$HOME/.dbt"

# Process each client
for CLIENT in "${CLIENTS[@]}"; do
  echo "Processing client: $CLIENT"
  
  # Set environment variables
  export DBT_CLIENT_DATASET=$CLIENT
  export DBT_BIGQUERY_PROJECT=$GCP_PROJECT
  
  # Run dbt models for this client
  dbt run --profiles-dir $PROFILES_DIR
  
  echo "Completed processing for $CLIENT"
  echo "---------------------------------"
done

echo "All clients processed successfully!" 