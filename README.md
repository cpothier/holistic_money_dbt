# Holistic Money dbt Project

This dbt Core project manages the transformation of QuickBooks and budget data into clean financial models with support for accountant comments.

## Project Structure

- **staging**: Initial cleanup of raw data
  - `stg_budget_template.sql`: Converts raw Google Sheet data to structured table
  
- **core**: Core business logic transformations
  - `budget_transformed.sql`: Unpivots budget data to be queryable by date
  - `p_l_view.sql`: Comprehensive P&L view combining all QuickBooks transaction types
  
- **marts**: Final presentation-ready models
  - `financial_comments.sql`: Table for storing accountant comments
  - `materialized_pl_budget_blend.sql`: Materialized table combining P&L and budget data
  - `pl_budget_with_comments.sql`: View joining financial data and comments
  - `profit_by_month.sql`: Monthly profit analysis

- **macros**: Reusable code
  - `create_external_table.sql`: Creates external connection to Google Sheets

## Setup

1. **Configure your dbt profile in `~/.dbt/profiles.yml`**:

   Copy the profiles.yml file from this project to your ~/.dbt directory:
   
   ```bash
   mkdir -p ~/.dbt
   cp holistic_money_dbt/profiles.yml ~/.dbt/profiles.yml
   ```
   
   Then edit it to add your specific GCP project ID:
   
   ```yaml
   holistic_money_dw:
     target: dev
     outputs:
       dev:
         type: bigquery
         method: oauth
         project: "your-gcp-project-id"  # <-- Update this
         dataset: "{{ env_var('DBT_CLIENT_DATASET', 'default_client') }}"
         threads: 4
         timeout_seconds: 300
         location: US
         priority: interactive
   ```

2. **Connect to BigQuery**:

   Choose one of these authentication methods:
   
   **a) OAuth (Interactive Development)**
   ```bash
   # Install Google Cloud SDK
   # https://cloud.google.com/sdk/docs/install
   
   # Log in interactively
   gcloud auth application-default login
   ```
   
   **b) Service Account (Automation/CI)**
   ```bash
   # Create service account key in GCP console
   # Download the JSON key file
   
   # Set environment variable to key file
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
   ```

3. **Setup Individual Client**:
   ```bash
   cd holistic_money_dbt
   ./setup_client.py \
     --client CLIENT_NAME \
     --project YOUR_GCP_PROJECT \
     --budget-sheet-url "https://docs.google.com/spreadsheets/d/SHEET_ID/edit" \
     --sheet-range "Budget Summary!A4:AS69"
   ```

4. **Run Models for All Clients**:
   ```bash
   cd holistic_money_dbt/scripts
   ./run_all_clients.sh
   ```

## Running the Data Warehouse

### Refresh Data for All Clients
To refresh data for all clients listed in `clients.txt`:
```bash
cd holistic_money_dbt/scripts
./run_all_clients.sh
```

### Refresh Data for a Single Client
To refresh data for a specific client:
```bash
cd holistic_money_dbt/scripts
./refresh_client.sh CLIENT_NAME
```
Example:
```bash
./refresh_client.sh golden_hour
```

### Running Individual dbt Commands
If you need to run specific dbt commands for a client:
```bash
cd holistic_money_dbt
export DBT_CLIENT_DATASET=golden_hour
export DBT_BIGQUERY_PROJECT=holistic-money
dbt run --target service_account
```

## Key Features

1. **Client Parameterization**: Easily switch between clients using variables
2. **Comment Preservation**: Uses merge logic to maintain comment relationships even as data changes
3. **Modular Design**: Clean separation between staging, core logic, and presentation
4. **Automated Setup**: Python wrapper for easy client onboarding

## Running Daily Refreshes

The models are designed to handle daily data refreshes. To update, run:

```bash
DBT_CLIENT_DATASET=CLIENT_NAME DBT_BIGQUERY_PROJECT=GCP_PROJECT_ID dbt run
```

This will:
1. Read the latest QuickBooks data that Airbyte has loaded
2. Update the P&L calculations
3. Refresh the materialized tables while preserving comment relationships 