#!/usr/bin/env python3
import os
import sys

# Create the profiles directory
home_dir = os.path.expanduser("~")
profiles_dir = os.path.join(home_dir, ".dbt")
os.makedirs(profiles_dir, exist_ok=True)

# Get absolute path to project root and credentials
current_dir = os.path.dirname(os.path.abspath(__file__))
creds_path = os.path.join(current_dir, "credentials", "service-account.json")

# Create the profiles.yml content
profiles_content = f"""holistic_money_dw:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: "holistic-money"
      dataset: "default_client"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
      
    service_account:
      type: bigquery
      method: service-account
      project: "holistic-money"
      dataset: "{{{{ env_var('DBT_CLIENT_DATASET', 'default_client') }}}}"
      keyfile: "{creds_path}"
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
"""

# Write the profiles.yml file
profiles_file = os.path.join(profiles_dir, "profiles.yml")
with open(profiles_file, "w") as f:
    f.write(profiles_content)

print(f"Updated dbt profile at {profiles_file}")
print(f"Service account keyfile path: {creds_path}") 