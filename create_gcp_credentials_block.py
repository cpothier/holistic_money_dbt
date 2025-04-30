#!/usr/bin/env python3
import json
from prefect_gcp import GcpCredentials

# Load service account file path
service_account_file = "credentials/service-account.json"

# Create credentials block
gcp_credentials = GcpCredentials(
    service_account_file=service_account_file
)

# Save block
gcp_credentials.save("holistic-money-credentials", overwrite=True)

print(f"GCP credentials block 'holistic-money-credentials' created successfully!") 