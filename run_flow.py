#!/usr/bin/env python
import os
import sys
import importlib.util
import subprocess
from pathlib import Path

# Determine the location of the script directory relative to this file
repo_root = Path(__file__).parent
scripts_dir = repo_root / "scripts"

def main():
    print(f"Current directory: {os.getcwd()}")
    print(f"Script directory: {scripts_dir}")
    
    # Ensure scripts_dir is in sys.path
    sys.path.insert(0, str(scripts_dir))

    # Check if run_clients_flow.py exists in the scripts directory
    flow_path = scripts_dir / "run_clients_flow.py"
    if not flow_path.exists():
        print(f"Error: Could not find {flow_path}")
        # List files in scripts directory
        print("Files in scripts directory:")
        try:
            subprocess.run(["ls", "-la", str(scripts_dir)])
        except Exception as e:
            print(f"Error listing files: {e}")
        sys.exit(1)

    # Import the flow
    try:
        spec = importlib.util.spec_from_file_location("run_clients_flow", flow_path)
        flow_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(flow_module)
        
        # Run the process_all_clients function
        flow_module.process_all_clients()
    except Exception as e:
        print(f"Error importing or running flow: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 