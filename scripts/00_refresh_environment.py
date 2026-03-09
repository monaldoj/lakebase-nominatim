#!/usr/bin/env python3
"""
Refresh Databricks database token and update .env file.

This script fetches a fresh OAuth token from Databricks WorkspaceClient
and updates the PGPASSWORD in the .env file.

Requirements:
    pip install databricks-sdk python-dotenv
"""

import os
import sys
import uuid
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient


def get_env_file_path():
    """Get the path to the .env file in the project root."""
    # Assume script is in scripts/ directory, so go up one level
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    return project_root / ".env"


def get_fresh_db_token(instance_name: str):
    """
    Generate a fresh database credential token from Databricks.

    Uses DATABRICKS_HOST and DATABRICKS_TOKEN from environment.
    Returns a 1-hour Lakebase OAuth token.
    """
    print(f"Requesting fresh token for instance: {instance_name}")
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    return cred.token


def update_env_file(env_path: Path, new_token: str):
    """
    Update the PGPASSWORD in the .env file with the new token.
    """
    if not env_path.exists():
        print(f"ERROR: .env file not found at {env_path}")
        sys.exit(1)

    # Read existing .env file
    with open(env_path, 'r') as f:
        lines = f.readlines()

    # Update PGPASSWORD line
    updated = False
    for i, line in enumerate(lines):
        if line.startswith('PGPASSWORD='):
            lines[i] = f'PGPASSWORD={new_token}\n'
            updated = True
            break

    if not updated:
        # If PGPASSWORD doesn't exist, add it after PGUSER
        for i, line in enumerate(lines):
            if line.startswith('PGUSER='):
                lines.insert(i + 1, f'PGPASSWORD={new_token}\n')
                updated = True
                break

    if not updated:
        # If still not updated, append to end
        lines.append(f'PGPASSWORD={new_token}\n')

    # Write updated .env file
    with open(env_path, 'w') as f:
        f.writelines(lines)

    print(f"✓ Updated PGPASSWORD in {env_path}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("Databricks Database Token Refresh")
    print("=" * 60)
    print()

    # Get .env file path
    env_path = get_env_file_path()

    # Load environment variables
    load_dotenv(env_path)

    # Get instance name from environment
    instance_name = os.getenv("PGINSTANCENAME")
    if not instance_name:
        print("ERROR: PGINSTANCENAME not found in .env file")
        print(f"Please add PGINSTANCENAME to {env_path}")
        sys.exit(1)

    # Check for required Databricks credentials
    if not os.getenv("DATABRICKS_HOST"):
        print("ERROR: DATABRICKS_HOST not found in environment")
        print("Please set DATABRICKS_HOST environment variable")
        sys.exit(1)

    if not os.getenv("DATABRICKS_TOKEN"):
        print("ERROR: DATABRICKS_TOKEN not found in environment")
        print("Please set DATABRICKS_TOKEN environment variable")
        sys.exit(1)

    print(f"Environment file: {env_path}")
    print(f"Database instance: {instance_name}")
    print()

    try:
        # Get fresh token
        new_token = get_fresh_db_token(instance_name)
        print("✓ Fresh token generated successfully")
        print()

        # Update .env file
        update_env_file(env_path, new_token)
        print()

        print("=" * 60)
        print("✓ Token refresh completed successfully!")
        print("=" * 60)
        print()
        print("The new token is valid for 1 hour.")

    except Exception as e:
        print(f"✗ ERROR: Failed to refresh token: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
