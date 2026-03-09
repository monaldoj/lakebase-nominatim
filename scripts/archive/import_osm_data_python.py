#!/usr/bin/env python3
"""
Python-based OSM data import script for Nominatim.

This script uses psycopg2 and the Nominatim Python API directly,
bypassing PostgreSQL CLI tools that don't work with OAuth tokens.

Requirements:
    pip install nominatim-db psycopg2-binary python-dotenv
"""

import os
import sys
import subprocess
import argparse
import tempfile
import shutil
import uuid
import psycopg
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

# Load environment variables
load_dotenv()


# Popular OSM data sources from Geofabrik
OSM_SOURCES = {
    # Small test datasets
    "monaco": "https://download.geofabrik.de/europe/monaco-latest.osm.pbf",
    "liechtenstein": "https://download.geofabrik.de/europe/liechtenstein-latest.osm.pbf",

    # Country examples
    "germany": "https://download.geofabrik.de/europe/germany-latest.osm.pbf",
    "france": "https://download.geofabrik.de/europe/france-latest.osm.pbf",
    "italy": "https://download.geofabrik.de/europe/italy-latest.osm.pbf",
    "spain": "https://download.geofabrik.de/europe/spain-latest.osm.pbf",
    "uk": "https://download.geofabrik.de/europe/great-britain-latest.osm.pbf",
    "usa": "https://download.geofabrik.de/north-america/us-latest.osm.pbf",
    "canada": "https://download.geofabrik.de/north-america/canada-latest.osm.pbf",

    # US States
    "california": "https://download.geofabrik.de/north-america/us/california-latest.osm.pbf",
    "virginia": "https://download.geofabrik.de/north-america/us/virginia-latest.osm.pbf",
    "maryland": "https://download.geofabrik.de/north-america/us/maryland-latest.osm.pbf",
    "new-york": "https://download.geofabrik.de/north-america/us/new-york-latest.osm.pbf",
    "texas": "https://download.geofabrik.de/north-america/us/texas-latest.osm.pbf",
}


def check_nominatim_installed():
    """Check if nominatim-db package is installed."""
    try:
        import nominatim_db
        return True
    except ImportError:
        return False

def get_fresh_db_token(instance_name: str):
    # Uses DATABRICKS_HOST/DATABRICKS_TOKEN from the environment
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    return cred.token  # 1-hour Lakebase OAuth token

def get_db_connection():
    instance_name = os.getenv("PGINSTANCENAME", "")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    database = os.getenv("PGDATABASE", "nominatim")
    sslmode = os.getenv("PGSSLMODE", "require")

    conn = psycopg.connect(
        host=host,
        user=user,
        password=get_fresh_db_token(instance_name), # Pass the function, not the string
        dbname=database
    )

    return conn

def verify_database_connection():
    """Verify database connection using psycopg3 (handles OAuth tokens)."""
    try:
        import psycopg
    except ImportError:
        print("ERROR: psycopg3 not installed. Install with: pip install psycopg-binary")
        return False

    print("Verifying database connection...")

    try:
        # Try to connect
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✓ Connected to PostgreSQL: {version[0][:50]}...")

        # Check PostGIS
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1 FROM pg_extension WHERE extname = 'postgis'
            );
        """)
        has_postgis = cursor.fetchone()[0]

        if has_postgis:
            print("✓ PostGIS extension is installed")
        else:
            print("⚠ WARNING: PostGIS extension not found!")
            print("  Run: python scripts/setup_postgis.py")
            return False

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False


# def create_nominatim_project(project_dir: Path):
#     """Create a Nominatim project directory with configuration."""
#     project_dir.mkdir(parents=True, exist_ok=True)

#     # Create .env file with individual PostgreSQL environment variables
#     # DO NOT use DATABASE_DSN as it causes issues with OAuth tokens
#     env_content = f"""# PostgreSQL Connection (using individual variables to avoid OAuth token issues)
# PGHOST={os.getenv("PGHOST", "localhost")}
# PGPORT={os.getenv("PGPORT", "5432")}
# PGUSER={os.getenv("PGUSER", "postgres")}
# PGPASSWORD={os.getenv("PGPASSWORD", "")}
# PGDATABASE={os.getenv("PGDATABASE", "nominatim")}
# PGSSLMODE={os.getenv("PGSSLMODE", "prefer")}

# # Nominatim Settings
# NOMINATIM_IMPORT_THREADS=2
# NOMINATIM_TOKENIZER=icu

# # IMPORTANT: Do NOT use DATABASE_DSN with Databricks OAuth tokens
# # The CLI tools will misparse it. Use the PG* variables above instead.
# """

#     env_file = project_dir / ".env"
#     with open(env_file, "w") as f:
#         f.write(env_content)

#     print(f"✓ Created Nominatim project directory: {project_dir}")
#     print(f"✓ Using PostgreSQL connection: {os.getenv('PGHOST')}:{os.getenv('PGPORT')}")
#     return project_dir


def download_osm_file(url: str, output_dir: Path) -> Path:
    """Download OSM data file."""
    filename = url.split("/")[-1]
    output_path = output_dir / filename

    if output_path.exists():
        print(f"  File already exists: {output_path}")
        response = input("  Use existing file? (y/n): ").lower()
        if response == 'y':
            return output_path
        print("  Deleting existing file...")
        output_path.unlink()

    print(f"  Downloading from: {url}")
    print(f"  Saving to: {output_path}")

    try:
        subprocess.run(
            ["curl", "-L", "-o", str(output_path), url],
            check=True,
        )
        print("  ✓ Download completed successfully")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Download failed: {e}")
        sys.exit(1)


def import_with_python_api(osm_file: Path, project_dir: Path, threads: int = 2):
    """Import OSM data using Nominatim Python API."""
    try:
        from nominatim_db.tools import exec_utils
        from nominatim_db.clicmd.api import APIImport
        from nominatim_db.config import Configuration

        print("\nInitializing Nominatim configuration...")

        # Initialize configuration
        config = Configuration(project_dir)

        print(f"\nImporting OSM data: {osm_file}")
        print("This may take a while depending on the data size...")
        print()

        # Create import command
        import_cmd = APIImport()

        # Set up arguments
        class Args:
            osm_file = str(osm_file)
            threads = threads
            continue_at = None
            prepare_database = True
            load_data = True
            index = True
            create_search_indices = True

        args = Args()

        # Run import
        result = import_cmd.run(args, config)

        if result == 0:
            print("\n" + "=" * 60)
            print("✓ OSM data import completed successfully!")
            print("=" * 60)
            return True
        else:
            print(f"\n✗ Import failed with code: {result}")
            return False

    except ImportError as e:
        print(f"✗ Nominatim Python API not available: {e}")
        print("\nInstall with: pip install nominatim-db")
        return False
    except Exception as e:
        print(f"\n✗ Import failed: {e}")
        return False


def prepare_database_for_nominatim():
    """
    Pre-create database and extensions using psycopg2 (handles OAuth tokens).
    This bypasses the need for Nominatim to call createdb.
    """
    try:
        # Connect to check if database exists and has required extensions
        print("Checking database setup...")
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        # Check for required extensions
        cursor.execute("SELECT extname FROM pg_extension WHERE extname IN ('postgis', 'hstore');")
        extensions = [row[0] for row in cursor.fetchall()]

        if 'postgis' not in extensions:
            print("⚠ PostGIS extension not found. Installing...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            print("✓ PostGIS extension created")

        if 'hstore' not in extensions:
            print("⚠ hstore extension not found. Installing...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
            print("✓ hstore extension created")

        cursor.close()
        conn.close()
        print("✓ Database is ready for Nominatim import")
        return True

    except Exception as e:
        print(f"✗ Database preparation failed: {e}")
        return False


def import_with_python_wrapper(osm_file: Path, project_dir: Path, threads: int = 2):
    """
    Import using a Python wrapper around Nominatim CLI.

    This creates a wrapper script that sets up the environment properly
    and calls Nominatim in a way that works with OAuth tokens.
    """
    print(f"\nImporting OSM data: {osm_file}")
    print(f"Project directory: {project_dir}")
    print(f"Threads: {threads}")
    print()

    # Prepare database first using psycopg2 (handles OAuth tokens)
    if not prepare_database_for_nominatim():
        print("\n⚠ Database preparation failed. Import may fail.")
        response = input("Continue anyway? (y/n): ").lower()
        if response != 'y':
            return False

    print("\nStarting OSM data import...")
    print("This may take a while depending on the data size...")
    print()

    try:
        # # Change to project directory
        # original_cwd = os.getcwd()
        # os.chdir(project_dir)

        # Run nominatim import using the project's .env
        # Skip prepare-database since we already did it with psycopg2
        cmd = [
            "nominatim",
            "import",
            "--osm-file", str(osm_file),
            "--threads", str(threads),
            "--prepare-database",  # This should work now with PG* env vars
        ]

        print(f"Running: {' '.join(cmd)}")
        # print(f"Working directory: {project_dir}")
        print()

        # Nominatim will read database connection from .env in current directory
        result = subprocess.run(cmd, check=True)

        # os.chdir(original_cwd)

        print("\n" + "=" * 60)
        print("✓ OSM data import completed successfully!")
        print("=" * 60)
        return True

    except subprocess.CalledProcessError as e:
        # os.chdir(original_cwd)
        print(f"\n✗ Import failed: {e}")
        print("\n" + "=" * 60)
        print("TROUBLESHOOTING: OAuth Token / Port Number Errors")
        print("=" * 60)
        print("\nIf you see errors about port numbers or connection strings:")
        print("1. This means Nominatim CLI can't handle Databricks OAuth tokens")
        print("2. Try the Python API method instead:")
        print(f"   python scripts/import_osm_data_python.py --file {osm_file} --method api")
        print("\nAlternative solutions:")
        print("• Import on a standard PostgreSQL instance, then pg_dump/pg_restore to Databricks")
        print("• Request standard password authentication (not OAuth) from Databricks")
        print("• Use a pre-populated Nominatim database image")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Import OpenStreetMap data for Nominatim (Python-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Import Monaco (small test dataset)
  python import_osm_data_python.py --region monaco

  # Import Germany
  python import_osm_data_python.py --region germany

  # Import from custom URL
  python import_osm_data_python.py --url https://download.geofabrik.de/europe/switzerland-latest.osm.pbf

  # Import existing local file
  python import_osm_data_python.py --file /path/to/data.osm.pbf

Available regions:
  """ + "\n  ".join(sorted(OSM_SOURCES.keys()))
    )

    parser.add_argument(
        "--region",
        choices=list(OSM_SOURCES.keys()),
        help="Predefined region to download",
    )
    parser.add_argument(
        "--url",
        help="Custom URL to download OSM data from",
    )
    parser.add_argument(
        "--file",
        type=Path,
        help="Path to existing OSM file",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./osm_data"),
        help="Directory to store downloaded OSM files (default: ./osm_data)",
    )
    # parser.add_argument(
    #     "--project-dir",
    #     type=Path,
    #     default=Path("./nominatim_project"),
    #     help="Nominatim project directory (default: ./nominatim_project)",
    # )
    parser.add_argument(
        "--threads",
        type=int,
        default=2,
        help="Number of threads for import (default: 2)",
    )
    parser.add_argument(
        "--method",
        choices=["python", "cli"],
        default="cli",
        help="Import method: 'python' (Python API) or 'cli' (CLI wrapper) [default: cli]",
    )

    args = parser.parse_args()

    # Validate arguments
    if not any([args.region, args.url, args.file]):
        parser.error("Must specify either --region, --url, or --file")

    print("=" * 60)
    print("Nominatim OSM Data Import (Python-based)")
    print("=" * 60)
    print()

    # Check if nominatim is available
    if not check_nominatim_installed():
        print("ERROR: nominatim-db package not found!")
        print("\nInstall with:")
        print("  pip install nominatim-db")
        print("\nOr add to requirements.txt:")
        print("  nominatim-db")
        sys.exit(1)

    print("✓ nominatim-db package is installed")
    print()

    # Verify database connection
    if not verify_database_connection():
        print("\n✗ Database connection failed. Please check your .env configuration.")
        sys.exit(1)

    print()

    # # Create Nominatim project directory
    # project_dir = create_nominatim_project(args.project_dir)
    # print()

    # Determine OSM file
    osm_file = None

    if args.file:
        # Use existing file
        osm_file = args.file
        if not osm_file.exists():
            print(f"ERROR: File not found: {osm_file}")
            sys.exit(1)
    else:
        # Download file
        args.output_dir.mkdir(parents=True, exist_ok=True)

        if args.region:
            url = OSM_SOURCES[args.region]
            print(f"Downloading OSM data for: {args.region}")
        elif args.url:
            url = args.url
            print(f"Downloading OSM data from: {url}")

        osm_file = download_osm_file(url, args.output_dir)
        print()

    # Import data
    if args.method == "api":
        success = import_with_python_api(osm_file, './', args.threads)
    else:
        success = import_with_python_wrapper(osm_file, './', args.threads)

    if success:
        print("\n✓ The Nominatim database is now ready to use.")
        print("  Start the FastAPI application:")
        print("  uvicorn app:app --reload")
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
