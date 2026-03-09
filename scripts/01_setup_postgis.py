#!/usr/bin/env python3
"""
Script to set up PostGIS extensions on Lakebase Managed Postgres.

This script connects to the PostgreSQL database and installs the required
PostGIS and hstore extensions needed for Nominatim.
"""

import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()


def main():
    """Set up PostGIS extensions."""
    # Get database connection parameters from environment
    conn_params = {
        "host": os.getenv("PGHOST", "localhost"),
        "port": os.getenv("PGPORT", "5432"),
        "user": os.getenv("PGUSER", "postgres"),
        "password": os.getenv("PGPASSWORD", ""),
        "database": os.getenv("PGDATABASE", "postgres"),
        "sslmode": os.getenv("PGSSLMODE", "prefer"),
    }

    print("=" * 60)
    print("PostGIS Setup for Nominatim")
    print("=" * 60)
    print(f"\nConnecting to database:")
    print(f"  Host: {conn_params['host']}")
    print(f"  Database: {conn_params['database']}")
    print(f"  User: {conn_params['user']}")
    print(f"  SSL Mode: {conn_params['sslmode']}")
    print()

    target_db = conn_params["database"]

    try:
        # Connect to the postgres maintenance database first to check/create target DB
        print("Connecting to PostgreSQL (maintenance database)...")
        maintenance_params = {**conn_params, "database": "postgres"}
        conn = psycopg2.connect(**maintenance_params)
        conn.autocommit = True
        cursor = conn.cursor()

        print("Connected successfully!\n")

        # Check PostgreSQL version
        print("Checking PostgreSQL version...")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"  {version}\n")

        # Check if target database exists; create it if not
        print(f"Checking if database '{target_db}' exists...")
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
        if cursor.fetchone():
            print(f"  Database '{target_db}' already exists")
        else:
            print(f"  Creating database '{target_db}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
            print(f"  Database '{target_db}' created successfully")

        cursor.close()
        conn.close()

        # Reconnect to the target database to install extensions
        print(f"\nConnecting to database '{target_db}'...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        print("Connected successfully!\n")

        # Install PostGIS extension
        print("Installing PostGIS extension...")
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
            print("  PostGIS extension created successfully")
        except psycopg2.Error as e:
            print(f"  Error creating PostGIS extension: {e}")
            print("  Note: You may need admin privileges to install extensions.")
            sys.exit(1)

        # Install hstore extension
        print("Installing hstore extension...")
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS hstore;")
            print("  hstore extension created successfully")
        except psycopg2.Error as e:
            print(f"  Error creating hstore extension: {e}")
            print("  Note: You may need admin privileges to install extensions.")
            sys.exit(1)

        # Verify extensions
        print("\nVerifying installed extensions...")
        cursor.execute("""
            SELECT extname, extversion
            FROM pg_extension
            WHERE extname IN ('postgis', 'hstore')
            ORDER BY extname;
        """)
        extensions = cursor.fetchall()

        if len(extensions) == 2:
            print("  Extensions verified successfully:")
            for ext_name, ext_version in extensions:
                print(f"    - {ext_name}: version {ext_version}")
        else:
            print("  Warning: Not all required extensions are installed")
            for ext_name, ext_version in extensions:
                print(f"    - {ext_name}: version {ext_version}")

        # Check PostGIS functions
        print("\nChecking PostGIS functionality...")
        cursor.execute("SELECT PostGIS_Version();")
        postgis_version = cursor.fetchone()[0]
        print(f"  PostGIS version: {postgis_version}")

        # Create www-data user required by Nominatim
        print("\nCreating www-data user for Nominatim...")
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = 'www-data';")
        if cursor.fetchone():
            print("  www-data user already exists")
        else:
            cursor.execute('CREATE USER "www-data";')
            print("  www-data user created successfully")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("PostGIS setup completed successfully!")
        print("=" * 60)
        print("\nYou can now proceed with importing OSM data using the")
        print("import_osm_data.py script.")

    except psycopg2.Error as e:
        print(f"\nDatabase error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
