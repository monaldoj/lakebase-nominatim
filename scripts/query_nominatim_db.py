#!/usr/bin/env python3
"""
Direct Nominatim database query script with OAuth token authentication.

This script connects directly to a Databricks Postgres instance running Nominatim
and executes geocoding queries using psycopg and OAuth tokens.
"""

import os
import sys
import uuid
import argparse
import logging
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import psycopg

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def get_fresh_oauth_token(instance_name: str) -> str:
    """
    Generate a fresh OAuth token from Databricks.

    Returns a 1-hour Lakebase OAuth token.
    """
    try:
        from databricks.sdk import WorkspaceClient
        logger.info(f"Fetching fresh OAuth token for instance: {instance_name}")
        w = WorkspaceClient()
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name],
        )
        logger.info("Fresh OAuth token obtained successfully")
        return cred.token
    except Exception as e:
        logger.error(f"Could not fetch fresh token: {e}")
        raise


def get_db_connection():
    """Get a psycopg connection to the Nominatim database."""
    # Get connection parameters from environment
    instance_name = os.getenv("PGINSTANCENAME")
    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    database = os.getenv("PGDATABASE", "nominatim")
    user = os.getenv("PGUSER")
    sslmode = os.getenv("PGSSLMODE", "require")

    if not all([instance_name, host, user]):
        raise ValueError("Missing required environment variables: PGINSTANCENAME, PGHOST, PGUSER")

    # Get fresh OAuth token
    password = get_fresh_oauth_token(instance_name)

    # Create connection
    logger.info(f"Connecting to {host}:{port}/{database}")
    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        sslmode=sslmode,
    )
    logger.info("Database connection established")
    return conn


def search_places(conn, query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Search for places matching the query (forward geocoding).

    Args:
        conn: psycopg connection
        query: Search query string
        limit: Maximum number of results

    Returns:
        List of place dictionaries
    """
    logger.info(f"Searching for: '{query}'")

    cursor = conn.cursor()

    sql = """
        SELECT
            place_id,
            osm_type,
            osm_id,
            ST_Y(centroid) as lat,
            ST_X(centroid) as lon,
            name->'name' as display_name,
            class,
            type,
            importance
        FROM placex
        WHERE name->'name' ILIKE %s
        ORDER BY importance DESC NULLS LAST
        LIMIT %s
    """

    cursor.execute(sql, (f'%{query}%', limit))
    rows = cursor.fetchall()

    places = []
    for row in rows:
        place = {
            "place_id": row[0],
            "osm_type": row[1],
            "osm_id": row[2],
            "lat": str(row[3]) if row[3] else None,
            "lon": str(row[4]) if row[4] else None,
            "display_name": row[5] or query,
            "class": row[6],
            "type": row[7],
            "importance": float(row[8]) if row[8] else 0.5,
        }
        places.append(place)

    logger.info(f"Found {len(places)} results")
    return places


def reverse_geocode(conn, lat: float, lon: float) -> Optional[Dict[str, Any]]:
    """
    Reverse geocode coordinates to find the nearest place.

    Args:
        conn: psycopg connection
        lat: Latitude
        lon: Longitude

    Returns:
        Place dictionary or None
    """
    logger.info(f"Reverse geocoding: ({lat}, {lon})")

    cursor = conn.cursor()

    sql = """
        SELECT
            place_id,
            osm_type,
            osm_id,
            ST_Y(centroid) as lat,
            ST_X(centroid) as lon,
            name->'name' as display_name,
            class,
            type
        FROM placex
        WHERE ST_DWithin(centroid, ST_SetSRID(ST_MakePoint(%s, %s), 4326), 0.1)
        ORDER BY ST_Distance(centroid, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
    """

    cursor.execute(sql, (lon, lat, lon, lat))
    row = cursor.fetchone()

    if not row:
        logger.info("No result found")
        return None

    place = {
        "place_id": row[0],
        "osm_type": row[1],
        "osm_id": row[2],
        "lat": str(row[3]) if row[3] else str(lat),
        "lon": str(row[4]) if row[4] else str(lon),
        "display_name": row[5] or f"{lat}, {lon}",
        "class": row[6],
        "type": row[7],
    }

    logger.info(f"Found: {place['display_name']}")
    return place


def main():
    parser = argparse.ArgumentParser(
        description="Query Nominatim database directly with OAuth authentication"
    )

    # Subcommands for different query types
    subparsers = parser.add_subparsers(dest='command', help='Query type')

    # Search command
    search_parser = subparsers.add_parser('search', help='Forward geocoding (search by address/name)')
    search_parser.add_argument('query', type=str, help='Search query (e.g., "San Francisco")')
    search_parser.add_argument('--limit', type=int, default=10, help='Maximum number of results')

    # Reverse command
    reverse_parser = subparsers.add_parser('reverse', help='Reverse geocoding (coordinates to address)')
    reverse_parser.add_argument('lat', type=float, help='Latitude')
    reverse_parser.add_argument('lon', type=float, help='Longitude')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        # Connect to database
        conn = get_db_connection()

        # Execute query based on command
        if args.command == 'search':
            results = search_places(conn, args.query, args.limit)

            if not results:
                print(f"\nNo results found for: {args.query}")
            else:
                print(f"\nFound {len(results)} results for: {args.query}\n")
                for i, place in enumerate(results, 1):
                    print(f"{i}. {place['display_name']}")
                    print(f"   Location: {place['lat']}, {place['lon']}")
                    print(f"   Type: {place['class']}/{place['type']}")
                    print(f"   OSM: {place['osm_type']}{place['osm_id']}")
                    print(f"   Importance: {place['importance']:.3f}")
                    print()

        elif args.command == 'reverse':
            result = reverse_geocode(conn, args.lat, args.lon)

            if not result:
                print(f"\nNo result found for coordinates: {args.lat}, {args.lon}")
            else:
                print(f"\nReverse geocoding result:\n")
                print(f"Location: {result['display_name']}")
                print(f"Coordinates: {result['lat']}, {result['lon']}")
                print(f"Type: {result['class']}/{result['type']}")
                print(f"OSM: {result['osm_type']}{result['osm_id']}")

        # Close connection
        conn.close()
        logger.info("Connection closed")

    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
