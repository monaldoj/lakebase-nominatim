"""Nominatim service wrapper for geocoding operations."""

import asyncio
import logging
import os
import uuid
import json
import subprocess
from typing import Optional, List, Dict, Any
import psycopg

from src.config.settings import settings

logger = logging.getLogger(__name__)


def get_fresh_db_token(instance_name: str) -> str:
    """
    Generate a fresh database credential token from Databricks.

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
        logger.warning(f"Could not fetch fresh token: {e}. Using token from .env")
        return settings.pg_password


class NominatimService:
    """Wrapper for Nominatim operations using direct psycopg connections."""

    def __init__(self):
        """Initialize the Nominatim service."""
        self.conn_params = None
        self._initialized = False
        self._fresh_token = None

    def initialize(self) -> None:
        """Initialize connection parameters for Nominatim database."""
        try:
            logger.info("Initializing Nominatim service...")
            logger.info(f"Target database: {settings.pg_host}:{settings.pg_port}/{settings.pg_database}")

            # Get fresh OAuth token if PGINSTANCENAME is set
            instance_name = os.getenv("PGINSTANCENAME")
            if instance_name and os.getenv("DATABRICKS_HOST"):
                self._fresh_token = get_fresh_db_token(instance_name)
            else:
                # Use token from .env
                self._fresh_token = settings.pg_password
                logger.info("Using database token from .env file")

            # Store connection parameters
            self.conn_params = {
                'host': settings.pg_host,
                'port': settings.pg_port,
                'dbname': settings.pg_database,
                'user': settings.pg_user,
                'password': self._fresh_token,
                'sslmode': settings.pg_sslmode,
            }

            self._initialized = True
            logger.info("Nominatim service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Nominatim API: {e}")
            raise

    def check_initialized(self) -> None:
        """Check if the service is initialized."""
        if not self._initialized or self.conn_params is None:
            raise RuntimeError("Nominatim service not initialized. Call initialize() first.")

    def _get_connection(self):
        """Get a fresh psycopg connection with OAuth token."""
        return psycopg.connect(**self.conn_params)

    async def search(
        self,
        query: str,
        limit: int = 10,
        country_codes: Optional[List[str]] = None,
        bounded: bool = False,
        dedupe: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Search for places matching the query (forward geocoding).
        Uses direct database queries via psycopg.
        """
        self.check_initialized()

        try:
            logger.info(f"Searching for: {query}")

            # Run query in thread pool to avoid blocking
            return await asyncio.to_thread(
                self._search_sync, query, limit, country_codes
            )

        except Exception as e:
            logger.error(f"Search error for query '{query}': {e}")
            raise

    def _search_sync(self, query: str, limit: int, country_codes: Optional[List[str]]) -> List[Dict[str, Any]]:
        """Synchronous search using direct SQL query."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Use Nominatim's place table with text search
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

            logger.info(f"Found {len(places)} results for query: {query}")
            return places

        finally:
            conn.close()

    async def reverse(
        self,
        lat: float,
        lon: float,
        zoom: int = 18,
    ) -> Optional[Dict[str, Any]]:
        """
        Reverse geocode coordinates to address.
        Uses direct database queries via psycopg.
        """
        self.check_initialized()

        try:
            logger.info(f"Reverse geocoding: {lat}, {lon}")

            # Run query in thread pool
            return await asyncio.to_thread(
                self._reverse_sync, lat, lon, zoom
            )

        except Exception as e:
            logger.error(f"Reverse geocoding error for ({lat}, {lon}): {e}")
            raise

    def _reverse_sync(self, lat: float, lon: float, zoom: int) -> Optional[Dict[str, Any]]:
        """Synchronous reverse geocoding using direct SQL query."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Find nearest place to the given coordinates
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
                logger.info(f"No result found for coordinates: {lat}, {lon}")
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

            logger.info(f"Reverse geocoding successful: {place['display_name']}")
            return place

        finally:
            conn.close()

    async def lookup(
        self,
        osm_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Look up places by OSM ID.
        Uses direct database queries via psycopg.
        """
        self.check_initialized()

        try:
            logger.info(f"Looking up OSM IDs: {osm_ids}")
            return await asyncio.to_thread(self._lookup_sync, osm_ids)

        except Exception as e:
            logger.error(f"Lookup error for OSM IDs {osm_ids}: {e}")
            raise

    def _lookup_sync(self, osm_ids: List[str]) -> List[Dict[str, Any]]:
        """Synchronous lookup using direct SQL query."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            places = []

            for osm_id in osm_ids:
                # Parse OSM ID (e.g., "R123456" -> type='R', id=123456)
                osm_type = osm_id[0].upper()
                osm_num = int(osm_id[1:])

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
                    WHERE osm_type = %s AND osm_id = %s
                    LIMIT 1
                """

                cursor.execute(sql, (osm_type, osm_num))
                row = cursor.fetchone()

                if row:
                    place = {
                        "place_id": row[0],
                        "osm_type": row[1],
                        "osm_id": row[2],
                        "lat": str(row[3]) if row[3] else None,
                        "lon": str(row[4]) if row[4] else None,
                        "display_name": row[5] or osm_id,
                        "class": row[6],
                        "type": row[7],
                    }
                    places.append(place)

            logger.info(f"Found {len(places)} places for OSM IDs")
            return places

        finally:
            conn.close()

    async def status(self) -> Dict[str, Any]:
        """
        Check database status.
        Uses direct database queries via psycopg.
        """
        self.check_initialized()

        try:
            return await asyncio.to_thread(self._status_sync)

        except Exception as e:
            logger.error(f"Status check error: {e}")
            return {
                "status": "error",
                "error": str(e),
            }

    def _status_sync(self) -> Dict[str, Any]:
        """Synchronous status check using direct SQL query."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Check database connection and get basic stats
            cursor.execute("SELECT version()")
            db_version = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM placex")
            place_count = cursor.fetchone()[0]

            return {
                "status": "ok",
                "database_version": db_version.split()[0],
                "place_count": place_count,
            }

        finally:
            conn.close()

    def close(self) -> None:
        """Close the Nominatim service."""
        self._initialized = False
        logger.info("Nominatim service closed")


# Global service instance
nominatim_service = NominatimService()
