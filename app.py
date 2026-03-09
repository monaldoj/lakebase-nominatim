#!/usr/bin/env python3
"""
FastAPI application for Nominatim geocoding services.

Provides REST API endpoints for forward and reverse geocoding using
a remote Databricks Postgres instance with OAuth authentication.
"""

import os
import uuid
import logging
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
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


# Pydantic models for API requests and responses
class GeocodeResponse(BaseModel):
    """Response model for geocoding results."""
    place_id: int
    osm_type: str
    osm_id: int
    lat: str
    lon: str
    display_name: str
    class_: str = Field(..., alias="class")
    type: str
    importance: Optional[float] = None

    class Config:
        populate_by_name = True


class ReverseGeocodeResponse(BaseModel):
    """Response model for reverse geocoding results."""
    place_id: int
    osm_type: str
    osm_id: int
    lat: str
    lon: str
    display_name: str
    class_: str = Field(..., alias="class")
    type: str

    class Config:
        populate_by_name = True


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str


# Database connection functions
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


def search_places(conn, query: str, limit: int = 10) -> List[dict]:
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


def reverse_geocode(conn, lat: float, lon: float) -> Optional[dict]:
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


# Initialize FastAPI app
app = FastAPI(
    title="Nominatim Geocoding API",
    description="REST API for forward and reverse geocoding using Nominatim database",
    version="1.0.0",
)


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint."""
    return {
        "message": "Nominatim Geocoding API",
        "endpoints": {
            "search": "/search?q={query}&limit={limit}",
            "geocode": "/geocode?q={query}&limit={limit}",
            "reverse": "/reverse?lat={lat}&lon={lon}",
            "health": "/health"
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint to verify database connectivity."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        conn.close()
        return HealthResponse(status="healthy", database="connected")
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Database connection failed: {str(e)}")


@app.get("/geocode", response_model=List[GeocodeResponse], tags=["Geocoding"])
@app.get("/search", response_model=List[GeocodeResponse], tags=["Geocoding"])
async def geocode(
    q: str = Query(..., description="Search query (e.g., 'San Francisco', 'Eiffel Tower')"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of results")
):
    """
    Forward geocoding: Convert address/place name to coordinates.

    Args:
        q: Search query string
        limit: Maximum number of results (1-50)

    Returns:
        List of matching places with coordinates
    """
    try:
        conn = get_db_connection()
        results = search_places(conn, q, limit)
        conn.close()

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No results found for query: {q}"
            )

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Geocoding error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/reverse", response_model=ReverseGeocodeResponse, tags=["Geocoding"])
async def reverse(
    lat: float = Query(..., ge=-90, le=90, description="Latitude (-90 to 90)"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude (-180 to 180)")
):
    """
    Reverse geocoding: Convert coordinates to address/place name.

    Args:
        lat: Latitude coordinate
        lon: Longitude coordinate

    Returns:
        Nearest place information
    """
    try:
        conn = get_db_connection()
        result = reverse_geocode(conn, lat, lon)
        conn.close()

        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No location found near coordinates: {lat}, {lon}"
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reverse geocoding error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
