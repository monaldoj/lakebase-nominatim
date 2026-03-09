"""API route definitions for Nominatim geocoding service."""

import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query

from src.api.models import SearchResponse, ReverseResponse, StatusResponse, ErrorResponse
from src.services.nominatim import nominatim_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", tags=["info"])
async def root():
    """API information and welcome message."""
    return {
        "service": "Nominatim Geocoding API",
        "version": "1.0.0",
        "description": "OpenStreetMap-based geocoding service on Databricks Apps",
        "endpoints": {
            "search": "/search?q=<query>",
            "reverse": "/reverse?lat=<lat>&lon=<lon>",
            "lookup": "/lookup?osm_ids=<comma-separated-ids>",
            "status": "/status",
        },
        "documentation": "/docs",
    }


@router.get("/search", response_model=List[SearchResponse], tags=["geocoding"])
async def search(
    q: str = Query(..., description="Search query (e.g., 'Berlin, Germany')"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of results"),
    countrycodes: Optional[str] = Query(None, description="Comma-separated country codes (e.g., 'us,ca')"),
    bounded: bool = Query(False, description="Restrict results to bounding box"),
    dedupe: bool = Query(True, description="Remove duplicate results"),
):
    """
    Search for places by address or name (forward geocoding).

    Returns a list of places matching the search query with their coordinates.

    Example:
        /search?q=1600+Amphitheatre+Parkway,+Mountain+View,+CA
    """
    try:
        # Parse country codes if provided
        country_list = None
        if countrycodes:
            country_list = [cc.strip().lower() for cc in countrycodes.split(",")]

        # Perform search
        results = await nominatim_service.search(
            query=q,
            limit=limit,
            country_codes=country_list,
            bounded=bounded,
            dedupe=dedupe,
        )

        return results

    except Exception as e:
        logger.error(f"Search endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@router.get("/reverse", response_model=ReverseResponse, tags=["geocoding"])
async def reverse(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    zoom: int = Query(18, ge=0, le=18, description="Level of detail (0=country, 18=building)"),
):
    """
    Reverse geocode coordinates to address.

    Returns the nearest address or place for the given coordinates.

    Example:
        /reverse?lat=37.4224764&lon=-122.0842499
    """
    try:
        result = await nominatim_service.reverse(lat=lat, lon=lon, zoom=zoom)

        if result is None:
            raise HTTPException(status_code=404, detail="No result found for the given coordinates")

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Reverse geocoding endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Reverse geocoding failed: {str(e)}")


@router.get("/lookup", response_model=List[SearchResponse], tags=["geocoding"])
async def lookup(
    osm_ids: str = Query(..., description="Comma-separated OSM IDs (e.g., 'R146656,N240109189')"),
):
    """
    Look up places by OpenStreetMap ID.

    OSM IDs should be prefixed with their type:
    - N: Node (point)
    - W: Way (line)
    - R: Relation (area)

    Example:
        /lookup?osm_ids=R146656,N240109189
    """
    try:
        # Parse OSM IDs
        osm_id_list = [id.strip().upper() for id in osm_ids.split(",")]

        # Perform lookup
        results = await nominatim_service.lookup(osm_ids=osm_id_list)

        return results

    except Exception as e:
        logger.error(f"Lookup endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Lookup failed: {str(e)}")


@router.get("/status", response_model=StatusResponse, tags=["health"])
async def status():
    """
    Check the status of the geocoding service and database.

    Returns database version and last update time if available.
    """
    try:
        status_info = await nominatim_service.status()
        return status_info

    except Exception as e:
        logger.error(f"Status endpoint error: {e}")
        return {
            "status": "error",
            "error": str(e),
        }


@router.get("/health", tags=["health"])
async def health():
    """
    Simple health check endpoint.

    Returns 200 OK if the service is running.
    """
    return {"status": "healthy"}
