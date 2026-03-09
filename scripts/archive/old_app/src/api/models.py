"""Pydantic models for API requests and responses."""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class SearchResponse(BaseModel):
    """Response model for search endpoint."""
    place_id: Optional[int] = Field(None, description="Internal place ID")
    osm_type: Optional[str] = Field(None, description="OSM object type (N=node, W=way, R=relation)")
    osm_id: Optional[int] = Field(None, description="OSM object ID")
    lat: str = Field(..., description="Latitude")
    lon: str = Field(..., description="Longitude")
    display_name: str = Field(..., description="Full display name of the place")
    class_: Optional[str] = Field(None, alias="class", description="OSM tag class")
    type: Optional[str] = Field(None, description="OSM tag type")
    importance: Optional[float] = Field(None, description="Importance score (0-1)")
    address: Optional[Dict[str, str]] = Field(None, description="Structured address components")


class ReverseResponse(BaseModel):
    """Response model for reverse geocoding endpoint."""
    place_id: Optional[int] = Field(None, description="Internal place ID")
    osm_type: Optional[str] = Field(None, description="OSM object type (N=node, W=way, R=relation)")
    osm_id: Optional[int] = Field(None, description="OSM object ID")
    lat: str = Field(..., description="Latitude")
    lon: str = Field(..., description="Longitude")
    display_name: str = Field(..., description="Full display name of the place")
    class_: Optional[str] = Field(None, alias="class", description="OSM tag class")
    type: Optional[str] = Field(None, description="OSM tag type")
    address: Optional[Dict[str, str]] = Field(None, description="Structured address components")


class StatusResponse(BaseModel):
    """Response model for status endpoint."""
    status: str = Field(..., description="Service status (ok/error)")
    database_version: Optional[int] = Field(None, description="Database version")
    data_updated: Optional[str] = Field(None, description="Last data update timestamp")
    error: Optional[str] = Field(None, description="Error message if status is error")


class ErrorResponse(BaseModel):
    """Response model for errors."""
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
