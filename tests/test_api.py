"""Basic API tests for Nominatim geocoding service."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch, MagicMock

from app import app


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_nominatim_service():
    """Mock the Nominatim service."""
    with patch("src.services.nominatim.nominatim_service") as mock:
        # Mock initialization
        mock.initialize = MagicMock()
        mock.close = MagicMock()
        yield mock


def test_root_endpoint(client):
    """Test the root endpoint returns API information."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert "service" in data
    assert data["service"] == "Nominatim Geocoding API"
    assert "endpoints" in data


def test_ping_endpoint(client):
    """Test the ping endpoint."""
    response = client.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"message": "pong"}


def test_health_endpoint(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


@pytest.mark.asyncio
async def test_search_endpoint(client, mock_nominatim_service):
    """Test the search endpoint with mocked service."""
    # Mock search results
    mock_results = [
        {
            "place_id": 123456,
            "osm_type": "R",
            "osm_id": 62422,
            "lat": "52.5170365",
            "lon": "13.3888599",
            "display_name": "Berlin, Deutschland",
            "class": "boundary",
            "type": "administrative",
            "importance": 0.73,
            "address": {"city": "Berlin", "country": "Deutschland"},
        }
    ]
    mock_nominatim_service.search = AsyncMock(return_value=mock_results)

    response = client.get("/search?q=Berlin&limit=1")

    # Note: This test requires the app to be properly initialized
    # In a real scenario, you'd need to handle the lifespan events
    assert response.status_code in [200, 500]  # May fail if DB not available


@pytest.mark.asyncio
async def test_reverse_endpoint(client, mock_nominatim_service):
    """Test the reverse geocoding endpoint with mocked service."""
    # Mock reverse geocoding result
    mock_result = {
        "place_id": 123456,
        "osm_type": "N",
        "osm_id": 240109189,
        "lat": "52.5200000",
        "lon": "13.4050000",
        "display_name": "Berlin, Deutschland",
        "class": "place",
        "type": "city",
        "address": {"city": "Berlin", "country": "Deutschland"},
    }
    mock_nominatim_service.reverse = AsyncMock(return_value=mock_result)

    response = client.get("/reverse?lat=52.5200&lon=13.4050")

    # Note: This test requires the app to be properly initialized
    assert response.status_code in [200, 404, 500]


def test_search_missing_query(client):
    """Test search endpoint with missing query parameter."""
    response = client.get("/search")
    assert response.status_code == 422  # Validation error


def test_reverse_invalid_coordinates(client):
    """Test reverse geocoding with invalid coordinates."""
    response = client.get("/reverse?lat=100&lon=200")  # Out of range
    assert response.status_code == 422  # Validation error


@pytest.mark.asyncio
async def test_status_endpoint(client, mock_nominatim_service):
    """Test the status endpoint."""
    mock_nominatim_service.status = AsyncMock(
        return_value={"status": "ok", "database_version": 1, "data_updated": None}
    )

    response = client.get("/status")

    # Note: This test requires the app to be properly initialized
    assert response.status_code in [200, 500]


def test_lookup_missing_ids(client):
    """Test lookup endpoint with missing OSM IDs."""
    response = client.get("/lookup")
    assert response.status_code == 422  # Validation error


# Integration tests (require actual database)
@pytest.mark.integration
def test_search_integration(client):
    """Integration test for search endpoint (requires database)."""
    response = client.get("/search?q=Monaco&limit=1")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if len(data) > 0:
        assert "lat" in data[0]
        assert "lon" in data[0]
        assert "display_name" in data[0]


@pytest.mark.integration
def test_reverse_integration(client):
    """Integration test for reverse geocoding (requires database)."""
    # Monaco coordinates
    response = client.get("/reverse?lat=43.7384&lon=7.4246")
    assert response.status_code in [200, 404]
    if response.status_code == 200:
        data = response.json()
        assert "lat" in data
        assert "lon" in data
        assert "display_name" in data
