#!/usr/bin/env python3
"""
FastAPI application for Nominatim geocoding services on Databricks Apps.

Provides REST API endpoints for forward geocoding, reverse geocoding,
and place lookup against a Nominatim PostgreSQL database hosted in
Databricks Lakebase.

Authentication uses in-workspace OAuth via the Databricks SDK when running
as a Databricks App, and falls back to PGPASSWORD for local development.
"""

import os
import time
import uuid
import logging
import threading
from contextlib import contextmanager
from typing import List, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
import psycopg
from psycopg_pool import ConnectionPool

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("nominatim-api")

# ---------------------------------------------------------------------------
# Environment - load .env for local dev; no-op in Databricks Apps
# ---------------------------------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
load_dotenv(ROOT_DIR / ".env")
load_dotenv()

# ---------------------------------------------------------------------------
# Helpers: detect deployment environment
# ---------------------------------------------------------------------------

def _is_databricks_app() -> bool:
    """Return True when running inside a Databricks Apps container."""
    return bool(os.getenv("DATABRICKS_APP_NAME"))


# ---------------------------------------------------------------------------
# OAuth token management
# ---------------------------------------------------------------------------

# Token cache: (token_str, expiry_epoch)
_token_lock = threading.Lock()
_cached_token: Optional[str] = None
_token_expiry: float = 0.0
# Refresh 5 minutes before expiry to avoid using stale tokens
_TOKEN_REFRESH_MARGIN = 300

_ws_client = None


def _get_workspace_client():
    """Return a cached WorkspaceClient singleton."""
    global _ws_client
    if _ws_client is None:
        from databricks.sdk import WorkspaceClient
        _ws_client = WorkspaceClient()
    return _ws_client


def _get_project_id_from_env() -> Optional[str]:
    """
    Resolve Lakebase autoscaling project id from environment.

    Checks PG_PROJECT_ID / PGPROJECTID first, then falls back to
    PGINSTANCENAME (treating non-endpoint values as project IDs).
    """
    project_id = os.getenv("PG_PROJECT_ID") or os.getenv("PGPROJECTID")
    if project_id:
        return project_id

    candidate = os.getenv("PGINSTANCENAME") or os.getenv("POSTGRES_INSTANCE_NAME")
    if candidate and not candidate.startswith("ep-"):
        return candidate
    return None


def _discover_project_from_host(pg_host: str) -> Optional[str]:
    """
    Discover the Lakebase project ID by matching the PGHOST endpoint against
    all projects visible in the workspace.

    This handles the Databricks Apps case where the runtime injects PGHOST
    (an ``ep-*`` endpoint hostname) but does not set PG_PROJECT_ID.
    """
    try:
        w = _get_workspace_client()
        for project in w.postgres.list_projects():
            # project.name is "projects/{project_id}"
            if not project.name:
                continue
            project_id = project.name.split("/")[-1]
            branch_path = f"projects/{project_id}/branches/production"
            try:
                for ep in w.postgres.list_endpoints(parent=branch_path):
                    if ep.status and ep.status.hosts and ep.status.hosts.host == pg_host:
                        logger.info(
                            "Discovered project '%s' for host '%s' via endpoint '%s'",
                            project_id, pg_host, ep.name,
                        )
                        return project_id
            except Exception as ep_err:
                logger.debug(
                    "Could not list endpoints for project %s: %s", project_id, ep_err,
                )
    except Exception as e:
        logger.warning("Project discovery from host failed: %s", e)
    return None


# Cache the resolved endpoint resource name so we don't re-list every token refresh
_endpoint_name_lock = threading.Lock()
_cached_endpoint_name: Optional[str] = None


def _resolve_endpoint_name(project_id: str, branch_id: str) -> str:
    """
    Return the full endpoint resource name for the given project/branch.

    The result is cached for the lifetime of the process since the endpoint
    name does not change.
    """
    global _cached_endpoint_name
    if _cached_endpoint_name:
        return _cached_endpoint_name

    with _endpoint_name_lock:
        if _cached_endpoint_name:
            return _cached_endpoint_name
        w = _get_workspace_client()
        branch_path = f"projects/{project_id}/branches/{branch_id}"
        endpoints = list(w.postgres.list_endpoints(parent=branch_path))
        if not endpoints:
            raise RuntimeError(f"No Postgres endpoints for branch: {branch_path}")
        _cached_endpoint_name = endpoints[0].name
        logger.info("Resolved endpoint name: %s", _cached_endpoint_name)
        return _cached_endpoint_name


def _get_oauth_token_for_project(project_id: str, branch_id: str) -> str:
    """
    Generate a fresh OAuth token via the ``postgres`` credential API.

    This is the correct path for autoscaling Lakebase projects.
    """
    endpoint_name = _resolve_endpoint_name(project_id, branch_id)
    w = _get_workspace_client()
    logger.info("Generating Lakebase OAuth token via endpoint: %s", endpoint_name)
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return cred.token


def _get_oauth_token_for_instance(instance_name: str) -> str:
    """
    Generate a fresh OAuth token via the legacy ``database`` credential API.

    This path is used for provisioned (non-autoscaling) Lakebase instances.
    """
    w = _get_workspace_client()
    logger.info("Generating Lakebase OAuth token for instance: %s", instance_name)
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name],
    )
    return cred.token


def _get_fresh_token() -> str:
    """
    Generate a fresh Lakebase OAuth token using the best available method.

    Resolution order:
      1. PG_PROJECT_ID / PGINSTANCENAME (explicit env config).
      2. Discover project by matching PGHOST against workspace endpoints.
      3. Legacy database credential API with instance name.
    """
    project_id = _get_project_id_from_env()
    branch_id = os.getenv("PG_BRANCH_ID", "production")
    host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST")
    oauth_errors: list[str] = []

    # --- Path 1: explicit project ID ---
    if project_id:
        try:
            return _get_oauth_token_for_project(project_id, branch_id)
        except Exception as e:
            oauth_errors.append(f"project {project_id}/{branch_id}: {e}")
            logger.warning("OAuth via explicit project failed: %s", e)

    # --- Path 2: discover project from PGHOST ---
    if host and not project_id:
        discovered = _discover_project_from_host(host)
        if discovered:
            try:
                return _get_oauth_token_for_project(discovered, branch_id)
            except Exception as e:
                oauth_errors.append(f"discovered project {discovered}: {e}")
                logger.warning("OAuth via discovered project failed: %s", e)

    # --- Path 3: legacy provisioned-instance API ---
    instance_name = os.getenv("PGINSTANCENAME") or os.getenv("POSTGRES_INSTANCE_NAME")
    if instance_name and not instance_name.startswith("ep-"):
        try:
            return _get_oauth_token_for_instance(instance_name)
        except Exception as e:
            oauth_errors.append(f"instance {instance_name}: {e}")
            logger.warning("OAuth via instance name failed: %s", e)

    details = "; ".join(oauth_errors) if oauth_errors else "no project/instance identifier available"
    raise RuntimeError(f"All OAuth token generation attempts failed: {details}")


def _get_password() -> str:
    """
    Return a password for the Postgres connection.

    In Databricks Apps the password is an OAuth token generated via the SDK.
    In local development the password comes from PGPASSWORD / .env.
    """
    global _cached_token, _token_expiry

    # --- Fast path: cached token still valid ---
    now = time.time()
    if _cached_token and now < (_token_expiry - _TOKEN_REFRESH_MARGIN):
        return _cached_token

    with _token_lock:
        now = time.time()
        if _cached_token and now < (_token_expiry - _TOKEN_REFRESH_MARGIN):
            return _cached_token

        # --- Try OAuth (Databricks Apps or SDK-authenticated local dev) ---
        if _is_databricks_app() or _get_project_id_from_env():
            try:
                token = _get_fresh_token()
                _cached_token = token
                _token_expiry = now + 3600  # Lakebase tokens last 1 hour
                logger.info("OAuth token refreshed, valid until %.0f", _token_expiry)
                return token
            except Exception as e:
                logger.warning("OAuth token generation failed: %s", e)
                # Fall through to PGPASSWORD

        # --- Fallback: static password for local development ---
        pgpassword = os.getenv("PGPASSWORD") or os.getenv("POSTGRES_PASSWORD")
        if pgpassword:
            logger.info("Using direct password auth from PGPASSWORD")
            return pgpassword

        raise RuntimeError(
            "Cannot determine Postgres password. "
            "In Databricks Apps, ensure the app has a database resource attached. "
            "For local dev, set PGPASSWORD in your .env file."
        )


# ---------------------------------------------------------------------------
# Connection pool with automatic token refresh
# ---------------------------------------------------------------------------

def _build_conninfo_base() -> str:
    """Build a libpq connection string *without* a password.

    The password is injected per-connection via the pool's ``configure``
    callback so that expired OAuth tokens are automatically replaced.
    """
    project_id = _get_project_id_from_env()
    branch_id = os.getenv("PG_BRANCH_ID", "production")
    host = os.getenv("PGHOST") or os.getenv("POSTGRES_HOST")

    # If host isn't set, try to resolve it from the Lakebase API
    if not host and project_id:
        try:
            endpoint_name = _resolve_endpoint_name(project_id, branch_id)
            w = _get_workspace_client()
            branch_path = f"projects/{project_id}/branches/{branch_id}"
            endpoints = list(w.postgres.list_endpoints(parent=branch_path))
            if endpoints and endpoints[0].status and endpoints[0].status.hosts:
                host = endpoints[0].status.hosts.host
                logger.info("Resolved Lakebase host: %s", host)
        except Exception as e:
            logger.warning("Failed to resolve Lakebase host: %s", e)

    port = os.getenv("PGPORT") or os.getenv("POSTGRES_PORT") or "5432"
    database = os.getenv("PGDATABASE") or os.getenv("POSTGRES_DATABASE") or "nominatim"
    user = os.getenv("PGUSER") or os.getenv("POSTGRES_USER")
    sslmode = os.getenv("PGSSLMODE") or os.getenv("POSTGRES_SSLMODE") or "require"

    if not host or not user:
        raise RuntimeError(
            "Cannot determine Postgres host/user. "
            "Set PGHOST + PGUSER (or attach a database resource in Databricks Apps)."
        )

    return psycopg.conninfo.make_conninfo(
        host=host,
        port=port,
        dbname=database,
        user=user,
        sslmode=sslmode,
        connect_timeout="15",
    )


def _password_kwargs() -> dict:
    """Callable passed as ``kwargs`` to the ConnectionPool.

    psycopg_pool calls this every time it creates a new connection, so
    the password is always a fresh (or cached-but-still-valid) OAuth token.
    """
    return {"password": _get_password()}


def _make_pool() -> ConnectionPool:
    """Create a psycopg ConnectionPool with automatic token refresh.

    The ``kwargs`` parameter accepts a callable (psycopg_pool >= 3.2).
    Each time the pool opens a new connection it calls ``_password_kwargs()``
    to get a fresh password, which transparently handles OAuth token
    rotation without rebuilding the pool.
    """
    base_conninfo = _build_conninfo_base()

    logger.info("Creating connection pool (min=2, max=10)")
    pool = ConnectionPool(
        conninfo=base_conninfo,
        min_size=2,
        max_size=10,
        max_idle=300,
        # Keep connections for at most 50 minutes (< 1h token lifetime)
        max_lifetime=3000,
        reconnect_timeout=30,
        open=False,
        kwargs=_password_kwargs,
    )
    return pool


_pool: Optional[ConnectionPool] = None
_pool_lock = threading.Lock()


def _get_pool() -> ConnectionPool:
    """Return the global pool, creating it lazily if needed."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = _make_pool()
                _pool.open()
    return _pool


@contextmanager
def get_conn():
    """
    Yield a connection from the pool.

    On auth / connection errors the pool is recycled with a fresh token
    and the caller should retry.
    """
    pool = _get_pool()
    try:
        with pool.connection(timeout=10) as conn:
            yield conn
    except psycopg.OperationalError:
        logger.warning("OperationalError - recycling connection pool")
        _rebuild_pool()
        raise


def _rebuild_pool():
    """Tear down the old pool and build a new one with a fresh token."""
    global _pool, _cached_token, _token_expiry, _cached_endpoint_name
    logger.info("Rebuilding connection pool with fresh credentials")
    with _token_lock:
        _cached_token = None
        _token_expiry = 0.0
    with _endpoint_name_lock:
        _cached_endpoint_name = None
    with _pool_lock:
        old = _pool
        _pool = None
    if old:
        try:
            old.close(timeout=5)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class AddressDetail(BaseModel):
    """Structured address components."""
    house_number: Optional[str] = None
    road: Optional[str] = None
    neighbourhood: Optional[str] = None
    suburb: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None
    postcode: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None


class GeocodeResult(BaseModel):
    """Single geocoding result."""
    place_id: int
    osm_type: Optional[str] = None
    osm_id: Optional[int] = None
    lat: str
    lon: str
    display_name: str
    category: str = Field(description="OSM feature class")
    type: str = Field(description="OSM feature type")
    importance: Optional[float] = None
    address: Optional[AddressDetail] = None

    model_config = ConfigDict(populate_by_name=True)


class ReverseResult(BaseModel):
    """Single reverse-geocoding result."""
    place_id: int
    osm_type: Optional[str] = None
    osm_id: Optional[int] = None
    lat: str
    lon: str
    display_name: str
    category: str
    type: str
    distance_m: Optional[float] = Field(None, description="Distance from query point in metres")
    address: Optional[AddressDetail] = None

    model_config = ConfigDict(populate_by_name=True)


class HealthResponse(BaseModel):
    status: str
    database: str
    pool_size: Optional[int] = None
    token_ttl_s: Optional[int] = None


class StatusResponse(BaseModel):
    """Detailed server status."""
    status: str
    version: str
    database: str
    data_updated: Optional[str] = None
    place_count: Optional[int] = None


# ---------------------------------------------------------------------------
# SQL query helpers
# ---------------------------------------------------------------------------

_SEARCH_SQL = """
SELECT
    p.place_id,
    p.osm_type,
    p.osm_id,
    ST_Y(p.centroid)::text  AS lat,
    ST_X(p.centroid)::text  AS lon,
    p.name -> 'name'        AS display_name,
    p.class   AS category,
    p.type,
    p.importance,
    -- address parts
    p.address -> 'housenumber' AS house_number,
    p.address -> 'street'      AS road,
    p.address -> 'suburb'      AS suburb,
    p.address -> 'city'        AS city,
    p.address -> 'county'      AS county,
    p.address -> 'state'       AS state,
    p.address -> 'postcode'    AS postcode,
    p.address -> 'country'     AS country,
    p.country_code
FROM placex p
WHERE (
        p.name -> 'name' ILIKE %(pattern)s
        OR p.address -> 'city' ILIKE %(pattern)s
        OR p.address -> 'country' ILIKE %(pattern)s
      )
  AND p.linked_place_id IS NULL
  AND p.rank_search BETWEEN 2 AND 30
ORDER BY
    p.importance DESC NULLS LAST,
    p.rank_search ASC
LIMIT %(limit)s
"""

# Structured search: each comma-separated part must match at least one
# address-related field.  Parts are ANDed together so "Tehran, Iran" means
# the place must match "Tehran" in *some* field AND "Iran" in *some* field.
_STRUCTURED_SEARCH_SQL_TEMPLATE = """
SELECT
    p.place_id,
    p.osm_type,
    p.osm_id,
    ST_Y(p.centroid)::text  AS lat,
    ST_X(p.centroid)::text  AS lon,
    p.name -> 'name'        AS display_name,
    p.class   AS category,
    p.type,
    p.importance,
    -- address parts
    p.address -> 'housenumber' AS house_number,
    p.address -> 'street'      AS road,
    p.address -> 'suburb'      AS suburb,
    p.address -> 'city'        AS city,
    p.address -> 'county'      AS county,
    p.address -> 'state'       AS state,
    p.address -> 'postcode'    AS postcode,
    p.address -> 'country'     AS country,
    p.country_code
FROM placex p
WHERE {part_conditions}
  AND p.linked_place_id IS NULL
  AND p.rank_search BETWEEN 2 AND 30
ORDER BY
    p.importance DESC NULLS LAST,
    p.rank_search ASC
LIMIT %(limit)s
"""


def _build_structured_search(parts: list[str]) -> tuple[str, dict]:
    """
    Build a structured search query from comma-separated address parts.

    Each part must match at least one of: name, street, city, county, state,
    or country.  All parts are ANDed together.

    Returns (sql_string, params_dict).
    """
    conditions = []
    params: dict = {}
    for i, part in enumerate(parts):
        key = f"part_{i}"
        params[key] = f"%{part}%"
        conditions.append(
            f"""(
                p.name -> 'name'    ILIKE %({key})s
                OR p.address -> 'street'  ILIKE %({key})s
                OR p.address -> 'city'    ILIKE %({key})s
                OR p.address -> 'county'  ILIKE %({key})s
                OR p.address -> 'state'   ILIKE %({key})s
                OR p.address -> 'country' ILIKE %({key})s
            )"""
        )
    return (
        _STRUCTURED_SEARCH_SQL_TEMPLATE.format(part_conditions="\n  AND ".join(conditions)),
        params,
    )

_REVERSE_SQL = """
SELECT
    p.place_id,
    p.osm_type,
    p.osm_id,
    ST_Y(p.centroid)::text  AS lat,
    ST_X(p.centroid)::text  AS lon,
    p.name -> 'name'        AS display_name,
    p.class   AS category,
    p.type,
    ST_Distance(
        p.centroid::geography,
        ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography
    ) AS distance_m,
    p.address -> 'housenumber' AS house_number,
    p.address -> 'street'      AS road,
    p.address -> 'suburb'      AS suburb,
    p.address -> 'city'        AS city,
    p.address -> 'county'      AS county,
    p.address -> 'state'       AS state,
    p.address -> 'postcode'    AS postcode,
    p.address -> 'country'     AS country,
    p.country_code
FROM placex p
WHERE ST_DWithin(
        p.centroid::geography,
        ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)::geography,
        %(radius_m)s
      )
  AND p.name -> 'name' IS NOT NULL
  AND p.linked_place_id IS NULL
ORDER BY distance_m ASC
LIMIT %(limit)s
"""

_LOOKUP_SQL = """
SELECT
    p.place_id,
    p.osm_type,
    p.osm_id,
    ST_Y(p.centroid)::text  AS lat,
    ST_X(p.centroid)::text  AS lon,
    p.name -> 'name'        AS display_name,
    p.class   AS category,
    p.type,
    p.importance,
    p.address -> 'housenumber' AS house_number,
    p.address -> 'street'      AS road,
    p.address -> 'suburb'      AS suburb,
    p.address -> 'city'        AS city,
    p.address -> 'county'      AS county,
    p.address -> 'state'       AS state,
    p.address -> 'postcode'    AS postcode,
    p.address -> 'country'     AS country,
    p.country_code
FROM placex p
WHERE p.osm_type = %(osm_type)s AND p.osm_id = %(osm_id)s
LIMIT 1
"""


def _row_to_geocode(row) -> dict:
    """Map a search/lookup row to a GeocodeResult dict."""
    return {
        "place_id": row[0],
        "osm_type": row[1],
        "osm_id": row[2],
        "lat": row[3] or "0",
        "lon": row[4] or "0",
        "display_name": row[5] or "Unknown",
        "category": row[6] or "",
        "type": row[7] or "",
        "importance": float(row[8]) if row[8] is not None else None,
        "address": _build_address(row, offset=9),
    }


def _row_to_reverse(row) -> dict:
    """Map a reverse row to a ReverseResult dict."""
    return {
        "place_id": row[0],
        "osm_type": row[1],
        "osm_id": row[2],
        "lat": row[3] or "0",
        "lon": row[4] or "0",
        "display_name": row[5] or "Unknown",
        "category": row[6] or "",
        "type": row[7] or "",
        "distance_m": round(float(row[8]), 2) if row[8] is not None else None,
        "address": _build_address(row, offset=9),
    }


def _build_address(row, offset: int) -> Optional[dict]:
    """Build an AddressDetail dict from row columns starting at offset."""
    fields = [
        "house_number", "road", "suburb", "city",
        "county", "state", "postcode", "country", "country_code",
    ]
    addr = {}
    for i, field in enumerate(fields):
        val = row[offset + i] if (offset + i) < len(row) else None
        if val:
            addr[field] = str(val)
    return addr if addr else None


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Nominatim Geocoding API",
    description=(
        "Forward and reverse geocoding powered by a Nominatim database "
        "running on Databricks Lakebase. Compatible with common Nominatim "
        "query parameters."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup():
    """Open the connection pool at startup."""
    try:
        logger.info("Opening connection pool ...")
        pool = _get_pool()  # creates + opens the pool
        pool.wait(timeout=30)  # block until min_size connections are ready
        logger.info("Connection pool ready")
    except Exception as e:
        logger.error("Failed to open connection pool: %s", e)
        # Don't crash - the pool will retry on first request


@app.on_event("shutdown")
async def shutdown():
    global _pool
    if _pool:
        logger.info("Closing connection pool ...")
        _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Health / status endpoints
# ---------------------------------------------------------------------------

@app.get("/", tags=["Health"])
@app.get("/api", tags=["Health"])
async def root():
    """API discovery endpoint."""
    return {
        "service": "Nominatim Geocoding API",
        "version": "2.0.0",
        "endpoints": {
            "search": "/search?q={query}&limit={limit}",
            "reverse": "/reverse?lat={lat}&lon={lon}&radius={meters}",
            "lookup": "/lookup?osm_ids={R123,W456,N789}",
            "health": "/health",
            "status": "/status",
            "docs": "/docs",
        },
    }


@app.get("/_reload-hash", include_in_schema=False, status_code=204)
async def reload_hash():
    """No-op probe used by dev hot-reload."""
    return Response(status_code=204)


@app.get("/health", response_model=HealthResponse, tags=["Health"])
@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health():
    """Lightweight health check - verifies database connectivity."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

        pool = _get_pool()
        pool_stats = pool.get_stats()
        ttl = max(0, int(_token_expiry - time.time())) if _cached_token else None

        return HealthResponse(
            status="healthy",
            database="connected",
            pool_size=pool_stats.get("pool_size", None),
            token_ttl_s=ttl,
        )
    except Exception as e:
        logger.error("Health check failed: %s", e)
        raise HTTPException(503, detail=f"Database unavailable: {e}")


@app.get("/status", response_model=StatusResponse, tags=["Health"])
@app.get("/api/status", response_model=StatusResponse, tags=["Health"])
async def status():
    """Detailed status including data freshness and row counts."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Total indexed places
                cur.execute(
                    "SELECT COUNT(*) FROM placex WHERE linked_place_id IS NULL"
                )
                count = cur.fetchone()[0]

                # Last import timestamp (if import_status table exists)
                data_updated = None
                try:
                    cur.execute("SELECT lastimportdate FROM import_status LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        data_updated = str(row[0])
                except Exception:
                    conn.rollback()

        return StatusResponse(
            status="ok",
            version="2.0.0",
            database="connected",
            data_updated=data_updated,
            place_count=count,
        )
    except Exception as e:
        logger.error("Status check failed: %s", e)
        raise HTTPException(503, detail=str(e))


# ---------------------------------------------------------------------------
# Geocoding endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/search",
    response_model=List[GeocodeResult],
    tags=["Geocoding"],
    summary="Forward geocoding - search by name or address",
)
@app.get("/api/search", response_model=List[GeocodeResult], tags=["Geocoding"], include_in_schema=False)
@app.get("/geocode", response_model=List[GeocodeResult], tags=["Geocoding"], include_in_schema=False)
@app.get("/api/geocode", response_model=List[GeocodeResult], tags=["Geocoding"], include_in_schema=False)
async def search(
    q: str = Query(..., min_length=1, description="Place name or address to search for"),
    limit: int = Query(10, ge=1, le=50, description="Maximum number of results"),
    countrycodes: Optional[str] = Query(
        None,
        description="Comma-separated ISO 3166-1 alpha-2 country codes to restrict results",
    ),
    street: Optional[str] = Query(None, description="Street name filter"),
    city: Optional[str] = Query(None, description="City name filter"),
    county: Optional[str] = Query(None, description="County filter"),
    state: Optional[str] = Query(None, description="State filter"),
    country: Optional[str] = Query(None, description="Country name filter"),
    format: str = Query("json", include_in_schema=False),
):
    """
    Forward geocoding: convert a place name or address into geographic
    coordinates.

    Supports free-form queries (``q=Tehran, Iran``), which are automatically
    split on commas and matched across name/address fields, as well as
    explicit address-part parameters (``city``, ``country``, etc.) that can
    be combined with ``q`` for more precise filtering.

    Mimics the Nominatim ``/search`` endpoint query parameters where practical.
    """
    # Collect explicit address-part filters supplied as query params
    explicit_parts: list[str] = []
    if street:
        explicit_parts.append(street.strip())
    if city:
        explicit_parts.append(city.strip())
    if county:
        explicit_parts.append(county.strip())
    if state:
        explicit_parts.append(state.strip())
    if country:
        explicit_parts.append(country.strip())

    # Split q on commas to detect structured queries like "Tehran, Iran"
    q_parts = [p.strip() for p in q.split(",") if p.strip()]
    # Merge: q parts + any explicit parts
    all_parts = q_parts + explicit_parts
    use_structured = len(all_parts) > 1

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if use_structured:
                    # Structured search: each part must match some field
                    sql, params = _build_structured_search(all_parts)
                    params["limit"] = limit
                    if countrycodes:
                        codes = [c.strip().lower() for c in countrycodes.split(",") if c.strip()]
                        sql = sql.replace(
                            "LIMIT %(limit)s",
                            "AND p.country_code = ANY(%(codes)s)\nLIMIT %(limit)s",
                        )
                        params["codes"] = codes
                    cur.execute(sql, params)
                else:
                    # Single-term search
                    pattern = f"%{q}%"
                    if countrycodes:
                        codes = [c.strip().lower() for c in countrycodes.split(",") if c.strip()]
                        sql = _SEARCH_SQL.replace(
                            "LIMIT %(limit)s",
                            "AND p.country_code = ANY(%(codes)s)\nLIMIT %(limit)s",
                        )
                        cur.execute(sql, {"pattern": pattern, "limit": limit, "codes": codes})
                    else:
                        cur.execute(_SEARCH_SQL, {"pattern": pattern, "limit": limit})

                rows = cur.fetchall()

        if not rows:
            raise HTTPException(404, detail=f"No results found for: {q}")

        return [_row_to_geocode(r) for r in rows]

    except HTTPException:
        raise
    except psycopg.OperationalError as e:
        logger.error("Database error during search: %s", e)
        raise HTTPException(503, detail="Database temporarily unavailable, please retry.")
    except Exception as e:
        logger.error("Search error: %s", e, exc_info=True)
        raise HTTPException(500, detail=f"Internal error: {e}")


@app.get(
    "/reverse",
    response_model=List[ReverseResult],
    tags=["Geocoding"],
    summary="Reverse geocoding - coordinates to place name",
)
@app.get("/api/reverse", response_model=List[ReverseResult], tags=["Geocoding"], include_in_schema=False)
async def reverse(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    radius: float = Query(
        1000, ge=1, le=50000, description="Search radius in metres (default 1 km)"
    ),
    limit: int = Query(1, ge=1, le=10, description="Number of results"),
    format: str = Query("json", include_in_schema=False),
):
    """
    Reverse geocoding: find the closest named place(s) to a pair of
    geographic coordinates.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _REVERSE_SQL,
                    {"lat": lat, "lon": lon, "radius_m": radius, "limit": limit},
                )
                rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                404,
                detail=f"No location found within {radius}m of ({lat}, {lon})",
            )

        return [_row_to_reverse(r) for r in rows]

    except HTTPException:
        raise
    except psycopg.OperationalError as e:
        logger.error("Database error during reverse: %s", e)
        raise HTTPException(503, detail="Database temporarily unavailable, please retry.")
    except Exception as e:
        logger.error("Reverse error: %s", e, exc_info=True)
        raise HTTPException(500, detail=f"Internal error: {e}")


@app.get(
    "/lookup",
    response_model=List[GeocodeResult],
    tags=["Geocoding"],
    summary="Look up places by OSM id",
)
@app.get("/api/lookup", response_model=List[GeocodeResult], tags=["Geocoding"], include_in_schema=False)
async def lookup(
    osm_ids: str = Query(
        ...,
        description=(
            "Comma-separated OSM identifiers, each prefixed with "
            "N (node), W (way), or R (relation). Example: R123,W456"
        ),
    ),
    format: str = Query("json", include_in_schema=False),
):
    """
    Look up one or more places by their OpenStreetMap identifiers.
    """
    type_map = {"N": "N", "W": "W", "R": "R"}
    ids = []
    for raw in osm_ids.split(","):
        raw = raw.strip()
        if len(raw) < 2 or raw[0].upper() not in type_map:
            continue
        try:
            ids.append((raw[0].upper(), int(raw[1:])))
        except ValueError:
            continue

    if not ids:
        raise HTTPException(400, detail="No valid OSM identifiers provided.")

    results = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for osm_type, osm_id in ids:
                    cur.execute(_LOOKUP_SQL, {"osm_type": osm_type, "osm_id": osm_id})
                    row = cur.fetchone()
                    if row:
                        results.append(_row_to_geocode(row))

        if not results:
            raise HTTPException(404, detail="None of the requested OSM ids were found.")

        return results

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Lookup error: %s", e, exc_info=True)
        raise HTTPException(500, detail=f"Internal error: {e}")


# ---------------------------------------------------------------------------
# Entry point for `python app.py` (local dev) and app.yml command
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("UVICORN_HOST", "0.0.0.0"),
        port=int(os.getenv("UVICORN_PORT", os.getenv("PORT", "8000"))),
    )
