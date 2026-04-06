# Nominatim Geocoding API on Databricks

OpenStreetMap-based geocoding service running on Databricks Apps with Lakebase Managed Postgres.

## Features

- **Forward Geocoding**: Convert addresses/place names to coordinates
- **Reverse Geocoding**: Convert coordinates to addresses with configurable radius
- **OSM Lookup**: Find places by OpenStreetMap ID (node, way, or relation)
- **Structured Addresses**: Every result includes parsed address components (city, state, country, etc.)
- **Connection Pooling**: psycopg connection pool (2-10 connections) with automatic reconnection
- **Cached OAuth Tokens**: Lakebase tokens are cached and refreshed automatically 5 minutes before expiry
- **FastAPI**: Modern Python API with auto-generated Swagger/OpenAPI docs
- **Databricks Apps**: Serverless deployment with in-workspace authentication
- **Managed Postgres**: Lakebase managed database with PostGIS

## Prerequisites

- Python 3.9+
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) installed and configured
- A Databricks workspace with Lakebase enabled
- `DATABRICKS_HOST` set (e.g. `export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com`)

## Project Structure

```
nominatim/
├── databricks.yml                     # Databricks Asset Bundle configuration
├── resources/
│   ├── lakebase.yml                   # Lakebase Postgres instance definition
│   ├── nominatim-geocoding-api.yml    # Databricks App resource + env config
│   └── nominatim-import-job.yml       # Lakeflow job definition (import pipeline)
├── app/
│   ├── app.py                         # FastAPI application (deployed to Databricks Apps)
│   ├── app.yml                        # Databricks Apps command configuration
│   └── requirements.txt               # Runtime Python dependencies for the app
├── job/
│   ├── 00_setup_catalog.ipynb         # Create UC catalog, schema, and volumes
│   ├── 00_refresh_environment.ipynb   # Refresh database OAuth token
│   ├── 01_setup_postgis.ipynb         # Install PostGIS/hstore extensions
│   ├── 02_download_osm_data.ipynb     # Download OSM data to UC volume
│   ├── 03_build_nominatim_server.ipynb # Full Nominatim import
│   ├── 04_resume_failed_indexing.ipynb # Resume interrupted indexing
│   ├── _helpers.ipynb                 # Shared helper functions
│   ├── nominatim_init.sh             # Init script for cluster setup
│   └── OSM_SOURCES.ipynb             # Reference list of OSM data sources
├── scripts/                           # Local (backup) deployment scripts
│   ├── 00_refresh_environment.py      # Refresh database OAuth token -> .env
│   ├── 01_setup_postgis.py            # Install PostGIS/hstore extensions
│   ├── 02_download_osm_data.py        # Download OSM data files
│   ├── 03_build_nominatim_server.sh   # Import OSM data and build Nominatim DB
│   ├── 04_resume_failed_indexing.sh   # Resume interrupted indexing
│   ├── geocode.py                     # CLI geocoding client (hits API endpoints)
│   └── query_nominatim_db.py          # Direct database query tool (bypasses API)
├── tests/
│   └── test_api.py                    # API tests
├── requirements.txt                   # Development Python dependencies
├── .env.template                      # Environment variable template
└── .gitignore
```

---

## Deploying with Databricks Asset Bundles (DAB)

This is the primary deployment method. The bundle deploys three resources:

1. **Lakebase Postgres instance** (`resources/lakebase.yml`) -- the managed PostGIS database
2. **Databricks App** (`resources/nominatim-geocoding-api.yml`) -- the FastAPI geocoding service
3. **Lakeflow Import Job** (`resources/nominatim-import-job.yml`) -- orchestrated pipeline that sets up PostGIS, downloads OSM data, and runs the Nominatim import

### 1. Validate the bundle

```bash
# Validate against the default target (dev)
databricks bundle validate

# Validate against a specific target
databricks bundle validate -t dev
databricks bundle validate -t test
databricks bundle validate -t prod
```

### 2. Deploy

```bash
# Deploy to dev (default target)
databricks bundle deploy

# Deploy to a specific target
databricks bundle deploy -t dev
databricks bundle deploy -t test
databricks bundle deploy -t prod
```

### 3. Grant the App Service Principal access to Lakebase

After the first deploy, the Databricks App gets its own Service Principal (SP). That SP needs permissions on the Lakebase Postgres project before the app can connect. This is a one-time manual step performed in the Databricks UI:

1. Navigate to your Lakebase project in the workspace (SQL Editor sidebar or Catalog Explorer).
2. Click **Branches** and select the branch name (default: **production**).
3. Click **Roles & Databases**.
4. Find the Service Principal role that was created for your app (it will match the app name).
5. Click **Edit Role** next to it.
6. Add the required permissions (at minimum, grant access to the `nominatim` database).
7. Save.

Without this step the app will fail with authentication/authorization errors when trying to query the database.

### 4. Run the OSM import job

The import job orchestrates the full pipeline: catalog setup, PostGIS extensions, OSM data download, and Nominatim import.

```bash
# Run with the default region (monaco)
databricks bundle run nominatim-import -t dev

# Override regions at runtime
databricks bundle run nominatim-import -t dev --var "OSM_REGIONS=virginia,maryland"
```

### 5. Monitor

```bash
# View app status
databricks apps get nominatim-geocoding-api-dev

# Get the app URL
databricks apps get nominatim-geocoding-api-dev --json | jq -r '.url'

# View app logs
databricks apps logs nominatim-geocoding-api-dev
databricks apps logs nominatim-geocoding-api-dev --follow

# List active job runs
databricks jobs list-runs --active-only true
```

### 6. Tear down

```bash
databricks bundle destroy -t dev
```

### Bundle variables

Variables can be overridden at deploy or run time with `--var "KEY=value"`:

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_NAME` | `nominatim-geocoding-api` | Databricks App name prefix |
| `PG_PROJECT_ID` | `nominatim-lakebase` | Lakebase Postgres project ID |
| `PG_BRANCH_ID` | `production` | Lakebase branch |
| `PG_MIN_CU` | `0.5` | Autoscaling minimum compute units |
| `PG_MAX_CU` | `4` | Autoscaling maximum compute units |
| `PG_USER` | `justin.monaldo@databricks.com` | Postgres user |
| `PG_DATABASE` | `nominatim` | Database name |
| `OSM_REGIONS` | `monaco` | Comma-separated regions to import |
| `UC_CATALOG` | `justinm` | Unity Catalog catalog for volumes |
| `UC_SCHEMA` | `nominatim` | Unity Catalog schema for volumes |

### Available targets

| Target | Mode | Description |
|--------|------|-------------|
| `dev` | development | Default target for development |
| `test` | development | Testing environment |
| `prod` | production | Production deployment |

---

## Testing the Deployed API

The deployed app is protected by Databricks workspace authentication. You need an OAuth token to call the endpoints.

### Get an OAuth token

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export OAUTH_TOKEN=$(databricks auth token --host $DATABRICKS_HOST | jq -r '.access_token')
```

### Test with curl

```bash
# Health check
curl -s \
    -H "Authorization: Bearer $OAUTH_TOKEN" \
    "https://nominatim-geocoding-api-1444828305810485.aws.databricksapps.com/api/health" | jq .

# Forward geocoding
curl -s \
    -H "Authorization: Bearer $OAUTH_TOKEN" \
    "https://nominatim-geocoding-api-1444828305810485.aws.databricksapps.com/api/search?q=Alexandria" | jq .

# Reverse geocoding
curl -s \
    -H "Authorization: Bearer $OAUTH_TOKEN" \
    "https://nominatim-geocoding-api-1444828305810485.aws.databricksapps.com/api/reverse?lat=38.8048&lon=-77.0469" | jq .

# OSM ID lookup
curl -s \
    -H "Authorization: Bearer $OAUTH_TOKEN" \
    "https://nominatim-geocoding-api-1444828305810485.aws.databricksapps.com/api/lookup?osm_ids=R146656,W5013364" | jq .
```

### Query from Python with a Service Principal

For automated or production access, use a Databricks Service Principal's `client_id` and `client_secret` to obtain an OAuth token, then call the API:

```python
import requests

# --- Configuration ---
DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
APP_URL = "https://nominatim-geocoding-api-1444828305810485.aws.databricksapps.com"
CLIENT_ID = "your-service-principal-client-id"
CLIENT_SECRET = "your-service-principal-client-secret"

# --- Get OAuth token via client credentials flow ---
token_response = requests.post(
    f"{DATABRICKS_HOST}/oidc/v1/token",
    data={
        "grant_type": "client_credentials",
        "scope": "all-apis",
    },
    auth=(CLIENT_ID, CLIENT_SECRET),
)
token_response.raise_for_status()
access_token = token_response.json()["access_token"]

headers = {"Authorization": f"Bearer {access_token}"}

# --- Health check ---
resp = requests.get(f"{APP_URL}/api/health", headers=headers)
print("Health:", resp.json())

# --- Forward geocoding ---
resp = requests.get(
    f"{APP_URL}/api/search",
    params={"q": "Alexandria", "limit": 3},
    headers=headers,
)
print("Search results:", resp.json())

# --- Reverse geocoding ---
resp = requests.get(
    f"{APP_URL}/api/reverse",
    params={"lat": 38.8048, "lon": -77.0469},
    headers=headers,
)
print("Reverse results:", resp.json())

# --- OSM lookup ---
resp = requests.get(
    f"{APP_URL}/api/lookup",
    params={"osm_ids": "R146656,W5013364"},
    headers=headers,
)
print("Lookup results:", resp.json())
```

---

## Local Deployment (Backup Method)

Use the scripts in `scripts/` when you want to run everything from your local machine and directly control each step.

### Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy and fill in environment variables
cp .env.template .env
# Edit .env with your values
```

### Build the Nominatim database

Run these scripts in order:

**Step 1: Refresh database token**

```bash
python scripts/00_refresh_environment.py
```

Fetches a fresh OAuth token from Databricks and updates `PGPASSWORD` in `.env`. Tokens expire after 1 hour -- re-run whenever you get auth errors.

**Step 2: Setup PostGIS extensions**

```bash
python scripts/01_setup_postgis.py
```

**Step 3: Download OSM data**

```bash
# Small test region (~2MB, ~5-10 min import)
python scripts/02_download_osm_data.py --region monaco

# US states
python scripts/02_download_osm_data.py --region virginia --region maryland

# Custom URL
python scripts/02_download_osm_data.py --url https://download.geofabrik.de/europe/switzerland-latest.osm.pbf
```

**Step 4: Build Nominatim server**

```bash
# Import a single file
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf

# Import multiple files
bash scripts/03_build_nominatim_server.sh osm_data/virginia-latest.osm.pbf osm_data/maryland-latest.osm.pbf

# With more threads
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf --threads 4
```

**Import times** (approximate):
- Monaco: ~5-10 minutes
- US State: ~30-60 minutes
- Country: 1-4 hours

**Resume failed indexing** (if import was interrupted):

```bash
bash scripts/04_resume_failed_indexing.sh
bash scripts/04_resume_failed_indexing.sh --threads 4
```

### Run the API locally

```bash
python app/app.py
# or with auto-reload:
uvicorn app.app:app --reload
```

Visit http://localhost:8000/docs for interactive Swagger documentation.

### Test locally

```bash
# Health check
curl http://localhost:8000/health

# Forward geocoding
curl "http://localhost:8000/search?q=Berlin,+Germany&limit=1"

# Reverse geocoding
curl "http://localhost:8000/reverse?lat=52.5200&lon=13.4050"

# OSM ID lookup
curl "http://localhost:8000/lookup?osm_ids=R146656,W5013364"
```

---

## API Reference

Base URL: `http://localhost:8000` (local) or your Databricks App URL.
Interactive docs at `/docs` (Swagger) and `/redoc` (ReDoc).
All geocoding endpoints are also available under `/api/*` (e.g. `/api/search`, `/api/reverse`).

### `GET /health`

Lightweight health check. Returns pool and token stats.

```json
{
  "status": "healthy",
  "database": "connected",
  "pool_size": 2,
  "token_ttl_s": 3241
}
```

### `GET /status`

Detailed status with data freshness and indexed place count.

```json
{
  "status": "ok",
  "version": "2.0.0",
  "database": "connected",
  "data_updated": "2025-03-09 08:00:00+00:00",
  "place_count": 1284503
}
```

### `GET /search`

Forward geocoding: address/place name to coordinates.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `q` | yes | -- | Place name or address |
| `limit` | no | `10` | Max results (1-50) |
| `countrycodes` | no | -- | ISO 3166-1 alpha-2 codes (e.g. `us,ca`) |

### `GET /reverse`

Reverse geocoding: coordinates to place name.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `lat` | yes | -- | Latitude (-90 to 90) |
| `lon` | yes | -- | Longitude (-180 to 180) |
| `radius` | no | `1000` | Search radius in metres (1-50000) |
| `limit` | no | `1` | Number of results (1-10) |

### `GET /lookup`

Look up places by OpenStreetMap ID.

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `osm_ids` | yes | -- | Comma-separated IDs prefixed with `N`/`W`/`R` (e.g. `R146656,W5013364`) |

### Error responses

| HTTP Code | Meaning |
|-----------|---------|
| `400` | Bad request (e.g. invalid OSM IDs) |
| `404` | No results found |
| `500` | Internal server error |
| `503` | Database unavailable (token expired, connection lost -- will auto-recover) |

---

## Troubleshooting

### Token expired

OAuth tokens expire after 1 hour. Refresh:
```bash
python scripts/00_refresh_environment.py
```

### PostGIS not found

```bash
python scripts/01_setup_postgis.py
```

### Import takes too long

- Start with Monaco for testing
- Increase threads: `--threads 4`
- Use a larger Lakebase compute tier (`PG_MAX_CU`)

### No results for valid addresses

- Address may be outside the imported region
- Try broader search terms
- Check `countrycodes` matches imported data

### Database connection fails

1. Verify `.env` has correct values
2. Refresh token: `python scripts/00_refresh_environment.py`
3. Confirm the Lakebase instance is running in the Databricks console
4. Check `PGHOST` and `PG_PROJECT_ID` match

---

## Resources

- [Nominatim Documentation](https://nominatim.org/release-docs/latest/)
- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Geofabrik OSM Downloads](https://download.geofabrik.de/)

## License

This project uses Nominatim, which is licensed under GPL v2.
