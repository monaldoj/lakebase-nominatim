# Nominatim Geocoding API on Databricks

OpenStreetMap-based geocoding service running on Databricks Apps with Lakebase Managed Postgres.

## Features

- **Forward Geocoding**: Convert addresses to coordinates
- **Reverse Geocoding**: Convert coordinates to addresses
- **OSM Lookup**: Find places by OpenStreetMap ID
- **FastAPI**: Modern, async Python API framework
- **Databricks Apps**: Serverless deployment on Databricks
- **Managed Postgres**: Lakebase managed database with PostGIS

## Quick Start

### Prerequisites

- Python 3.9+
- Databricks workspace with CLI configured
- Lakebase Managed Postgres instance
- `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables set
### Installation

```bash
# Clone and navigate to project
cd nominatim

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Configure Environment

Create a `.env` file in the project root:

```env
# Databricks (set in shell, not .env)
# DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
# DATABRICKS_TOKEN=your-databricks-token

# Database Instance
PGINSTANCENAME=your-instance-id

# Database Connection (PGPASSWORD auto-updated by script)
PGHOST=your-instance.database.cloud.databricks.com
PGUSER=your.email@company.com
PGPASSWORD=<auto-refreshed>
PGDATABASE=nominatim
PGPORT=5432
PGSSLMODE=require

# Nominatim
NOMINATIM_SCHEMA=public
NOMINATIM_URL=http://localhost:8000
```

## Setup Workflow

Run these scripts **in order** to build your Nominatim geocoding server:

### Step 1: Refresh Database Token

```bash
python scripts/00_refresh_environment.py
```

- Fetches a fresh OAuth token from Databricks
- Updates `PGPASSWORD` in your `.env` file
- Token is valid for 1 hour

**Note**: Run this whenever you get authentication errors (token expires after 1 hour).

### Step 2: Setup PostGIS Extensions

```bash
python scripts/01_setup_postgis.py
```

- Installs PostGIS and hstore extensions on your Postgres instance
- Verifies extensions are working correctly

### Step 3: Download OSM Data

```bash
# Small test region (Monaco ~2MB, ~5-10 min import)
python scripts/02_download_osm_data.py --region monaco

# Other small regions
python scripts/02_download_osm_data.py --region liechtenstein

# US states
python scripts/02_download_osm_data.py --region california
python scripts/02_download_osm_data.py --region virginia
python scripts/02_download_osm_data.py --region new-york
python scripts/02_download_osm_data.py --region texas

# Countries (large files, longer import times)
python scripts/02_download_osm_data.py --region germany
python scripts/02_download_osm_data.py --region france
python scripts/02_download_osm_data.py --region usa

# Custom URL
python scripts/02_download_osm_data.py --url https://download.geofabrik.de/europe/switzerland-latest.osm.pbf
```

Downloads to `osm_data/` directory by default. Use `--region` and `--url` multiple times to download several files at once:

```bash
# Download multiple regions in one command
python scripts/02_download_osm_data.py --region virginia --region maryland
```

### Step 4: Build Nominatim Server

```bash
# Import a single file
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf

# Import multiple files in one build
bash scripts/03_build_nominatim_server.sh osm_data/virginia-latest.osm.pbf osm_data/maryland-latest.osm.pbf

# With more threads (faster)
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf --threads 4

# Database is always recreated clean automatically on each import run
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf
```

This script:
- Creates the `nominatim` database if needed
- Imports OSM data
- Sets up all Nominatim tables and indexes
- Creates `nominatim_project/` directory with configuration
- Automatically enables import-optimized PostgreSQL user settings, then restores normal settings on exit

**Import Times**:
- Monaco: ~5-10 minutes
- US State: ~30-60 minutes
- Country: 1-4 hours

## Testing Locally

### Start the API Server

```bash
# Using Python directly
python app.py

# Or using uvicorn with auto-reload
uvicorn app:app --reload

# Or using make
make local
make local-reload  # with auto-reload
```

Visit http://localhost:8000/docs for interactive API documentation.

### Quick Query After `app.py`

If you started with `python app.py`, run a query from a second terminal:

```bash
curl "http://localhost:8000/search?q=Tehran&limit=3"
```

### Test with Geocoding Script

```bash
# Forward geocoding (address → coordinates)
python scripts/geocode.py --search "1600 Amphitheatre Parkway, Mountain View, CA"
python scripts/geocode.py --search "Eiffel Tower, Paris"

# Reverse geocoding (coordinates → address)
python scripts/geocode.py --reverse 37.4224764 -122.0842499

# With options
python scripts/geocode.py --search "Paris" --limit 3
python scripts/geocode.py --search "Springfield" --country us
python scripts/geocode.py --reverse 48.8584 2.2945 --zoom 10

# Output JSON
python scripts/geocode.py --search "Monaco" --json

# Custom server URL
python scripts/geocode.py --search "Berlin" --url http://localhost:8000
```

### Test with curl

```bash
# Health check
curl http://localhost:8000/health

# Status
curl http://localhost:8000/status

# Forward geocoding
curl "http://localhost:8000/search?q=Berlin,+Germany&limit=1"

# Reverse geocoding
curl "http://localhost:8000/reverse?lat=52.5200&lon=13.4050"

# OSM Lookup
curl "http://localhost:8000/lookup?osm_ids=R146656"
```

## Deployment to Databricks

### Using Makefile (Recommended)

```bash
# Show all commands
make help

# Deploy to development
make deploy-dev

# View logs
make logs-dev
make logs-dev-follow  # follow logs

# Check status
make status-dev

# Check health
make health-dev

# Build/rebuild Nominatim database from OSM files
make build-import OSM_FILES="osm_data/monaco-latest.osm.pbf"

# Resume failed indexing after interrupted import
make resume-index

# Quick local query (after app.py/make local is running)
make local-query Q=Tehran

# Deploy to production
make deploy-prod

# Full dev workflow (validate, deploy, follow logs)
make dev
```

### Using Databricks CLI

```bash
# Validate bundle
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod

# View logs
databricks apps logs nominatim-geocoding-api-dev
databricks apps logs nominatim-geocoding-api-dev --follow

# Check status
databricks apps get nominatim-geocoding-api-dev

# List apps
databricks apps list

# Destroy deployment
databricks bundle destroy -t dev
```

### Get App URL

```bash
# Using databricks CLI
databricks apps get nominatim-geocoding-api-dev

# Or extract just the URL
databricks apps get nominatim-geocoding-api-dev --json | jq -r '.url'
```

## API Endpoints

### `GET /health`
Simple health check (returns 200 OK).

### `GET /status`
Database status and version information.

### `GET /search` - Forward Geocoding
Convert address to coordinates.

**Parameters**:
- `q` (required): Search query
- `limit` (optional, default=10): Max results (1-50)
- `countrycodes` (optional): Comma-separated country codes (e.g., "us,ca")

**Example**:
```bash
curl "https://your-app-url/search?q=Berlin,+Germany&limit=1"
```

### `GET /reverse` - Reverse Geocoding
Convert coordinates to address.

**Parameters**:
- `lat` (required): Latitude (-90 to 90)
- `lon` (required): Longitude (-180 to 180)
- `zoom` (optional, default=18): Detail level (0=country, 18=building)

**Example**:
```bash
curl "https://your-app-url/reverse?lat=52.5200&lon=13.4050"
```

### `GET /lookup` - OSM Lookup
Look up places by OpenStreetMap ID.

**Parameters**:
- `osm_ids` (required): Comma-separated OSM IDs (e.g., "R146656,N240109189")

**Example**:
```bash
curl "https://your-app-url/lookup?osm_ids=R146656"
```

## Project Structure

```
nominatim/
├── app.py                          # FastAPI application
├── requirements.txt                # Python dependencies
├── databricks.yml                  # Databricks bundle configuration
├── Makefile                        # Deployment commands
├── README.md                       # This file
├── .env                            # Environment variables (not in git)
├── .env.template                   # Environment template
├── scripts/
│   ├── 00_refresh_environment.py   # Refresh database OAuth token
│   ├── 01_setup_postgis.py         # Install PostGIS extensions
│   ├── 02_download_osm_data.py     # Download OSM data files
│   ├── 03_build_nominatim_server.sh # Import OSM data and build Nominatim
│   ├── 04_resume_failed_indexing.sh # Resume interrupted indexing safely
│   ├── geocode.py                  # CLI geocoding tool
│   └── query_nominatim_db.py       # Direct database query tool
├── osm_data/                       # Downloaded OSM files
├── nominatim_project/              # Nominatim working directory (created by script)
└── tests/                          # Tests
```

## Troubleshooting

### Token Expired Errors

**Symptom**: Authentication failures, "invalid credentials" errors

**Solution**: OAuth tokens expire after 1 hour. Refresh it:
```bash
python scripts/00_refresh_environment.py
```

### PostGIS Not Found

**Symptom**: "extension postgis does not exist"

**Solution**: Run the PostGIS setup script:
```bash
python scripts/01_setup_postgis.py
```

### Import Takes Too Long

**Solutions**:
- Start with a smaller region (Monaco) for testing
- Increase threads: `bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf --threads 4`
- Use import-optimized PostgreSQL user settings automatically via the build script
- Use a more powerful database instance

### Resume Failed Indexing

If import was interrupted during indexing (for example after rank 30), resume without rebuilding:

```bash
# Default retry-friendly setting
bash scripts/04_resume_failed_indexing.sh

# Or set threads explicitly
bash scripts/04_resume_failed_indexing.sh --threads 4
```

### No Results for Valid Addresses

**Possible causes**:
- Address is outside the imported region
- Try broader search terms
- Check country codes match imported data

### API Returns 500 Errors

**Debugging steps**:
```bash
# Check logs
make logs-dev
databricks apps logs nominatim-geocoding-api-dev

# Verify database connection
python scripts/query_nominatim_db.py

# Test locally first
python app.py
```

### Database Connection Fails

**Debugging steps**:
1. Check `.env` file has correct values
2. Refresh token: `python scripts/00_refresh_environment.py`
3. Verify instance is running in Databricks console
4. Check `PGHOST` and `PGINSTANCENAME` match

## Development

### Run Tests

```bash
# Run all tests
pytest tests/

# With coverage
pytest tests/ --cov

# Using make
make test
```

### Code Quality

```bash
# Format code
black .

# Lint
flake8 .

# Type checking
mypy app.py
```

### Clean Cache

```bash
make clean
```

## Performance Tuning

### Database

- **Indexes**: Automatically created by Nominatim import
- **Connection Pooling**: Implemented in `app.py`
- **Query Optimization**: Use appropriate zoom levels for reverse geocoding

### API

- **Caching**: Consider adding Redis for frequently requested locations
- **Rate Limiting**: Add middleware for production use
- **Monitoring**: Use Databricks monitoring and logging

### Scaling

- **API Layer**: Databricks Apps auto-scales
- **Database**: May need vertical scaling for large datasets
- **Regional Data**: Import only necessary regions to reduce size

## Data Updates

OSM data changes frequently. To update:

```bash
# Re-download latest data
python scripts/02_download_osm_data.py --region monaco

# Re-import (always cleans and rebuilds)
bash scripts/03_build_nominatim_server.sh osm_data/monaco-latest.osm.pbf
```

## Resources

- [Nominatim Documentation](https://nominatim.org/release-docs/latest/)
- [Databricks Apps Documentation](https://docs.databricks.com/apps/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Geofabrik OSM Downloads](https://download.geofabrik.de/)
- [PostGIS Documentation](https://postgis.net/documentation/)

## License

This project uses Nominatim, which is licensed under GPL v2.

## Support

For issues:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review app logs: `make logs-dev`
3. Validate configuration: `databricks bundle validate`
4. Test locally first: `make local`
