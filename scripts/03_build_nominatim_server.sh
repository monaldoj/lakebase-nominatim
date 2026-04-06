#!/bin/bash
# Build Nominatim server by importing OSM data.
#
# Prerequisites:
#   1. Run: python scripts/00_refresh_environment.py
#   2. Run: python scripts/01_setup_postgis.py
#   3. Run: python scripts/02_download_osm_data.py --region <region>

set -Eeuo pipefail

# Global state for pgpass file and background refresher
PGPASS_TEMP=""
REFRESHER_PID=""
IMPORT_PID=""
WATCHDOG_PID=""
IMPORT_PG_SETTINGS_APPLIED=false
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$PROJECT_ROOT/nominatim_project"
IMPORT_LOG="$PROJECT_DIR/import.log"
PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-15}"
TOKEN_REFRESH_SECONDS="${TOKEN_REFRESH_SECONDS:-2700}" # 45 minutes
STALL_TIMEOUT_SECONDS="${STALL_TIMEOUT_SECONDS:-0}"    # 0 disables watchdog abort
IMPORT_RETRY_MAX="${IMPORT_RETRY_MAX:-2}"              # retries after first failed attempt

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
    log "ERROR: $*"
    exit 1
}

# Clean up temp pgpass file and background refresher on exit
cleanup() {
    local exit_code=$?
    set +e

    if [ "$IMPORT_PG_SETTINGS_APPLIED" = true ]; then
        restore_postgres_normal_settings || log "WARNING: Failed to fully restore PostgreSQL user settings to normal."
    fi

    if [ -n "$WATCHDOG_PID" ]; then
        kill "$WATCHDOG_PID" 2>/dev/null || true
    fi
    if [ -n "$IMPORT_PID" ]; then
        kill "$IMPORT_PID" 2>/dev/null || true
    fi
    if [ -n "$REFRESHER_PID" ]; then
        kill "$REFRESHER_PID" 2>/dev/null || true
    fi
    if [ -n "$PGPASS_TEMP" ] && [ -f "$PGPASS_TEMP" ]; then
        rm -f "$PGPASS_TEMP"
    fi
    return "$exit_code"
}
trap cleanup EXIT INT TERM

# Write a token into the pgpass file.
# Uses umask 177 so the intermediate .tmp file is created with 0600 permissions;
# mv then carries those permissions to the destination (preventing the
# "group or world access" warning from psql).
_write_pgpass() {
    local token="$1"
    (umask 177 && printf '%s:%s:*:%s:%s\n' "$PGHOST" "$PGPORT" "$PGUSER" "$token" \
        > "${PGPASS_TEMP}.tmp")
    mv "${PGPASS_TEMP}.tmp" "$PGPASS_TEMP"
}

# Set up a temp pgpass file and export PGPASSFILE so all child processes
# (osm2pgsql, nominatim, psql) read the token from disk on each new connection.
# Must be called after the initial token is in PGPASSWORD.
setup_pgpass() {
    PGPASS_TEMP="$(umask 177 && mktemp)"
    export PGPASSFILE="$PGPASS_TEMP"
    _write_pgpass "$PGPASSWORD"
    unset PGPASSWORD   # PGPASSWORD takes precedence over PGPASSFILE; remove it
    log "✓ Using pgpass file for credentials (0600): $PGPASS_TEMP"
}

# Start a background process that refreshes the Databricks OAuth token in the
# pgpass file every 45 minutes using DATABRICKS_HOST and DATABRICKS_TOKEN.
# Tokens expire after 1 hour, so this keeps the credential current for
# long-running osm2pgsql/nominatim runs.
start_token_refresher() {
    local pass_file="$PGPASS_TEMP"
    local root_dir="$PROJECT_ROOT"
    (
        while true; do
            sleep "$TOKEN_REFRESH_SECONDS"
            log "[Background] Refreshing OAuth token..."
            python "$root_dir/scripts/00_refresh_environment.py" > /dev/null 2>&1 || {
                log "[Background] WARNING: token refresh command failed"
                continue
            }
            new_token="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "$PGPASSWORD"' _ "$root_dir/.env" 2>/dev/null || true)"
            if [ -n "$new_token" ]; then
                (umask 177 && printf '%s:%s:*:%s:%s\n' "$PGHOST" "$PGPORT" "$PGUSER" "$new_token" \
                    > "${pass_file}.tmp") && mv "${pass_file}.tmp" "$pass_file"
                log "[Background] Token refreshed in pgpass"
            else
                log "[Background] WARNING: Could not extract new token from .env"
            fi
        done
    ) &
    REFRESHER_PID=$!
    log "✓ Background token refresher started (PID: $REFRESHER_PID, interval: ${TOKEN_REFRESH_SECONDS}s)"
}

load_env() {
    if [ -f "$PROJECT_ROOT/.env" ]; then
        set -a
        # shellcheck disable=SC1091
        source "$PROJECT_ROOT/.env"
        set +a
    fi
}

require_var() {
    local name="$1"
    if [ -z "${!name:-}" ]; then
        die "Required variable '$name' is missing. Run: python scripts/00_refresh_environment.py"
    fi
}

refresh_token_now() {
    log "Refreshing OAuth token..."
    python "$PROJECT_ROOT/scripts/00_refresh_environment.py" > /dev/null
    load_env
    require_var PGPASSWORD
    log "✓ OAuth token refreshed"
}

refresh_pgpass_token_now() {
    refresh_token_now
    _write_pgpass "$PGPASSWORD"
    unset PGPASSWORD
    log "✓ Updated pgpass with refreshed OAuth token"
}

detect_default_threads() {
    local cpu_count="4"
    if command -v nproc >/dev/null 2>&1; then
        cpu_count="$(nproc)"
    elif command -v sysctl >/dev/null 2>&1; then
        cpu_count="$(sysctl -n hw.logicalcpu 2>/dev/null || echo 4)"
    fi
    if [ "$cpu_count" -lt 2 ]; then
        echo "2"
    elif [ "$cpu_count" -gt 12 ]; then
        echo "12"
    else
        echo "$cpu_count"
    fi
}

detect_cache_mb() {
    local total_mb=""
    if [ -r /proc/meminfo ]; then
        total_mb="$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 ))"
    elif command -v sysctl >/dev/null 2>&1; then
        total_mb="$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 ))"
    fi

    if [ -z "$total_mb" ] || [ "$total_mb" -le 0 ]; then
        echo "2000"
        return
    fi

    # Use roughly one-third of host memory for osm2pgsql cache.
    local cache="$(( total_mb / 3 ))"
    if [ "$cache" -lt 2000 ]; then
        cache=2000
    elif [ "$cache" -gt 16000 ]; then
        cache=16000
    fi
    echo "$cache"
}

get_mtime() {
    local path="$1"
    stat -f %m "$path" 2>/dev/null || stat -c %Y "$path" 2>/dev/null || echo "0"
}

log_has_ssl_disconnect_error() {
    local log_file="$1"
    grep -qE "SSL connection has been closed unexpectedly|consuming input failed" "$log_file"
}

run_import_with_watchdog() {
    local continue_at="$1"
    local import_threads="$2"
    local start_ts
    local import_exit_code
    local import_cmd=()

    if [ "$continue_at" = "none" ]; then
        import_cmd=(nominatim import "${OSM_FILE_ARGS[@]}")
    else
        import_cmd=(nominatim import --continue "$continue_at")
    fi

    import_cmd+=(--threads "$import_threads" --osm2pgsql-cache "$OSM2PGSQL_CACHE_MB")

    {
        echo ""
        echo "============================================================"
        echo "Import attempt ($(date '+%Y-%m-%d %H:%M:%S'))"
        if [ "$continue_at" = "none" ]; then
            echo "Mode: fresh import"
        else
            echo "Mode: continue at '$continue_at'"
        fi
        echo "Threads: $import_threads"
        echo "============================================================"
    } >> "$IMPORT_LOG"

    start_ts="$(date +%s)"
    "${import_cmd[@]}" >> "$IMPORT_LOG" 2>&1 &
    IMPORT_PID=$!

    (
        last_mtime=""
        now=""
        idle=""
        while kill -0 "$IMPORT_PID" 2>/dev/null; do
            sleep 60
            [ -f "$IMPORT_LOG" ] || continue
            last_mtime="$(get_mtime "$IMPORT_LOG")"
            now="$(date +%s)"
            idle="$(( now - last_mtime ))"

            # Emit heartbeat every cycle so long-running imports look alive.
            log "Import running... log: $IMPORT_LOG (idle ${idle}s)"

            if [ "$STALL_TIMEOUT_SECONDS" -gt 0 ] && [ "$idle" -ge "$STALL_TIMEOUT_SECONDS" ]; then
                log "ERROR: Import log has been idle for ${idle}s (threshold ${STALL_TIMEOUT_SECONDS}s). Aborting import."
                kill "$IMPORT_PID" 2>/dev/null || true
                exit 1
            fi
        done
    ) &
    WATCHDOG_PID=$!

    import_exit_code=0
    wait "$IMPORT_PID" || import_exit_code=$?
    kill "$WATCHDOG_PID" 2>/dev/null || true
    WATCHDOG_PID=""
    IMPORT_PID=""

    log "Import attempt finished in $(( $(date +%s) - start_ts ))s with exit code $import_exit_code"
    return "$import_exit_code"
}

# Parse arguments
OSM_FILES=()
THREADS=""
OSM2PGSQL_CACHE_MB=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --threads)
            [ "$#" -ge 2 ] || die "--threads requires a value"
            THREADS="$2"
            shift 2
            ;;
        --threads=*)
            THREADS="${1#*=}"
            shift
            ;;
        --cache-mb)
            [ "$#" -ge 2 ] || die "--cache-mb requires a value"
            OSM2PGSQL_CACHE_MB="$2"
            shift 2
            ;;
        --cache-mb=*)
            OSM2PGSQL_CACHE_MB="${1#*=}"
            shift
            ;;
        --stall-timeout-minutes)
            [ "$#" -ge 2 ] || die "--stall-timeout-minutes requires a value"
            STALL_TIMEOUT_SECONDS="$(( $2 * 60 ))"
            shift 2
            ;;
        --stall-timeout-minutes=*)
            STALL_TIMEOUT_SECONDS="$(( ${1#*=} * 60 ))"
            shift
            ;;
        *.pbf|*.osm)
            OSM_FILES+=("$1")
            shift
            ;;
        *)
            die "Unknown argument: $1"
            ;;
    esac
done

[ -n "$THREADS" ] || THREADS="$(detect_default_threads)"
[ -n "$OSM2PGSQL_CACHE_MB" ] || OSM2PGSQL_CACHE_MB="$(detect_cache_mb)"

[[ "$THREADS" =~ ^[0-9]+$ ]] || die "Threads must be a positive integer"
[[ "$OSM2PGSQL_CACHE_MB" =~ ^[0-9]+$ ]] || die "Cache size must be a positive integer (MB)"
[[ "$STALL_TIMEOUT_SECONDS" =~ ^[0-9]+$ ]] || die "Stall timeout must be a non-negative integer"
[[ "$IMPORT_RETRY_MAX" =~ ^[0-9]+$ ]] || die "IMPORT_RETRY_MAX must be a non-negative integer"
if [ "$THREADS" -lt 1 ]; then
    die "Threads must be >= 1"
fi

if [ ${#OSM_FILES[@]} -eq 0 ]; then
    echo "Usage: $0 <osm-file.pbf> [osm-file2.pbf ...] [--threads N] [--cache-mb MB]"
    echo ""
    echo "Options:"
    echo "  --threads  Number of import threads (default: auto-detected)"
    echo "  --cache-mb osm2pgsql cache in MB (default: auto-detected)"
    echo "  --stall-timeout-minutes Abort if import log is idle for N minutes (default: 0=disabled)"
    echo ""
    echo "Examples:"
    echo "  $0 osm_data/monaco-latest.osm.pbf"
    echo "  $0 osm_data/iran-latest.osm.pbf osm_data/iraq-latest.osm.pbf --threads 8 --cache-mb 8000"
    exit 1
fi

for f in "${OSM_FILES[@]}"; do
    if [ ! -f "$f" ]; then
        echo "Error: OSM file not found: $f"
        exit 1
    fi
done

load_env
require_var PGHOST
require_var PGPORT
require_var PGUSER
require_var PGDATABASE

echo "============================================================"
echo "Nominatim OSM Import"
echo "============================================================"
echo ""
echo "OSM Files:    ${OSM_FILES[*]}"
echo "Threads:      $THREADS"
echo "Cache (MB):   $OSM2PGSQL_CACHE_MB"
echo "Database:     $PGUSER@$PGHOST:$PGPORT/$PGDATABASE"
echo "Clean DB:     always (automatic drop/rebuild)"
echo "Retry mode:   up to $IMPORT_RETRY_MAX retry attempts on SSL disconnect"
if [ "$STALL_TIMEOUT_SECONDS" -gt 0 ]; then
    echo "Stall abort:  ${STALL_TIMEOUT_SECONDS}s idle log timeout"
else
    echo "Stall abort:  disabled"
fi
echo ""

refresh_token_now
setup_pgpass
start_token_refresher

# Function to run psql
run_psql() {
    local sql="$1"
    local db="${2:-postgres}"
    psql -w -v ON_ERROR_STOP=1 \
        "host=$PGHOST port=$PGPORT dbname=$db user=$PGUSER sslmode=${PGSSLMODE:-prefer} connect_timeout=$PGCONNECT_TIMEOUT" \
        -tAc "$sql"
}

apply_postgres_import_settings() {
    echo "============================================================"
    echo "Applying PostgreSQL user-level IMPORT optimizations..."
    echo "============================================================"

    # Managed databases usually allow user-level settings even without superuser.
    run_psql "ALTER USER \"$PGUSER\" SET maintenance_work_mem = '1GB';" postgres > /dev/null 2>&1 && \
        echo "✓ Set maintenance_work_mem = 1GB" || \
        echo "⚠ Could not set maintenance_work_mem"

    run_psql "ALTER USER \"$PGUSER\" SET work_mem = '128MB';" postgres > /dev/null 2>&1 && \
        echo "✓ Set work_mem = 128MB" || \
        echo "⚠ Could not set work_mem"

    run_psql "ALTER USER \"$PGUSER\" SET synchronous_commit = 'off';" postgres > /dev/null 2>&1 && \
        echo "✓ Set synchronous_commit = off" || \
        echo "⚠ Could not set synchronous_commit"

    run_psql "ALTER USER \"$PGUSER\" SET random_page_cost = 1.1;" postgres > /dev/null 2>&1 && \
        echo "✓ Set random_page_cost = 1.1" || \
        echo "⚠ Could not set random_page_cost"

    IMPORT_PG_SETTINGS_APPLIED=true
    echo ""
}

restore_postgres_normal_settings() {
    echo ""
    echo "============================================================"
    echo "Restoring PostgreSQL user-level settings to NORMAL..."
    echo "============================================================"

    run_psql "ALTER USER \"$PGUSER\" RESET maintenance_work_mem;" postgres > /dev/null 2>&1 && \
        echo "✓ Reset maintenance_work_mem" || \
        echo "⚠ Could not reset maintenance_work_mem"

    run_psql "ALTER USER \"$PGUSER\" RESET work_mem;" postgres > /dev/null 2>&1 && \
        echo "✓ Reset work_mem" || \
        echo "⚠ Could not reset work_mem"

    run_psql "ALTER USER \"$PGUSER\" SET synchronous_commit = 'on';" postgres > /dev/null 2>&1 && \
        echo "✓ Set synchronous_commit = on" || \
        echo "⚠ Could not set synchronous_commit = on"

    run_psql "ALTER USER \"$PGUSER\" RESET random_page_cost;" postgres > /dev/null 2>&1 && \
        echo "✓ Reset random_page_cost" || \
        echo "⚠ Could not reset random_page_cost"

    IMPORT_PG_SETTINGS_APPLIED=false
    echo ""
}

echo "============================================================"
echo "Setting up prerequisites..."
echo "============================================================"
echo ""

# Create www-data user if needed
echo "Checking for www-data user..."
USER_EXISTS="$(run_psql "SELECT 1 FROM pg_roles WHERE rolname='www-data' LIMIT 1;" postgres | tr -d '[:space:]' || true)"
if [ "$USER_EXISTS" != "1" ]; then
    run_psql "CREATE USER \"www-data\";" postgres > /dev/null
    echo "✓ Created www-data user"
else
    echo "✓ www-data user exists"
fi
echo ""

# Handle database
echo "Ensuring clean database '$PGDATABASE'..."
DB_EXISTS="$(run_psql "SELECT 1 FROM pg_database WHERE datname='$PGDATABASE' LIMIT 1;" postgres | tr -d '[:space:]' || true)"

if [ "$DB_EXISTS" = "1" ]; then
    echo "⚠  Database exists - dropping before import..."
    run_psql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$PGDATABASE' AND pid <> pg_backend_pid();" postgres > /dev/null 2>&1 || true
    sleep 1
    run_psql "DROP DATABASE \"$PGDATABASE\";" postgres > /dev/null
    sleep 1
    echo "✓ Database dropped"
else
    echo "✓ Database does not exist yet; import will initialize it"
fi
echo ""

apply_postgres_import_settings

mkdir -p "$PROJECT_DIR"
cd "$PROJECT_DIR"

cat > .env << EOF
PGHOST=$PGHOST
PGPORT=$PGPORT
PGUSER=$PGUSER
PGDATABASE=$PGDATABASE
PGSSLMODE=${PGSSLMODE:-prefer}
NOMINATIM_DATABASE_DSN=pgsql:dbname=$PGDATABASE;host=$PGHOST;port=$PGPORT;user=$PGUSER;sslmode=${PGSSLMODE:-prefer}

NOMINATIM_IMPORT_THREADS=$THREADS
NOMINATIM_TOKENIZER=icu
EOF

echo "✓ Created project directory"
echo ""

# Build --osm-file args array
OSM_FILE_ARGS=()
for f in "${OSM_FILES[@]}"; do
    OSM_FILE_ARGS+=(--osm-file "../$f")
done

echo "============================================================"
echo "Starting Nominatim import..."
echo "============================================================"
echo ""
echo "This may take a while depending on data size:"
echo "  Monaco:        ~5-10 minutes"
echo "  Country-level: 30 minutes to several hours"
echo ""
echo "Token will be refreshed automatically in the background."
echo ""

if [ "$STALL_TIMEOUT_SECONDS" -gt 0 ]; then
    log "Import watchdog enabled (${STALL_TIMEOUT_SECONDS}s idle log timeout)."
fi

: > "$IMPORT_LOG"

attempt=1
max_attempts="$(( IMPORT_RETRY_MAX + 1 ))"
IMPORT_EXIT_CODE=1
continue_mode="none"
attempt_threads="$THREADS"

while [ "$attempt" -le "$max_attempts" ]; do
    log "Starting import attempt ${attempt}/${max_attempts} (mode: $continue_mode, threads: $attempt_threads)"

    if run_import_with_watchdog "$continue_mode" "$attempt_threads"; then
        IMPORT_EXIT_CODE=0
        break
    fi

    IMPORT_EXIT_CODE=$?

    if [ "$attempt" -lt "$max_attempts" ] && log_has_ssl_disconnect_error "$IMPORT_LOG"; then
        log "Detected SSL connection drop during import. Refreshing token and resuming at indexing..."
        refresh_pgpass_token_now
        continue_mode="indexing"
        if [ "$attempt_threads" -gt 4 ]; then
            attempt_threads=4
            log "Reducing retry thread count to $attempt_threads for connection stability."
        fi
        attempt=$((attempt + 1))
        sleep 3
        continue
    fi

    break
done

if [ "$IMPORT_EXIT_CODE" -ne 0 ]; then
    echo ""
    echo "============================================================"
    echo "✗ Import failed (exit code: $IMPORT_EXIT_CODE)"
    echo "============================================================"
    echo ""
    echo "Last lines from import log:"
    tail -n 40 "$IMPORT_LOG" || true
    exit "$IMPORT_EXIT_CODE"
fi

cd ..

echo ""
echo "============================================================"
echo "✓ Import completed successfully!"
echo "============================================================"
echo ""
echo "Database: $PGDATABASE on $PGHOST"
echo ""
echo "Next steps:"
echo "  1. Test:          cd $PROJECT_DIR && nominatim admin --check-database"
echo "  2. Start API:     uvicorn app.app:app --reload"
echo "  3. Test geocoding: curl 'http://localhost:8000/search?q=Monaco'"
