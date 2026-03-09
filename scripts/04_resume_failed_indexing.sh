#!/bin/bash
# Resume Nominatim indexing after an interrupted import.
#
# Usage:
#   bash scripts/04_resume_failed_indexing.sh
#   bash scripts/04_resume_failed_indexing.sh --threads 4

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$PROJECT_ROOT/nominatim_project"
THREADS="${RESUME_INDEX_THREADS:-4}"
PGPASS_TEMP=""
REFRESHER_PID=""
TOKEN_REFRESH_SECONDS="${TOKEN_REFRESH_SECONDS:-2700}" # 45 minutes

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
    log "ERROR: $*"
    exit 1
}

cleanup() {
    if [ -n "$REFRESHER_PID" ]; then
        kill "$REFRESHER_PID" 2>/dev/null || true
    fi
    if [ -n "$PGPASS_TEMP" ] && [ -f "$PGPASS_TEMP" ]; then
        rm -f "$PGPASS_TEMP"
    fi
}
trap cleanup EXIT INT TERM

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
        *)
            die "Unknown argument: $1"
            ;;
    esac
done

[[ "$THREADS" =~ ^[0-9]+$ ]] || die "Threads must be a positive integer"
if [ "$THREADS" -lt 1 ]; then
    die "Threads must be >= 1"
fi

load_root_env() {
    [ -f "$PROJECT_ROOT/.env" ] || die "Missing $PROJECT_ROOT/.env"
    set -a
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.env"
    set +a
}

refresh_token_now() {
    log "Refreshing OAuth token..."
    python "$PROJECT_ROOT/scripts/00_refresh_environment.py" > /dev/null
    load_root_env
    [ -n "${PGPASSWORD:-}" ] || die "PGPASSWORD is missing after token refresh"
    log "✓ OAuth token refreshed"
}

write_pgpass() {
    local token="$1"
    (umask 177 && printf '%s:%s:*:%s:%s\n' "$PGHOST" "$PGPORT" "$PGUSER" "$token" > "$PGPASS_TEMP")
}

start_token_refresher() {
    (
        while true; do
            sleep "$TOKEN_REFRESH_SECONDS"
            log "[Background] Refreshing OAuth token..."
            python "$PROJECT_ROOT/scripts/00_refresh_environment.py" > /dev/null 2>&1 || {
                log "[Background] WARNING: token refresh command failed"
                continue
            }
            new_token="$(bash -c 'set -a; source "$1"; set +a; printf "%s" "$PGPASSWORD"' _ "$PROJECT_ROOT/.env" 2>/dev/null || true)"
            if [ -n "$new_token" ]; then
                write_pgpass "$new_token"
                log "[Background] Token refreshed in pgpass"
            else
                log "[Background] WARNING: Could not extract new token from .env"
            fi
        done
    ) &
    REFRESHER_PID=$!
    log "✓ Background token refresher started (PID: $REFRESHER_PID, interval: ${TOKEN_REFRESH_SECONDS}s)"
}

[ -d "$PROJECT_DIR" ] || die "Missing project directory: $PROJECT_DIR"

refresh_token_now

[ -n "${PGHOST:-}" ] || die "PGHOST is missing in $PROJECT_ROOT/.env"
[ -n "${PGPORT:-}" ] || die "PGPORT is missing in $PROJECT_ROOT/.env"
[ -n "${PGUSER:-}" ] || die "PGUSER is missing in $PROJECT_ROOT/.env"
[ -n "${PGDATABASE:-}" ] || die "PGDATABASE is missing in $PROJECT_ROOT/.env"

export NOMINATIM_DATABASE_DSN="pgsql:dbname=$PGDATABASE;host=$PGHOST;port=$PGPORT;user=$PGUSER;sslmode=${PGSSLMODE:-require}"
log "✓ Using NOMINATIM_DATABASE_DSN for remote PostgreSQL host"

PGPASS_TEMP="$(umask 177 && mktemp)"
export PGPASSFILE="$PGPASS_TEMP"
write_pgpass "$PGPASSWORD"
unset PGPASSWORD
log "✓ Using temporary pgpass credentials: $PGPASS_TEMP"

start_token_refresher

cd "$PROJECT_DIR"
log "Resuming import indexing with $THREADS threads..."
nominatim import --continue indexing --threads "$THREADS"
log "✓ Resume indexing completed"
