#!/usr/bin/env bash
# replay.sh — Replay Jetstream firehose from a specific timestamp
#
# Resets the ingest checkpoint and sets the Jetstream cursor to replay
# events from a given point in time. Jetstream retains ~72 hours of data.
#
# This restarts the full pipeline (ingest → staging → core → sentiment)
# since all downstream checkpoints must be cleared to avoid duplicate
# processing of replayed data.
#
# Usage:
#   ./scripts/replay.sh 2026-04-11T10:00:00Z    # Replay from ISO timestamp
#   ./scripts/replay.sh -1h                       # Replay last 1 hour
#   ./scripts/replay.sh -30m                      # Replay last 30 minutes
#   ./scripts/replay.sh --dry-run -2h             # Preview what would happen

set -uo pipefail

# --- Configuration ---
DRY_RUN=false
LOG_FILE="logs/replay.log"
CURSOR_ENV_VAR="JETSTREAM_CURSOR"
SERVICE="spark-unified"
VOLUME="atmosphere_spark-checkpoints"

# --- Parse args ---
TIMESTAMP=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
        -*[0-9]*[hm]) TIMESTAMP="$1"; shift ;;
        *) TIMESTAMP="$1"; shift ;;
    esac
done

if [[ -z "$TIMESTAMP" ]]; then
    echo "Usage: $0 [--dry-run] <timestamp>"
    echo ""
    echo "Timestamp formats:"
    echo "  ISO 8601:  2026-04-11T10:00:00Z"
    echo "  Relative:  -1h  (1 hour ago)"
    echo "             -30m (30 minutes ago)"
    echo "             -2h  (2 hours ago)"
    echo ""
    echo "Note: Jetstream retains ~72 hours of data."
    exit 1
fi

# --- Helpers ---
timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(timestamp)] $1"
    echo "$msg"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
}

# Convert relative time (-1h, -30m) to microseconds since epoch
resolve_cursor() {
    local ts=$1
    local epoch_us

    if [[ "$ts" =~ ^-([0-9]+)h$ ]]; then
        local hours=${BASH_REMATCH[1]}
        local epoch_s=$(( $(date +%s) - hours * 3600 ))
        epoch_us=$(( epoch_s * 1000000 ))
    elif [[ "$ts" =~ ^-([0-9]+)m$ ]]; then
        local mins=${BASH_REMATCH[1]}
        local epoch_s=$(( $(date +%s) - mins * 60 ))
        epoch_us=$(( epoch_s * 1000000 ))
    else
        # ISO 8601 timestamp
        local epoch_s
        epoch_s=$(date -d "$ts" +%s 2>/dev/null)
        if [[ $? -ne 0 || -z "$epoch_s" ]]; then
            echo "ERROR: Cannot parse timestamp: $ts"
            exit 1
        fi
        epoch_us=$(( epoch_s * 1000000 ))
    fi

    echo "$epoch_us"
}

# --- Resolve cursor ---
CURSOR=$(resolve_cursor "$TIMESTAMP")
HUMAN_TIME=$(date -d "@$(( CURSOR / 1000000 ))" '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || echo "$TIMESTAMP")

echo ""
echo "=== Atmosphere Replay ==="
echo "  Replay from: $HUMAN_TIME"
echo "  Cursor:      $CURSOR"
echo ""

# Check how far back this is
NOW_US=$(( $(date +%s) * 1000000 ))
AGO_HOURS=$(( (NOW_US - CURSOR) / 1000000 / 3600 ))
if (( AGO_HOURS > 72 )); then
    echo "WARNING: Requested replay is ${AGO_HOURS}h ago. Jetstream only retains ~72h."
    echo "         Events before the retention window will not be available."
    echo ""
fi

if $DRY_RUN; then
    echo "[DRY-RUN] Would:"
    echo "  1. Stop $SERVICE"
    echo "  2. Clear all checkpoint subdirectories"
    echo "  3. Set JETSTREAM_CURSOR=$CURSOR"
    echo "  4. Restart $SERVICE"
    exit 0
fi

log "Starting replay from $HUMAN_TIME (cursor=$CURSOR)"

# --- Stop the unified service ---
echo "=== Stopping $SERVICE ==="
echo "  Stopping $SERVICE..."
docker compose stop "$SERVICE" 2>/dev/null
echo ""

# --- Clear all checkpoints ---
echo "=== Clearing all checkpoints ==="
echo "  Clearing $VOLUME..."
docker run --rm -v "${VOLUME}:/cp" alpine sh -c 'rm -rf /cp/*' 2>/dev/null
echo ""

# --- Restart with cursor ---
echo "=== Setting replay cursor ==="
echo "  $CURSOR_ENV_VAR=$CURSOR"
echo ""

echo "=== Restarting $SERVICE ==="
echo "  Starting $SERVICE..."
JETSTREAM_CURSOR="$CURSOR" docker compose up -d "$SERVICE" 2>/dev/null
echo ""

log "Replay started from $HUMAN_TIME"
echo "=== Replay in progress ==="
echo "Monitor with: make monitor"
