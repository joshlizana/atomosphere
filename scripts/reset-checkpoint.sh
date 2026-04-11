#!/usr/bin/env bash
# reset-checkpoint.sh — Clear streaming checkpoint for a specific layer
#
# Stops spark-unified, removes checkpoint data for the specified layer(s),
# and restarts the service. All layers share one container and one volume
# with subdirectories per layer.
#
# Usage:
#   ./scripts/reset-checkpoint.sh ingest
#   ./scripts/reset-checkpoint.sh staging core
#   ./scripts/reset-checkpoint.sh --all              # Reset all layers
#   ./scripts/reset-checkpoint.sh --dry-run sentiment

set -uo pipefail

# --- Configuration ---
DRY_RUN=false
RESET_ALL=false
LOG_FILE="logs/reset-checkpoint.log"
SERVICE="spark-unified"
VOLUME="atmosphere_spark-checkpoints"

# Layer name → checkpoint subdirectory
declare -A CHECKPOINT_SUBDIRS=(
    [ingest]="ingest-raw"
    [staging]="staging"
    [core]="core"
    [sentiment]="sentiment"
)

VALID_LAYERS=(ingest staging core sentiment)

# --- Parse args ---
TARGETS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
        --all) RESET_ALL=true; shift ;;
        ingest|staging|core|sentiment) TARGETS+=("$1"); shift ;;
        *) echo "Unknown option or layer: $1"; echo "Valid layers: ${VALID_LAYERS[*]}"; exit 1 ;;
    esac
done

if $RESET_ALL; then
    TARGETS=("${VALID_LAYERS[@]}")
fi

if (( ${#TARGETS[@]} == 0 )); then
    echo "Usage: $0 [--dry-run] [--all] <layer> [layer...]"
    echo "Layers: ${VALID_LAYERS[*]}"
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

# --- Reset ---
log "Checkpoint reset requested for: ${TARGETS[*]} (dry-run=$DRY_RUN)"
echo ""

# Stop the unified service
echo "=== Stopping $SERVICE ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] Would stop $SERVICE"
else
    echo "  Stopping $SERVICE..."
    docker compose stop "$SERVICE" 2>/dev/null
fi
echo ""

# Clear checkpoint subdirectories for targets
echo "=== Clearing checkpoints ==="
for target in "${TARGETS[@]}"; do
    subdir=${CHECKPOINT_SUBDIRS[$target]}

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would clear $VOLUME:/$subdir"
    else
        echo "  Clearing $subdir..."
        docker run --rm -v "${VOLUME}:/cp" alpine sh -c "rm -rf /cp/${subdir}/*" 2>/dev/null
        log "Cleared checkpoint subdirectory $subdir for layer $target"
    fi
done
echo ""

# Restart the unified service
echo "=== Restarting $SERVICE ==="
if $DRY_RUN; then
    echo "  [DRY-RUN] Would start $SERVICE"
else
    echo "  Starting $SERVICE..."
    docker compose start "$SERVICE" 2>/dev/null
fi
echo ""

log "Checkpoint reset complete"
