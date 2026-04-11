#!/usr/bin/env bash
# reset-checkpoint.sh — Clear streaming checkpoint for a specific layer
#
# Stops the service, removes its checkpoint data, and restarts it so
# it reprocesses from the latest upstream data.
#
# Usage:
#   ./scripts/reset-checkpoint.sh spark-staging
#   ./scripts/reset-checkpoint.sh spark-sentiment
#   ./scripts/reset-checkpoint.sh --all              # Reset all layers
#   ./scripts/reset-checkpoint.sh --dry-run spark-core

set -uo pipefail

# --- Configuration ---
DRY_RUN=false
RESET_ALL=false
LOG_FILE="logs/reset-checkpoint.log"

declare -A CHECKPOINT_VOLUMES=(
    [spark-ingest]=spark-ingest-checkpoints
    [spark-staging]=spark-staging-checkpoints
    [spark-core]=spark-core-checkpoints
    [spark-sentiment]=spark-sentiment-checkpoints
)

# Dependency order — downstream services must restart after upstream
SERVICES_ORDERED=(spark-ingest spark-staging spark-core spark-sentiment)

# --- Parse args ---
TARGETS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
        --all) RESET_ALL=true; shift ;;
        spark-ingest|spark-staging|spark-core|spark-sentiment) TARGETS+=("$1"); shift ;;
        *) echo "Unknown option or service: $1"; echo "Valid services: ${SERVICES_ORDERED[*]}"; exit 1 ;;
    esac
done

if $RESET_ALL; then
    TARGETS=("${SERVICES_ORDERED[@]}")
fi

if (( ${#TARGETS[@]} == 0 )); then
    echo "Usage: $0 [--dry-run] [--all] <service> [service...]"
    echo "Services: ${SERVICES_ORDERED[*]}"
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

# --- Collect downstream services that need restart ---
get_downstream() {
    local target=$1
    local found=false
    local downstream=()
    for svc in "${SERVICES_ORDERED[@]}"; do
        if $found; then
            downstream+=("$svc")
        fi
        if [[ "$svc" == "$target" ]]; then
            found=true
        fi
    done
    echo "${downstream[*]}"
}

# --- Reset ---
log "Checkpoint reset requested for: ${TARGETS[*]} (dry-run=$DRY_RUN)"
echo ""

# Build full list of services to restart (targets + all downstream)
declare -A RESTART_SET
for target in "${TARGETS[@]}"; do
    RESTART_SET[$target]=1
    for ds in $(get_downstream "$target"); do
        RESTART_SET[$ds]=1
    done
done

# Stop affected services in reverse order
echo "=== Stopping services ==="
for (( i=${#SERVICES_ORDERED[@]}-1; i>=0; i-- )); do
    svc=${SERVICES_ORDERED[$i]}
    if [[ -n "${RESTART_SET[$svc]:-}" ]]; then
        if $DRY_RUN; then
            echo "  [DRY-RUN] Would stop $svc"
        else
            echo "  Stopping $svc..."
            docker compose stop "$svc" 2>/dev/null
        fi
    fi
done
echo ""

# Clear checkpoint volumes for targets only (not downstream)
echo "=== Clearing checkpoints ==="
for target in "${TARGETS[@]}"; do
    local_volume=${CHECKPOINT_VOLUMES[$target]}
    project_volume="atmosphere_${local_volume}"

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would clear volume $project_volume"
    else
        echo "  Clearing $project_volume..."
        docker run --rm -v "${project_volume}:/cp" alpine sh -c 'rm -rf /cp/*' 2>/dev/null
        log "Cleared checkpoint volume $project_volume for $target"
    fi
done
echo ""

# Restart affected services in dependency order
echo "=== Restarting services ==="
for svc in "${SERVICES_ORDERED[@]}"; do
    if [[ -n "${RESTART_SET[$svc]:-}" ]]; then
        if $DRY_RUN; then
            echo "  [DRY-RUN] Would start $svc"
        else
            echo "  Starting $svc..."
            docker compose start "$svc" 2>/dev/null
        fi
    fi
done
echo ""

log "Checkpoint reset complete"
