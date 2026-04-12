#!/usr/bin/env bash
# monitor.sh — Atmosphere pipeline health monitor (orchestrator).
#
# One long-running process. Periodically runs checks (fast + slow tiers),
# writes metrics to SQLite, flushes batched metrics to Iceberg every 5 min,
# and self-heals via rate-limited restarts.
#
# Usage:
#   ./scripts/monitor.sh                    # Daemon, 30s fast-tier interval
#   ./scripts/monitor.sh --dry-run          # Don't actually restart anything
#   ./scripts/monitor.sh --once             # Single check cycle (no loop)
#   ./scripts/monitor.sh --interval 60      # Override fast-tier interval
#
# Architecture:
#   lib/common.sh     — shared helpers, config, runtime state
#   lib/state.sh      — SQLite schema init + pruning
#   lib/metrics.sh    — metric_emit (SQLite) + batch flush to Iceberg
#   lib/heal.sh       — rate-limited restarts, consecutive-breach tracking
#   checks/*.sh       — individual check modules (sourced, not subprocesses)
#
# Tiered cadence:
#   Fast tier  (every cycle, ~30s): containers, memory, freshness,
#                                    checkpoints, gpu, logs, throughput,
#                                    e2e_latency, duplicates, data_loss,
#                                    ratios, sentiment, query_api_drift,
#                                    grafana, spark_streaming
#   Slow tier  (every 10th cycle, ~5min): mart_sla, maintenance
#   Flush     (every 10th cycle, ~5min): SQLite → Iceberg

set -uo pipefail

INTERVAL=30
DRY_RUN=false
ONCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)  DRY_RUN=true; shift ;;
        --once)     ONCE=true; shift ;;
        --interval) INTERVAL=$2; shift 2 ;;
        -h|--help)
            sed -n '2,15p' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Resolve script dir + source libs ---------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITOR_DIR="${SCRIPT_DIR}/monitor"
export INTERVAL DRY_RUN

# shellcheck source=monitor/lib/common.sh
source "${MONITOR_DIR}/lib/common.sh"
# shellcheck source=monitor/lib/state.sh
source "${MONITOR_DIR}/lib/state.sh"
# shellcheck source=monitor/lib/metrics.sh
source "${MONITOR_DIR}/lib/metrics.sh"
# shellcheck source=monitor/lib/heal.sh
source "${MONITOR_DIR}/lib/heal.sh"

for f in "${MONITOR_DIR}"/checks/*.sh; do
    # shellcheck source=/dev/null
    source "$f"
done

# --- Runtime ----------------------------------------------------------------

run_cycle() {
    CYCLE_NUM=$((CYCLE_NUM + 1))
    CYCLE_ID=$(date +%s)
    HEALED_THIS_CYCLE=false
    HEAL_QUEUE=()
    PASS_COUNT=0
    WARN_COUNT=0
    FAIL_COUNT=0

    trace "cycle #${CYCLE_NUM} starting (cycle_id=${CYCLE_ID})"

    if stack_is_down; then
        trace "stack is down — nothing to check"
        log "Stack is down — nothing to check"
        return
    fi

    echo ""
    echo "[$(timestamp)] ATMOSPHERE MONITOR — cycle #${CYCLE_NUM}"
    echo "================================================="

    # --- Refresh query-api catalog caches (cycle start) --------------------
    # One POST invalidates the CachingCatalog for every table this cycle
    # will read, so subsequent /api/sql calls see fresh snapshots.
    query_api_refresh \
        atmosphere.raw.raw_events \
        atmosphere.staging.stg_posts \
        atmosphere.staging.stg_likes \
        atmosphere.staging.stg_reposts \
        atmosphere.staging.stg_follows \
        atmosphere.staging.stg_blocks \
        atmosphere.staging.stg_profiles \
        atmosphere.core.core_posts \
        atmosphere.core.core_mentions \
        atmosphere.core.core_hashtags \
        atmosphere.core.core_engagement \
        atmosphere.core.core_post_sentiment

    # --- Fast tier (every cycle) -------------------------------------------
    check_containers
    check_memory
    check_pipeline
    check_checkpoints
    check_gpu
    check_logs
    check_e2e_latency
    check_sentiment
    check_query_api_drift
    check_grafana
    check_spark_streaming

    # --- Slow tier (every 10th cycle, ~5 min) ------------------------------
    if (( CYCLE_NUM % 10 == 0 )); then
        check_mart_sla
        check_maintenance

        section "METRICS FLUSH"
        if metrics_flush_to_iceberg; then
            result "flush" PASS "rows committed to atmosphere.ops.monitor_metrics"
        else
            result "flush" WARN "flush failed — retrying next cycle"
        fi

        state_prune
    fi

    # --- Apply self-heal requests ------------------------------------------
    apply_heals

    # --- Summary -----------------------------------------------------------
    echo ""
    echo "================================================="
    if (( FAIL_COUNT > 0 )); then
        echo "OVERALL: FAIL ($FAIL_COUNT failures, $WARN_COUNT warnings, $PASS_COUNT passed)"
    elif (( WARN_COUNT > 0 )); then
        echo "OVERALL: WARN ($WARN_COUNT warnings, $PASS_COUNT passed)"
    else
        echo "OVERALL: PASS ($PASS_COUNT checks passed)"
    fi
    echo ""

    log "CYCLE $CYCLE_NUM: pass=$PASS_COUNT warn=$WARN_COUNT fail=$FAIL_COUNT healed=$HEALED_THIS_CYCLE"
}

# --- Bootstrap --------------------------------------------------------------

command -v sqlite3 >/dev/null 2>&1 || {
    echo "error: sqlite3 not found (apt-get install sqlite3)" >&2
    exit 1
}

state_init
log "monitor starting (interval=${INTERVAL}s, dry_run=${DRY_RUN}, once=${ONCE})"

if $ONCE; then
    run_cycle
    exit $((FAIL_COUNT > 0 ? 1 : 0))
fi

trap 'log "monitor shutting down"; exit 0' INT TERM

while true; do
    run_cycle
    sleep "$INTERVAL"
done
