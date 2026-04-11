#!/usr/bin/env bash
# monitor.sh — Atmosphere pipeline health monitor with self-healing
#
# Checks container health, layer freshness, GPU status, and data integrity.
# Restarts only the specific failing component (upstream first).
# Skips data validation if any service was restarted this cycle.
#
# Usage:
#   ./scripts/monitor.sh              # Single check
#   ./scripts/monitor.sh --loop       # Run every 30s
#   ./scripts/monitor.sh --dry-run    # Check only, no restarts
#   ./scripts/monitor.sh --loop --dry-run

set -uo pipefail

# --- Configuration ---
LOOP=false
DRY_RUN=false
INTERVAL=30
LOG_FILE="logs/monitor.log"
RESTART_WINDOW=300  # 5 minutes
MAX_RESTARTS=3
SPARK_SQL_CONTAINER="spark-core"

# Freshness SLA thresholds (seconds)
INGEST_LAG_MAX=15
STAGING_LAG_MAX=20
CORE_LAG_MAX=25
SENTIMENT_LAG_MAX=60

# Data validation thresholds
SENTIMENT_COVERAGE_MIN=95
STAGING_COVERAGE_MIN=85

# Expected traffic ratios (Bluesky patterns)
POSTS_RAW_MIN=10
POSTS_RAW_MAX=30
LIKES_RAW_MIN=35
LIKES_RAW_MAX=65

# Services in dependency order (upstream first)
SERVICES=(spark-ingest spark-staging spark-core spark-sentiment)

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --loop) LOOP=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --interval) INTERVAL=$2; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# --- State tracking ---
declare -A RESTART_COUNTS
declare -A RESTART_TIMESTAMPS
HEALED_THIS_CYCLE=false
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

# --- Helpers ---
timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(timestamp)] $1"
    echo "$msg"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
}

result() {
    local name=$1 status=$2 detail=$3
    case $status in
        PASS) printf "  %-28s %s\n" "$name" "[PASS] $detail"; ((PASS_COUNT++)) ;;
        FAIL) printf "  %-28s %s\n" "$name" "[FAIL] $detail"; ((FAIL_COUNT++)) ;;
        WARN) printf "  %-28s %s\n" "$name" "[WARN] $detail"; ((WARN_COUNT++)) ;;
        SKIP) printf "  %-28s %s\n" "$name" "[SKIP] $detail" ;;
    esac
}

spark_sql() {
    docker exec "$SPARK_SQL_CONTAINER" /opt/spark/bin/spark-sql \
        --conf spark.sql.defaultCatalog=atmosphere \
        -e "$1" 2>/dev/null | grep -v "^$" | tail -n +1
}

should_restart() {
    local service=$1
    local now
    now=$(date +%s)
    local count=${RESTART_COUNTS[$service]:-0}
    local last_ts=${RESTART_TIMESTAMPS[$service]:-0}

    # Reset counter if outside window
    if (( now - last_ts > RESTART_WINDOW )); then
        RESTART_COUNTS[$service]=0
        count=0
    fi

    (( count < MAX_RESTARTS ))
}

restart_service() {
    local service=$1 reason=$2

    if ! should_restart "$service"; then
        log "ESCALATE: $service has restarted ${MAX_RESTARTS}+ times in ${RESTART_WINDOW}s — manual intervention needed"
        result "$service" FAIL "restart loop detected"
        return 1
    fi

    if $DRY_RUN; then
        log "DRY-RUN: would restart $service ($reason)"
        result "$service" WARN "needs restart: $reason (dry-run)"
    else
        log "HEAL: restarting $service ($reason)"
        docker compose restart "$service" >> "$LOG_FILE" 2>&1
        RESTART_COUNTS[$service]=$(( ${RESTART_COUNTS[$service]:-0} + 1 ))
        RESTART_TIMESTAMPS[$service]=$(date +%s)
        result "$service" WARN "restarted: $reason"
    fi
    HEALED_THIS_CYCLE=true
}

# --- Check functions ---

check_containers() {
    echo "CONTAINERS"
    local running=0 total=0 exited_services=()

    for service in "${SERVICES[@]}"; do
        ((total++))
        local state
        state=$(docker compose ps --format '{{.State}}' "$service" 2>/dev/null || echo "missing")

        if [[ "$state" == "running" ]]; then
            ((running++))
        else
            exited_services+=("$service")
        fi
    done

    if (( ${#exited_services[@]} == 0 )); then
        result "services" PASS "$running/$total running"
    else
        for svc in "${exited_services[@]}"; do
            restart_service "$svc" "container not running (state: $(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null))"
        done
    fi

    # Check for restart loops (high restart count)
    for service in "${SERVICES[@]}"; do
        local restarts
        restarts=$(docker inspect --format='{{.RestartCount}}' "$service" 2>/dev/null || echo "0")
        if (( restarts > 5 )); then
            result "$service restarts" WARN "$restarts total restarts"
        fi
    done
    echo ""
}

check_freshness() {
    echo "FRESHNESS"

    if $HEALED_THIS_CYCLE; then
        result "all layers" SKIP "service restarted this cycle"
        echo ""
        return
    fi

    local -A tables=(
        [raw_events]="atmosphere.raw.raw_events"
        [stg_posts]="atmosphere.staging.stg_posts"
        [core_posts]="atmosphere.core.core_posts"
        [core_post_sentiment]="atmosphere.core.core_post_sentiment"
    )
    local -A thresholds=(
        [raw_events]=$INGEST_LAG_MAX
        [stg_posts]=$STAGING_LAG_MAX
        [core_posts]=$CORE_LAG_MAX
        [core_post_sentiment]=$SENTIMENT_LAG_MAX
    )
    local -A service_map=(
        [raw_events]=spark-ingest
        [stg_posts]=spark-staging
        [core_posts]=spark-core
        [core_post_sentiment]=spark-sentiment
    )

    local upstream_stale=false

    for layer in raw_events stg_posts core_posts core_post_sentiment; do
        if $upstream_stale; then
            result "$layer" SKIP "upstream stale"
            continue
        fi

        local table=${tables[$layer]}
        local threshold=${thresholds[$layer]}
        local service=${service_map[$layer]}

        local lag_seconds
        lag_seconds=$(spark_sql "SELECT CAST(TIMESTAMPDIFF(SECOND, MAX(event_time), current_timestamp()) AS INT) FROM $table" 2>/dev/null | tr -d '[:space:]' || echo "-1")

        # Handle ingested_at for raw_events
        if [[ "$layer" == "raw_events" ]]; then
            lag_seconds=$(spark_sql "SELECT CAST(TIMESTAMPDIFF(SECOND, MAX(ingested_at), current_timestamp()) AS INT) FROM $table" 2>/dev/null | tr -d '[:space:]' || echo "-1")
        fi

        if [[ "$lag_seconds" == "-1" || "$lag_seconds" == "NULL" || -z "$lag_seconds" ]]; then
            result "$layer" WARN "no data yet"
            upstream_stale=true
            continue
        fi

        if (( lag_seconds > threshold )); then
            result "$layer" FAIL "${lag_seconds}s (SLA: <${threshold}s)"
            restart_service "$service" "lag ${lag_seconds}s exceeds ${threshold}s SLA"
            upstream_stale=true
        else
            result "$layer" PASS "${lag_seconds}s"
        fi
    done
    echo ""
}

check_gpu() {
    echo "GPU"

    local gpu_info
    gpu_info=$(docker exec spark-sentiment nvidia-smi --query-gpu=name,memory.used,memory.total --format=csv,noheader,nounits 2>/dev/null || echo "")

    if [[ -z "$gpu_info" ]]; then
        result "gpu" WARN "not detected — running in CPU mode"
    else
        local gpu_name mem_used mem_total
        gpu_name=$(echo "$gpu_info" | cut -d',' -f1 | xargs)
        mem_used=$(echo "$gpu_info" | cut -d',' -f2 | xargs)
        mem_total=$(echo "$gpu_info" | cut -d',' -f3 | xargs)
        local pct=$(( mem_used * 100 / mem_total ))

        if (( pct > 90 )); then
            result "gpu" WARN "$gpu_name ${mem_used}/${mem_total} MiB (${pct}% — high)"
        else
            result "gpu" PASS "$gpu_name ${mem_used}/${mem_total} MiB (${pct}%)"
        fi
    fi
    echo ""
}

check_data() {
    echo "TABLE COUNTS"

    if $HEALED_THIS_CYCLE; then
        result "all tables" SKIP "service restarted this cycle"
        echo ""
        return
    fi

    # Get counts
    local raw stg_posts stg_likes stg_reposts stg_follows stg_blocks stg_profiles
    local core_posts core_mentions core_hashtags core_engagement core_sentiment

    raw=$(spark_sql "SELECT COUNT(*) FROM atmosphere.raw.raw_events" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_posts=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_posts" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_likes=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_likes" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_reposts=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_reposts" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_follows=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_follows" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_blocks=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_blocks" 2>/dev/null | tr -d '[:space:]' || echo "0")
    stg_profiles=$(spark_sql "SELECT COUNT(*) FROM atmosphere.staging.stg_profiles" 2>/dev/null | tr -d '[:space:]' || echo "0")
    core_posts=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_posts" 2>/dev/null | tr -d '[:space:]' || echo "0")
    core_mentions=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_mentions" 2>/dev/null | tr -d '[:space:]' || echo "0")
    core_hashtags=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_hashtags" 2>/dev/null | tr -d '[:space:]' || echo "0")
    core_engagement=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_engagement" 2>/dev/null | tr -d '[:space:]' || echo "0")
    core_sentiment=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment" 2>/dev/null | tr -d '[:space:]' || echo "0")

    local stg_total=$(( stg_posts + stg_likes + stg_reposts + stg_follows + stg_blocks + stg_profiles ))

    printf "  %-28s %'d\n" "raw_events" "$raw"
    printf "  %-28s %'d\n" "stg_posts" "$stg_posts"
    printf "  %-28s %'d\n" "stg_likes" "$stg_likes"
    printf "  %-28s %'d\n" "stg_reposts" "$stg_reposts"
    printf "  %-28s %'d\n" "stg_follows" "$stg_follows"
    printf "  %-28s %'d\n" "stg_blocks" "$stg_blocks"
    printf "  %-28s %'d\n" "stg_profiles" "$stg_profiles"
    printf "  %-28s %'d\n" "core_posts" "$core_posts"
    printf "  %-28s %'d\n" "core_mentions" "$core_mentions"
    printf "  %-28s %'d\n" "core_hashtags" "$core_hashtags"
    printf "  %-28s %'d\n" "core_engagement" "$core_engagement"
    printf "  %-28s %'d\n" "core_post_sentiment" "$core_sentiment"
    echo ""

    # --- Ratios ---
    echo "RATIOS"

    if (( raw > 0 )); then
        local stg_pct=$(( stg_total * 100 / raw ))
        if (( stg_pct >= STAGING_COVERAGE_MIN )); then
            result "staging/raw" PASS "${stg_pct}% (SLA: >${STAGING_COVERAGE_MIN}%)"
        else
            result "staging/raw" FAIL "${stg_pct}% (SLA: >${STAGING_COVERAGE_MIN}%)"
        fi

        local posts_pct=$(( stg_posts * 100 / raw ))
        if (( posts_pct >= POSTS_RAW_MIN && posts_pct <= POSTS_RAW_MAX )); then
            result "posts/raw" PASS "${posts_pct}% (expected: ${POSTS_RAW_MIN}-${POSTS_RAW_MAX}%)"
        else
            result "posts/raw" WARN "${posts_pct}% (expected: ${POSTS_RAW_MIN}-${POSTS_RAW_MAX}%)"
        fi

        local likes_pct=$(( stg_likes * 100 / raw ))
        if (( likes_pct >= LIKES_RAW_MIN && likes_pct <= LIKES_RAW_MAX )); then
            result "likes/raw" PASS "${likes_pct}% (expected: ${LIKES_RAW_MIN}-${LIKES_RAW_MAX}%)"
        else
            result "likes/raw" WARN "${likes_pct}% (expected: ${LIKES_RAW_MIN}-${LIKES_RAW_MAX}%)"
        fi
    else
        result "ratios" SKIP "no raw data"
    fi

    if (( core_posts > 0 )); then
        local sent_pct=$(( core_sentiment * 100 / core_posts ))
        if (( sent_pct >= SENTIMENT_COVERAGE_MIN )); then
            result "sentiment coverage" PASS "${sent_pct}% (SLA: >${SENTIMENT_COVERAGE_MIN}%)"
        else
            result "sentiment coverage" FAIL "${sent_pct}% (SLA: >${SENTIMENT_COVERAGE_MIN}%)"
        fi
    else
        result "sentiment coverage" SKIP "no core_posts data"
    fi
    echo ""

    # --- Sentiment distribution ---
    echo "SENTIMENT DISTRIBUTION"

    if (( core_sentiment > 0 )); then
        local dist
        dist=$(spark_sql "SELECT sentiment_label, COUNT(*) as cnt, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct FROM atmosphere.core.core_post_sentiment GROUP BY sentiment_label ORDER BY cnt DESC" 2>/dev/null || echo "")

        if [[ -n "$dist" ]]; then
            echo "$dist" | while IFS=$'\t' read -r label cnt pct; do
                printf "  %-28s %'d (%s%%)\n" "$label" "$cnt" "$pct"
            done
        fi

        # Score validity
        local invalid
        invalid=$(spark_sql "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE ABS(sentiment_positive + sentiment_negative + sentiment_neutral - 1.0) > 0.01" 2>/dev/null | tr -d '[:space:]' || echo "-1")

        if [[ "$invalid" == "0" ]]; then
            result "score validity" PASS "all scores sum to 1.0"
        elif [[ "$invalid" == "-1" ]]; then
            result "score validity" SKIP "query failed"
        else
            result "score validity" FAIL "$invalid rows with invalid scores"
        fi
    else
        result "sentiment" SKIP "no sentiment data"
    fi
    echo ""
}

# --- Main ---

run_check() {
    HEALED_THIS_CYCLE=false
    PASS_COUNT=0
    FAIL_COUNT=0
    WARN_COUNT=0

    echo ""
    echo "[$(timestamp)] ATMOSPHERE PIPELINE HEALTH"
    echo "================================================="
    echo ""

    check_containers
    check_freshness
    check_gpu
    check_data

    # --- Summary ---
    echo "================================================="
    if (( FAIL_COUNT > 0 )); then
        echo "OVERALL: FAIL ($FAIL_COUNT failures, $WARN_COUNT warnings, $PASS_COUNT passed)"
    elif (( WARN_COUNT > 0 )); then
        echo "OVERALL: WARN ($WARN_COUNT warnings, $PASS_COUNT passed)"
    else
        echo "OVERALL: PASS ($PASS_COUNT checks passed)"
    fi
    echo ""

    log "CHECK: pass=$PASS_COUNT warn=$WARN_COUNT fail=$FAIL_COUNT healed=$HEALED_THIS_CYCLE"
}

if $LOOP; then
    log "Starting monitor loop (interval=${INTERVAL}s, dry-run=$DRY_RUN)"
    while true; do
        run_check
        sleep "$INTERVAL"
    done
else
    run_check
fi
