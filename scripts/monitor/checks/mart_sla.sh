# checks/mart_sla.sh — per-mart read latency via query-api. NFR-04: <5s each.
# On breach: log + POST /api/maintenance/run (compaction) per self-heal policy.
# SLOW TIER — runs every 10th cycle (~5 min) not every cycle.

MART_NAMES=(
    events_per_second
    engagement_velocity
    sentiment_timeseries
    trending_hashtags
    most_mentioned
    language_distribution
    content_breakdown
    embed_usage
    top_posts
    firehose_stats
)

check_mart_sla() {
    section "MART SLA (read latency)"

    local mart json elapsed status any_breach=false
    for mart in "${MART_NAMES[@]}"; do
        json=$(curl -sf --max-time 15 "${QUERY_API_URL}/api/mart/${mart}?limit=10" 2>/dev/null) || {
            result "$mart" WARN "query failed"
            metric_emit mart_sla query_time_seconds "" warn query-api "$mart"
            continue
        }
        elapsed=$(echo "$json" | python3 -c "
import sys, json
try:
    print(json.loads(sys.stdin.read())['query_time_seconds'])
except Exception:
    print('-1')" 2>/dev/null || echo "-1")

        if [[ "$elapsed" == "-1" ]]; then
            result "$mart" WARN "parse failed"
            continue
        fi

        # Bash integer compare: use awk for float threshold
        local over
        over=$(awk -v e="$elapsed" -v t="$MART_SLA_SECONDS" 'BEGIN { print (e > t) ? 1 : 0 }')
        status=$([[ "$over" == "1" ]] && echo breach || echo ok)
        metric_emit mart_sla query_time_seconds "$elapsed" "$status" query-api "$mart"

        if [[ "$over" == "1" ]]; then
            result "$mart" FAIL "${elapsed}s (SLA: <${MART_SLA_SECONDS}s)"
            log_breach "mart_sla_${mart}" "${elapsed}s" "<${MART_SLA_SECONDS}s"
            any_breach=true
        else
            result "$mart" PASS "${elapsed}s"
        fi
    done

    if $any_breach && ! $DRY_RUN; then
        log "HEAL: mart SLA breach — triggering /api/maintenance/run"
        curl -sf --max-time 10 -X POST "${QUERY_API_URL}/api/maintenance/run" \
            -H 'Content-Type: application/json' -d '{}' > /dev/null \
            && log_alert "mart SLA breach triggered compaction" \
            || log "HEAL: maintenance/run POST failed"
    fi
}
