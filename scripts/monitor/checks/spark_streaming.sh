# checks/spark_streaming.sh — detect stuck streaming queries via Spark UI API.
# A query is "stuck" if its latest batchId hasn't changed in STUCK_QUERY_BATCH_AGE_MAX
# seconds. Restart spark-unified on stuck detection (writes a heal request —
# orchestrator dedupes if freshness check already queued one).

# Per-query batchId cache — tracks the last seen batch across cycles.
declare -gA STREAM_LAST_BATCH
declare -gA STREAM_LAST_SEEN_TS

check_spark_streaming() {
    section "SPARK STREAMING"

    if $HEALED_THIS_CYCLE; then
        result "streaming" SKIP "service restarted this cycle"
        return
    fi

    local state
    state=$(container_state spark-unified)
    [[ "$state" != "running" ]] && { result "streaming" SKIP "spark-unified not running"; return; }

    # Fetch active streaming queries from Spark UI (runs inside the container
    # since :4040 isn't necessarily bound to host when streaming is disabled).
    local json
    json=$(docker exec spark-unified curl -sf --max-time 5 http://localhost:4040/api/v1/applications 2>/dev/null) || {
        result "streaming" WARN "Spark UI unreachable"
        metric_emit spark_streaming reachable 0 warn spark-unified ""
        return
    }

    local app_id
    app_id=$(echo "$json" | python3 -c "
import sys, json
try:
    apps = json.loads(sys.stdin.read())
    print(apps[0]['id'] if apps else '')
except Exception:
    print('')" 2>/dev/null)
    [[ -z "$app_id" ]] && { result "streaming" WARN "no Spark application"; return; }

    local queries
    queries=$(docker exec spark-unified curl -sf --max-time 5 \
        "http://localhost:4040/api/v1/applications/${app_id}/streaming/statistics" 2>/dev/null \
        || docker exec spark-unified curl -sf --max-time 5 \
        "http://localhost:4040/api/v1/applications/${app_id}/sql" 2>/dev/null)

    # Structured streaming exposes per-query state at /sql endpoint with running
    # batches. Parse and extract each query's latest batchId + name.
    local parsed
    parsed=$(echo "$queries" | python3 -c "
import sys, json
try:
    items = json.loads(sys.stdin.read())
    # Each item has description + id. We track last batch id by description.
    for it in items:
        desc = it.get('description', '')[:60]
        bid  = it.get('id', -1)
        print(f'{desc}\t{bid}')
except Exception:
    pass
" 2>/dev/null)

    if [[ -z "$parsed" ]]; then
        result "streaming" WARN "no active queries parsed"
        return
    fi

    local now=$(date +%s)
    local desc bid last age active_count=0 stuck_count=0
    while IFS=$'\t' read -r desc bid; do
        [[ -z "$desc" ]] && continue
        ((active_count++))
        last=${STREAM_LAST_BATCH[$desc]:-}
        if [[ "$last" != "$bid" ]]; then
            STREAM_LAST_BATCH[$desc]=$bid
            STREAM_LAST_SEEN_TS[$desc]=$now
            age=0
        else
            age=$(( now - ${STREAM_LAST_SEEN_TS[$desc]:-$now} ))
        fi

        metric_emit spark_streaming batch_age_seconds "$age" \
            "$( (( age > STUCK_QUERY_BATCH_AGE_MAX )) && echo breach || echo ok )" spark-unified "$desc"

        if (( age > STUCK_QUERY_BATCH_AGE_MAX )); then
            ((stuck_count++))
            result "$desc" FAIL "stuck at batch $bid for ${age}s"
            log_breach "stuck_query" "${desc}@${age}s" "<${STUCK_QUERY_BATCH_AGE_MAX}s"
        fi
    done <<< "$parsed"

    metric_emit spark_streaming active_queries "$active_count" ok spark-unified ""
    metric_emit spark_streaming stuck_queries  "$stuck_count" \
        "$( (( stuck_count > 0 )) && echo breach || echo ok )" spark-unified ""

    if (( stuck_count > 0 )); then
        heal_request spark-unified "${stuck_count} stuck streaming quer$( (( stuck_count == 1 )) && echo y || echo ies)"
    else
        result "queries" PASS "${active_count} active, none stuck"
    fi
}
