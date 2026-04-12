# monitor/lib/metrics.sh — SQLite writer + periodic flush to Iceberg.
#
# Checks call `metric_emit` after each measurement. Rows land in SQLite
# immediately (availability) and are batch-pushed to atmosphere.ops.monitor_metrics
# via query-api every ~5 minutes (durability + query-ability).

_sql_escape() {
    # Escape a value for safe inclusion in a single-quoted SQL literal.
    local v=$1
    v=${v//\'/\'\'}
    printf "%s" "$v"
}

metric_emit() {
    # metric_emit <check_name> <metric_key> <value_num> <status> [service] [value_str]
    # value_num: pass "" if not numeric
    # status: ok | warn | breach
    local check_name=$1 metric_key=$2 value_num=$3 status=$4
    local service=${5:-}
    local value_str=${6:-}

    local ts=$CYCLE_ID
    local num_sql="NULL"
    [[ -n "$value_num" ]] && num_sql="$value_num"

    local svc_sql="NULL"
    [[ -n "$service" ]] && svc_sql="'$(_sql_escape "$service")'"

    local str_sql="NULL"
    [[ -n "$value_str" ]] && str_sql="'$(_sql_escape "$value_str")'"

    sqlite3 "$METRICS_DB" "INSERT INTO metrics
        (ts, cycle_id, check_name, metric_key, value_num, value_str, status, service)
        VALUES ($ts, $CYCLE_ID, '$(_sql_escape "$check_name")', '$(_sql_escape "$metric_key")',
                $num_sql, $str_sql, '$(_sql_escape "$status")', $svc_sql);" 2>/dev/null || true

    local display="${value_num}"
    [[ -z "$display" ]] && display="${value_str}"
    [[ -z "$display" ]] && display="-"
    local svc_tag=""
    [[ -n "$service" ]] && svc_tag=" [$service]"
    local str_tag=""
    [[ -n "$value_str" && -n "$value_num" ]] && str_tag=" ($value_str)"
    trace "metric ${check_name}.${metric_key}=${display}${str_tag} ${status}${svc_tag}"
}

metrics_flush_to_iceberg() {
    # Read all rows where flushed=0, POST to /api/ops/metrics, mark flushed on success.
    local pending
    pending=$(sqlite3 "$METRICS_DB" "SELECT COUNT(*) FROM metrics WHERE flushed=0;" 2>/dev/null || echo "0")
    [[ "$pending" == "0" ]] && { log_quiet "metrics flush: nothing pending"; return 0; }

    log "metrics flush: pushing $pending rows to Iceberg"

    # Emit JSONL to a temp file. Each line: one metric row.
    local payload
    payload=$(mktemp)
    sqlite3 -separator $'\x1f' "$METRICS_DB" \
        "SELECT ts, cycle_id, check_name, metric_key,
                COALESCE(value_num, ''), COALESCE(value_str, ''),
                status, COALESCE(service, '')
           FROM metrics WHERE flushed=0 ORDER BY ts;" 2>/dev/null \
    | python3 -c "
import sys, json
for line in sys.stdin:
    parts = line.rstrip('\n').split('\x1f')
    if len(parts) != 8:
        continue
    ts, cycle_id, check_name, metric_key, value_num, value_str, status, service = parts
    row = {
        'ts': int(ts),
        'cycle_id': int(cycle_id),
        'check_name': check_name,
        'metric_key': metric_key,
        'value_num': float(value_num) if value_num != '' else None,
        'value_str': value_str if value_str != '' else None,
        'status': status,
        'service': service if service != '' else None,
    }
    print(json.dumps(row))
" > "$payload"

    local rc
    curl -sf --max-time 30 -X POST "${QUERY_API_URL}/api/ops/metrics" \
        -H 'Content-Type: application/x-ndjson' \
        --data-binary "@${payload}" > /dev/null
    rc=$?
    rm -f "$payload"

    if (( rc == 0 )); then
        sqlite3 "$METRICS_DB" "UPDATE metrics SET flushed=1 WHERE flushed=0;" 2>/dev/null || true
        log "metrics flush: $pending rows committed"
        return 0
    else
        log "metrics flush: POST failed (rc=$rc), rows remain pending"
        return 1
    fi
}
