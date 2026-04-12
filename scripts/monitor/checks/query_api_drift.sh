# checks/query_api_drift.sh — detect query-api's CachingCatalog regression.
# Writer (spark-unified) count vs reader (query-api) count. Sustained drift
# means the TTL fix broke or the catalog pinned a stale snapshot.
# Per self-heal policy: restart query-api immediately on first breach.

check_query_api_drift() {
    section "QUERY-API DRIFT"

    if $HEALED_THIS_CYCLE; then
        result "drift" SKIP "service restarted this cycle"
        return
    fi

    local writer reader delta
    writer=$(spark_sql_scalar "SELECT COUNT(*) FROM atmosphere.raw.raw_events")
    reader=$(query_api_sql_scalar "SELECT COUNT(*) FROM atmosphere.raw.raw_events")

    if [[ "$writer" == "-1" || "$reader" == "-1" ]]; then
        result "drift" WARN "writer=$writer reader=$reader (one side failed)"
        metric_emit drift delta_rows "" warn "" ""
        return
    fi

    # Writer is always >= reader (firehose streams into writer; reader lags
    # by at most the micro-batch commit interval). Negative delta can happen
    # briefly if reader crossed a commit between the two queries; clamp to 0.
    delta=$(( writer - reader ))
    (( delta < 0 )) && delta=0

    metric_emit drift writer_rows "$writer" ok spark-unified ""
    metric_emit drift reader_rows "$reader" ok query-api ""
    metric_emit drift delta_rows "$delta" \
        "$( (( delta > DRIFT_ROW_THRESHOLD )) && echo breach || echo ok )" query-api ""

    if (( delta > DRIFT_ROW_THRESHOLD )); then
        result "writer vs reader" FAIL "${delta} row drift (writer=${writer}, reader=${reader}, SLA: <${DRIFT_ROW_THRESHOLD})"
        log_breach "query_api_drift" "${delta} rows" "<${DRIFT_ROW_THRESHOLD}"
        heal_request query-api "reader lags writer by ${delta} rows (CachingCatalog regression)"
    else
        result "writer vs reader" PASS "${delta} row drift (writer=${writer}, reader=${reader})"
    fi
}
