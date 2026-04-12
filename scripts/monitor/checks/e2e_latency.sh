# checks/e2e_latency.sh — time from the latest sentiment row's linked post
# event_time to now(). Breaches at E2E_LATENCY_MAX.

check_e2e_latency() {
    section "END-TO-END LATENCY"

    if $HEALED_THIS_CYCLE; then
        result "e2e latency" SKIP "service restarted this cycle"
        return
    fi

    local latency
    latency=$(spark_sql_scalar "
        SELECT CAST(TIMESTAMPDIFF(SECOND, MIN(p.event_time), current_timestamp()) AS INT)
        FROM atmosphere.core.core_post_sentiment s
        INNER JOIN atmosphere.core.core_posts p
            ON p.did = s.did AND p.time_us = s.time_us
        WHERE s.event_time = (SELECT MAX(event_time) FROM atmosphere.core.core_post_sentiment)
    ")

    if [[ "$latency" == "-1" ]]; then
        result "e2e latency" WARN "no data yet"
        metric_emit e2e_latency seconds "" warn "" ""
        return
    fi

    metric_emit e2e_latency seconds "$latency" "$( (( latency > E2E_LATENCY_MAX )) && echo breach || echo ok )" "" ""

    if (( latency > E2E_LATENCY_MAX )); then
        result "e2e latency" FAIL "${latency}s (SLA: <${E2E_LATENCY_MAX}s)"
        log_breach "e2e_latency" "${latency}s" "<${E2E_LATENCY_MAX}s"
    else
        result "e2e latency" PASS "${latency}s"
    fi
}
