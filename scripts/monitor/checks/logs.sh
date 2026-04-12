# checks/logs.sh — grep recent container logs for error/exception/oom.
# Warns (does not breach) above 10 hits per service.

check_logs() {
    section "LOG ERRORS"

    local service errors last_error

    for service in "${SERVICES[@]}"; do
        errors=$(docker compose logs --tail="$LOG_TAIL_LINES" "$service" 2>/dev/null \
            | grep -ciE 'error|exception|oom|out of memory|killed'; true)
        errors=${errors:-0}

        metric_emit logs error_count "$errors" "$( (( errors > 10 )) && echo warn || echo ok )" "$service" ""

        if (( errors > 10 )); then
            last_error=$(docker compose logs --tail="$LOG_TAIL_LINES" "$service" 2>/dev/null \
                | grep -iE 'error|exception|oom|out of memory|killed' | tail -1 | cut -c1-120)
            result "$service" WARN "${errors} errors in last ${LOG_TAIL_LINES} lines: ${last_error}"
        elif (( errors > 0 )); then
            result "$service" PASS "${errors} errors in last ${LOG_TAIL_LINES} lines"
        else
            result "$service" PASS "clean"
        fi
    done
}
