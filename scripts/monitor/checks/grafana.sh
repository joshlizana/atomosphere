# checks/grafana.sh — /api/health reachability. Restart on failure.

check_grafana() {
    section "GRAFANA"

    local state
    state=$(container_state grafana)
    if [[ "$state" != "running" ]]; then
        # Container-level handling already covered by check_containers.
        result "grafana" SKIP "container state: ${state:-missing}"
        return
    fi

    if curl -sf --max-time 5 "${GRAFANA_URL}/api/health" > /dev/null 2>&1; then
        metric_emit grafana reachable 1 ok grafana ""
        result "health" PASS "reachable"
    else
        metric_emit grafana reachable 0 breach grafana ""
        result "health" FAIL "/api/health unreachable"
        log_breach "grafana_health" "down" "up"
        heal_request grafana "/api/health unreachable"
    fi
}
