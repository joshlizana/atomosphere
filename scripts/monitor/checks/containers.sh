# checks/containers.sh — docker compose ps + restart-count warning.

check_containers() {
    section "CONTAINERS"
    local running=0 total=0
    local service state

    for service in "${SERVICES[@]}"; do
        ((total++))
        state=$(container_state "$service")
        if [[ "$state" == "running" ]]; then
            ((running++))
            metric_emit containers state 1 ok "$service" running
        else
            metric_emit containers state 0 breach "$service" "${state:-missing}"
            heal_request "$service" "container not running (state: ${state:-missing})"
        fi
    done

    result "services" PASS "$running/$total running"
    metric_emit containers running_count "$running" ok "" ""

    for service in "${SERVICES[@]}"; do
        local restarts
        restarts=$(docker inspect --format='{{.RestartCount}}' "$service" 2>/dev/null || echo "0")
        metric_emit containers restart_count "$restarts" ok "$service" ""
        if (( restarts > 5 )); then
            result "$service restarts" WARN "$restarts total restarts"
        fi
    done
}
