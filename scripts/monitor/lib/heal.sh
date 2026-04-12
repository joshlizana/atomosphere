# monitor/lib/heal.sh — rate-limited restart + consecutive-breach tracking.
#
# Checks call `heal_request <service> <reason>` to enqueue a restart.
# Orchestrator calls `apply_heals` after all checks run. Duplicates in the
# queue are deduped. Rate limit: MAX_RESTARTS per RESTART_WINDOW per service.

heal_request() {
    local service=$1 reason=$2
    HEAL_QUEUE+=("${service}|${reason}")
    trace "HEAL-REQUEST ${service}: ${reason}"
}

_restarts_in_window() {
    local service=$1
    local cutoff=$(( $(date +%s) - RESTART_WINDOW ))
    sqlite3 "$METRICS_DB" \
        "SELECT COUNT(*) FROM heal_history WHERE service='$service' AND ts >= $cutoff;" \
        2>/dev/null || echo "0"
}

_record_heal() {
    local service=$1 reason=$2
    local escaped_reason=${reason//\'/\'\'}
    sqlite3 "$METRICS_DB" \
        "INSERT INTO heal_history (ts, service, reason) VALUES ($(date +%s), '$service', '$escaped_reason');" \
        2>/dev/null || true
}

restart_service() {
    local service=$1 reason=$2

    local count
    count=$(_restarts_in_window "$service")
    if (( count >= MAX_RESTARTS )); then
        log_alert "ESCALATE: $service has restarted ${count}+ times in ${RESTART_WINDOW}s — manual intervention needed"
        result "$service" FAIL "restart loop detected ($reason)"
        return 1
    fi

    if $DRY_RUN; then
        trace "HEAL(dry-run) $service — $reason"
        log "DRY-RUN: would restart $service ($reason)"
        result "$service" WARN "needs restart: $reason (dry-run)"
    else
        trace "HEAL restart $service — $reason"
        log "HEAL: restarting $service ($reason)"
        docker compose restart "$service" >> "$LOG_FILE" 2>&1
        _record_heal "$service" "$reason"
        result "$service" WARN "restarted: $reason"
    fi
    HEALED_THIS_CYCLE=true
}

apply_heals() {
    # Drain HEAL_QUEUE. Dedupe by service — one restart per service per cycle.
    (( ${#HEAL_QUEUE[@]} == 0 )) && return 0

    section "SELF-HEAL"
    declare -A seen
    local entry service reason
    for entry in "${HEAL_QUEUE[@]}"; do
        service=${entry%%|*}
        reason=${entry#*|}
        [[ -n "${seen[$service]:-}" ]] && continue
        seen[$service]=1
        restart_service "$service" "$reason"
    done

    HEAL_QUEUE=()
}

# --- Consecutive-breach tracking -------------------------------------------

breach_streak_increment() {
    # breach_streak_increment <key>  → echoes new count
    local key=$1
    local cur=${BREACH_STREAK[$key]:-0}
    cur=$((cur + 1))
    BREACH_STREAK[$key]=$cur
    echo "$cur"
}

breach_streak_reset() {
    local key=$1
    BREACH_STREAK[$key]=0
}

breach_streak_get() {
    local key=$1
    echo "${BREACH_STREAK[$key]:-0}"
}
