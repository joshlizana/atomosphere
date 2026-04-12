# checks/maintenance.sh — poll /api/maintenance/table-stats for file_count /
# avg_kb breaches. Auto-triggers compaction per self-heal policy.
# SLOW TIER — runs every 10th cycle.

check_maintenance() {
    section "ICEBERG MAINTENANCE"

    local json
    json=$(query_api_get /api/maintenance/table-stats) || {
        result "maintenance" WARN "table-stats fetch failed"
        return
    }

    # Parse JSON with python, emit one row per table, count breaches.
    local summary
    summary=$(echo "$json" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
tables = d.get('tables', d) if isinstance(d, dict) else d
if isinstance(tables, dict):
    items = [{'name': k, **v} for k, v in tables.items()]
else:
    items = tables
breaches = 0
for t in items:
    name = t.get('name') or t.get('table') or '?'
    fc   = t.get('file_count', -1)
    akb  = t.get('avg_file_kb', -1)
    needs = t.get('needs_compaction', False)
    rl   = t.get('rate_limited', False)
    if needs and not rl:
        breaches += 1
    print(f'{name}\t{fc}\t{akb}\t{int(bool(needs))}\t{int(bool(rl))}')
print(f'__BREACHES__\t{breaches}')
" 2>/dev/null || echo "__BREACHES__\t-1")

    local any_breach=0
    local line name fc akb needs rl
    while IFS=$'\t' read -r name fc akb needs rl; do
        [[ -z "$name" ]] && continue
        if [[ "$name" == "__BREACHES__" ]]; then
            any_breach=$fc
            continue
        fi

        metric_emit maintenance file_count "$fc" \
            "$( [[ "$needs" == "1" ]] && echo breach || echo ok )" "" "$name"
        metric_emit maintenance avg_file_kb "$akb" \
            "$( [[ "$needs" == "1" ]] && echo breach || echo ok )" "" "$name"

        if [[ "$needs" == "1" && "$rl" == "0" ]]; then
            result "$name" FAIL "files=${fc} avg_kb=${akb} (needs compaction)"
            log_breach "maintenance_${name}" "files=${fc} avg_kb=${akb}" "files<${MAINT_FILE_COUNT_MAX} avg_kb>${MAINT_AVG_KB_MIN}"
        elif [[ "$needs" == "1" && "$rl" == "1" ]]; then
            result "$name" WARN "needs compaction (rate-limited, skipping)"
        else
            result "$name" PASS "files=${fc} avg_kb=${akb}"
        fi
    done <<< "$summary"

    if (( any_breach > 0 )) && ! $DRY_RUN; then
        log "HEAL: $any_breach table(s) need compaction — triggering /api/maintenance/run"
        curl -sf --max-time 10 -X POST "${QUERY_API_URL}/api/maintenance/run" \
            -H 'Content-Type: application/json' -d '{}' > /dev/null \
            && log_alert "auto-triggered compaction for ${any_breach} table(s)" \
            || log "HEAL: maintenance/run POST failed"
    fi
}
