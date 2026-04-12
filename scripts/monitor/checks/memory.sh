# checks/memory.sh — docker stats mem usage vs limit, warn at MEMORY_WARN_PCT.

_to_mib() {
    local val=$1
    local num=${val%%[A-Za-z]*}
    local unit=${val##*[0-9.]}
    case $unit in
        GiB) awk -v n="$num" 'BEGIN { printf "%d", n * 1024 }' ;;
        MiB) echo "${num%%.*}" ;;
        KiB) echo "0" ;;
        *)   echo "0" ;;
    esac
}

check_memory() {
    section "MEMORY"
    local service stats used_raw limit_raw used_mib limit_mib pct

    for service in "${SERVICES[@]}"; do
        stats=$(docker stats --no-stream --format '{{.MemUsage}}' "$service" 2>/dev/null || echo "")
        [[ -z "$stats" ]] && continue

        used_raw=$(echo "$stats" | awk -F' / ' '{print $1}' | xargs)
        limit_raw=$(echo "$stats" | awk -F' / ' '{print $2}' | xargs)
        used_mib=$(_to_mib "$used_raw")
        limit_mib=$(_to_mib "$limit_raw")

        (( limit_mib == 0 )) && continue

        pct=$(( used_mib * 100 / limit_mib ))
        metric_emit memory used_mib "$used_mib" ok "$service" ""
        metric_emit memory limit_mib "$limit_mib" ok "$service" ""
        metric_emit memory used_pct "$pct" "$( (( pct >= MEMORY_WARN_PCT )) && echo warn || echo ok )" "$service" ""

        if (( pct >= MEMORY_WARN_PCT )); then
            result "$service" WARN "${used_raw} / ${limit_raw} (${pct}%)"
        else
            result "$service" PASS "${used_raw} / ${limit_raw} (${pct}%)"
        fi
    done
}
