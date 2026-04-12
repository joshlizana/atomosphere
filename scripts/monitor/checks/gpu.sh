# checks/gpu.sh — nvidia-smi memory usage, warn at >90%.

check_gpu() {
    section "GPU"

    local gpu_info
    gpu_info=$(docker exec spark-unified nvidia-smi \
        --query-gpu=name,memory.used,memory.total \
        --format=csv,noheader,nounits 2>/dev/null || echo "")

    if [[ -z "$gpu_info" ]]; then
        result "gpu" WARN "not detected — running in CPU mode"
        metric_emit gpu available 0 warn spark-unified ""
        return
    fi

    local gpu_name mem_used mem_total pct
    gpu_name=$(echo "$gpu_info" | cut -d',' -f1 | xargs)
    mem_used=$(echo "$gpu_info" | cut -d',' -f2 | xargs)
    mem_total=$(echo "$gpu_info" | cut -d',' -f3 | xargs)
    pct=$(( mem_used * 100 / mem_total ))

    metric_emit gpu mem_used_mib "$mem_used" ok spark-unified "$gpu_name"
    metric_emit gpu mem_total_mib "$mem_total" ok spark-unified "$gpu_name"
    metric_emit gpu mem_used_pct "$pct" "$( (( pct > 90 )) && echo warn || echo ok )" spark-unified "$gpu_name"

    if (( pct > 90 )); then
        result "gpu" WARN "$gpu_name ${mem_used}/${mem_total} MiB (${pct}% — high)"
    else
        result "gpu" PASS "$gpu_name ${mem_used}/${mem_total} MiB (${pct}%)"
    fi
}
