# checks/checkpoints.sh — checkpoint subdir mtime freshness.
# Fails if a layer's newest checkpoint file is older than 3× the monitor interval.

CHECKPOINT_VOLUME="${CHECKPOINT_VOLUME:-atmosphere_spark-checkpoints}"

declare -gA CHECKPOINT_SUBDIRS=(
    [ingest]="ingest-raw"
    [staging]="staging"
    [core-posts]="core/posts"
    [core-engagement]="core/engagement"
    [sentiment]="sentiment"
)

check_checkpoints() {
    section "CHECKPOINTS"

    if $HEALED_THIS_CYCLE; then
        result "all checkpoints" SKIP "service restarted this cycle"
        return
    fi

    local layer subdir last_modified now age max_age
    now=$(date +%s)
    max_age=$(( INTERVAL * 3 ))

    for layer in ingest staging core-posts core-engagement sentiment; do
        subdir=${CHECKPOINT_SUBDIRS[$layer]}
        last_modified=$(docker run --rm -v "${CHECKPOINT_VOLUME}:/cp" alpine sh -c \
            "find /cp/${subdir} -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f1" \
            2>/dev/null || echo "")

        if [[ -z "$last_modified" ]]; then
            result "$layer" WARN "no checkpoint data"
            metric_emit checkpoints age_seconds "" warn spark-unified "$layer"
            continue
        fi

        age=$(( now - ${last_modified%%.*} ))
        metric_emit checkpoints age_seconds "$age" "$( (( age > max_age )) && echo breach || echo ok )" spark-unified "$layer"

        if (( age > max_age )); then
            result "$layer" FAIL "checkpoint stale (${age}s old)"
            log_breach "checkpoint_${layer}" "${age}s" "<${max_age}s"
        else
            result "$layer" PASS "checkpoint advancing (${age}s ago)"
        fi
    done
}
