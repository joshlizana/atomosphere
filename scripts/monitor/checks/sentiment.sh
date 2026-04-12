# checks/sentiment.sh — sentiment distribution (informational) + score validity
# (scores must sum to 1.0 ± 0.01).

check_sentiment() {
    section "SENTIMENT DISTRIBUTION"

    if $HEALED_THIS_CYCLE; then
        result "sentiment" SKIP "service restarted this cycle"
        return
    fi

    local core_sentiment
    core_sentiment=$(spark_sql_scalar "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment")

    if [[ "$core_sentiment" == "-1" || "$core_sentiment" == "0" ]]; then
        result "sentiment" SKIP "no sentiment data"
        return
    fi

    local dist
    dist=$(spark_sql "
        SELECT sentiment_label,
               COUNT(*) as cnt,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM atmosphere.core.core_post_sentiment
        GROUP BY sentiment_label
        ORDER BY cnt DESC" 2>/dev/null || echo "")

    if [[ -n "$dist" ]]; then
        local label cnt pct
        while IFS=$'\t' read -r label cnt pct; do
            [[ -z "$label" ]] && continue
            printf "  %-28s %'d (%s%%)\n" "$label" "$cnt" "$pct"
            metric_emit sentiment dist_count "$cnt" ok "" "$label"
            metric_emit sentiment dist_pct "$pct" ok "" "$label"
        done <<< "$dist"
    fi

    local invalid
    invalid=$(spark_sql_scalar "
        SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment
        WHERE ABS(sentiment_positive + sentiment_negative + sentiment_neutral - 1.0) > 0.01")

    if [[ "$invalid" == "-1" ]]; then
        result "score validity" SKIP "query failed"
    elif [[ "$invalid" == "0" ]]; then
        metric_emit sentiment invalid_scores 0 ok "" ""
        result "score validity" PASS "all scores sum to 1.0"
    else
        metric_emit sentiment invalid_scores "$invalid" breach "" ""
        result "score validity" FAIL "$invalid rows with invalid scores"
        log_breach "score_validity" "$invalid invalid" "0"
    fi
}
