# checks/pipeline.sh — consolidated pipeline health check.
#
# Replaces freshness, throughput, duplicates, data_loss, and ratios with a
# single UNION ALL query to query-api. One HTTP round-trip, one Spark job,
# all pipeline metrics parsed from the response.
#
# Output rows are keyed: "<prefix>:<name>" -> value
#   fresh:*     — lag in seconds (MAX timestamp vs now)
#   thru:*      — event count over THROUGHPUT_WINDOW
#   dup:*       — duplicate (did,time_us) pair count over DUPLICATE_WINDOW
#   dloss:*     — row count over DATA_LOSS_WINDOW (for cross-layer drop %)
#   count:*     — full-table row counts (for ratios + trending signal)

DATA_LOSS_WINDOW=300

check_pipeline() {
    if $HEALED_THIS_CYCLE; then
        section "PIPELINE (freshness · throughput · duplicates · data-loss · ratios)"
        result "all layers" SKIP "service restarted this cycle"
        return
    fi

    local tw=$THROUGHPUT_WINDOW
    local dw=$DUPLICATE_WINDOW
    local lw=$DATA_LOSS_WINDOW

    local sql="
        SELECT 'fresh:raw_events'         AS k, CAST(TIMESTAMPDIFF(SECOND, MAX(ingested_at), current_timestamp()) AS INT) AS v FROM atmosphere.raw.raw_events
        UNION ALL SELECT 'fresh:stg_posts',          CAST(TIMESTAMPDIFF(SECOND, MAX(event_time), current_timestamp()) AS INT) FROM atmosphere.staging.stg_posts
        UNION ALL SELECT 'fresh:core_posts',         CAST(TIMESTAMPDIFF(SECOND, MAX(event_time), current_timestamp()) AS INT) FROM atmosphere.core.core_posts
        UNION ALL SELECT 'fresh:core_post_sentiment',CAST(TIMESTAMPDIFF(SECOND, MAX(event_time), current_timestamp()) AS INT) FROM atmosphere.core.core_post_sentiment
        UNION ALL SELECT 'thru:raw_events',    COUNT(*) FROM atmosphere.raw.raw_events          WHERE ingested_at  >= current_timestamp() - INTERVAL ${tw} SECONDS
        UNION ALL SELECT 'thru:stg_posts',     COUNT(*) FROM atmosphere.staging.stg_posts       WHERE event_time   >= current_timestamp() - INTERVAL ${tw} SECONDS
        UNION ALL SELECT 'thru:core_posts',    COUNT(*) FROM atmosphere.core.core_posts         WHERE event_time   >= current_timestamp() - INTERVAL ${tw} SECONDS
        UNION ALL SELECT 'thru:core_post_sentiment', COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE event_time >= current_timestamp() - INTERVAL ${tw} SECONDS
        UNION ALL SELECT 'dup:stg_posts', COUNT(*) FROM (SELECT did,time_us,COUNT(*) c FROM atmosphere.staging.stg_posts WHERE event_time >= current_timestamp() - INTERVAL ${dw} SECONDS GROUP BY did,time_us HAVING c>1)
        UNION ALL SELECT 'dup:core_posts', COUNT(*) FROM (SELECT did,time_us,COUNT(*) c FROM atmosphere.core.core_posts WHERE event_time >= current_timestamp() - INTERVAL ${dw} SECONDS GROUP BY did,time_us HAVING c>1)
        UNION ALL SELECT 'dup:core_post_sentiment', COUNT(*) FROM (SELECT did,time_us,COUNT(*) c FROM atmosphere.core.core_post_sentiment WHERE event_time >= current_timestamp() - INTERVAL ${dw} SECONDS GROUP BY did,time_us HAVING c>1)
        UNION ALL SELECT 'dloss:raw_posts',      COUNT(*) FROM atmosphere.raw.raw_events        WHERE collection='app.bsky.feed.post' AND ingested_at >= current_timestamp() - INTERVAL ${lw} SECONDS
        UNION ALL SELECT 'dloss:stg_posts',      COUNT(*) FROM atmosphere.staging.stg_posts     WHERE event_time >= current_timestamp() - INTERVAL ${lw} SECONDS
        UNION ALL SELECT 'dloss:core_posts',     COUNT(*) FROM atmosphere.core.core_posts       WHERE event_time >= current_timestamp() - INTERVAL ${lw} SECONDS
        UNION ALL SELECT 'dloss:sentiment_posts',COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE event_time >= current_timestamp() - INTERVAL ${lw} SECONDS
        UNION ALL SELECT 'count:raw_events',     COUNT(*) FROM atmosphere.raw.raw_events
        UNION ALL SELECT 'count:stg_posts',      COUNT(*) FROM atmosphere.staging.stg_posts
        UNION ALL SELECT 'count:stg_likes',      COUNT(*) FROM atmosphere.staging.stg_likes
        UNION ALL SELECT 'count:stg_reposts',    COUNT(*) FROM atmosphere.staging.stg_reposts
        UNION ALL SELECT 'count:stg_follows',    COUNT(*) FROM atmosphere.staging.stg_follows
        UNION ALL SELECT 'count:stg_blocks',     COUNT(*) FROM atmosphere.staging.stg_blocks
        UNION ALL SELECT 'count:stg_profiles',   COUNT(*) FROM atmosphere.staging.stg_profiles
        UNION ALL SELECT 'count:core_posts',     COUNT(*) FROM atmosphere.core.core_posts
        UNION ALL SELECT 'count:core_mentions',  COUNT(*) FROM atmosphere.core.core_mentions
        UNION ALL SELECT 'count:core_hashtags',  COUNT(*) FROM atmosphere.core.core_hashtags
        UNION ALL SELECT 'count:core_engagement',COUNT(*) FROM atmosphere.core.core_engagement
        UNION ALL SELECT 'count:core_post_sentiment', COUNT(*) FROM atmosphere.core.core_post_sentiment
    "

    # Single query-api call for all pipeline metrics
    declare -A m
    local k v
    while IFS=$'\t' read -r k v; do
        [[ -z "$k" ]] && continue
        m[$k]=$v
    done < <(query_api_sql_tsv "$sql" 50)

    # --- FRESHNESS -------------------------------------------------------------
    section "FRESHNESS"

    local -A fresh_thresh=(
        [raw_events]=$INGEST_LAG_MAX
        [stg_posts]=$STAGING_LAG_MAX
        [core_posts]=$CORE_LAG_MAX
        [core_post_sentiment]=$SENTIMENT_LAG_MAX
    )

    local upstream_stale=false layer threshold lag streak
    for layer in raw_events stg_posts core_posts core_post_sentiment; do
        if $upstream_stale; then
            result "$layer" SKIP "upstream stale"
            continue
        fi
        threshold=${fresh_thresh[$layer]}
        lag=${m[fresh:$layer]:-}

        if [[ -z "$lag" || "$lag" == "-1" ]]; then
            result "$layer" WARN "no data yet"
            upstream_stale=true
            continue
        fi

        metric_emit freshness lag_seconds "$lag" "$( (( lag > threshold )) && echo breach || echo ok )" spark-unified "$layer"

        if (( lag > threshold )); then
            streak=$(breach_streak_increment "freshness_${layer}")
            result "$layer" FAIL "${lag}s (SLA: <${threshold}s) [streak ${streak}/${FRESHNESS_CONSECUTIVE_BREACHES}]"
            log_breach "freshness_${layer}" "${lag}s" "<${threshold}s"
            if (( streak >= FRESHNESS_CONSECUTIVE_BREACHES )); then
                heal_request spark-unified "freshness ${layer}: ${lag}s > ${threshold}s for ${streak} cycles"
                breach_streak_reset "freshness_${layer}"
            fi
            upstream_stale=true
        else
            breach_streak_reset "freshness_${layer}"
            result "$layer" PASS "${lag}s"
        fi
    done

    # --- THROUGHPUT -----------------------------------------------------------
    section "THROUGHPUT"

    local count rate
    for layer in raw_events stg_posts core_posts core_post_sentiment; do
        count=${m[thru:$layer]:-}
        if [[ -z "$count" || "$count" == "-1" ]]; then
            result "$layer" WARN "query failed"
            continue
        fi
        rate=$(( count / THROUGHPUT_WINDOW ))
        metric_emit throughput events_per_sec "$rate" ok spark-unified "$layer"
        result "$layer" PASS "${rate} events/sec (${count} in ${THROUGHPUT_WINDOW}s)"
    done

    # --- DUPLICATES -----------------------------------------------------------
    section "DUPLICATES"

    local dupes
    for layer in stg_posts core_posts core_post_sentiment; do
        dupes=${m[dup:$layer]:-}
        if [[ -z "$dupes" || "$dupes" == "-1" ]]; then
            result "$layer" WARN "query failed"
            continue
        fi
        metric_emit duplicates count "$dupes" "$( (( dupes > 0 )) && echo breach || echo ok )" "" "$layer"
        if (( dupes > 0 )); then
            result "$layer" FAIL "$dupes duplicate (did,time_us) pairs in last ${DUPLICATE_WINDOW}s"
            log_breach "duplicates_${layer}" "$dupes" "0"
        else
            result "$layer" PASS "no duplicates in last ${DUPLICATE_WINDOW}s"
        fi
    done

    # --- DATA LOSS ------------------------------------------------------------
    section "DATA LOSS"

    local upstream downstream drop_pct
    local -A dloss_pairs=(
        [raw_to_staging]="raw_posts stg_posts"
        [staging_to_core]="stg_posts core_posts"
        [core_to_sentiment]="core_posts sentiment_posts"
    )

    for pair_name in raw_to_staging staging_to_core core_to_sentiment; do
        read -r up_key dn_key <<< "${dloss_pairs[$pair_name]}"
        upstream=${m[dloss:$up_key]:-}
        downstream=${m[dloss:$dn_key]:-}

        if [[ -z "$upstream" || -z "$downstream" ]]; then
            result "$pair_name" WARN "query failed"
            continue
        fi
        if (( upstream == 0 )); then
            result "$pair_name" SKIP "no upstream data"
            continue
        fi
        drop_pct=$(( (upstream - downstream) * 100 / upstream ))
        (( drop_pct < 0 )) && drop_pct=0
        metric_emit data_loss drop_pct "$drop_pct" "$( (( drop_pct > DATA_LOSS_MAX_PCT )) && echo breach || echo ok )" "" "$pair_name"
        if (( drop_pct > DATA_LOSS_MAX_PCT )); then
            result "$pair_name" FAIL "${drop_pct}% loss ($downstream/$upstream) in last ${DATA_LOSS_WINDOW}s"
            log_breach "data_loss_${pair_name}" "${drop_pct}%" "<${DATA_LOSS_MAX_PCT}%"
        else
            result "$pair_name" PASS "${drop_pct}% loss ($downstream/$upstream)"
        fi
    done

    # --- RATIOS & COUNTS ------------------------------------------------------
    section "RATIOS & COUNTS"

    local name val raw stg_posts_c stg_likes_c stg_reposts_c stg_follows_c stg_blocks_c stg_profiles_c core_posts_c core_sentiment_c

    for name in raw_events stg_posts stg_likes stg_reposts stg_follows stg_blocks stg_profiles \
                core_posts core_mentions core_hashtags core_engagement core_post_sentiment; do
        val=${m[count:$name]:-}
        [[ -z "$val" || "$val" == "-1" ]] && continue
        metric_emit table_count rows "$val" ok "" "$name"
        printf "  %-28s %'d\n" "$name" "$val"
    done
    echo ""

    raw=${m[count:raw_events]:-0}
    stg_posts_c=${m[count:stg_posts]:-0}
    stg_likes_c=${m[count:stg_likes]:-0}
    stg_reposts_c=${m[count:stg_reposts]:-0}
    stg_follows_c=${m[count:stg_follows]:-0}
    stg_blocks_c=${m[count:stg_blocks]:-0}
    stg_profiles_c=${m[count:stg_profiles]:-0}
    core_posts_c=${m[count:core_posts]:-0}
    core_sentiment_c=${m[count:core_post_sentiment]:-0}

    if [[ "$raw" == "0" ]]; then
        result "ratios" SKIP "no raw data"
    else
        local stg_total=$(( stg_posts_c + stg_likes_c + stg_reposts_c + stg_follows_c + stg_blocks_c + stg_profiles_c ))
        local stg_pct=$(( stg_total * 100 / raw ))
        metric_emit ratios staging_coverage_pct "$stg_pct" \
            "$( (( stg_pct >= STAGING_COVERAGE_MIN )) && echo ok || echo breach )" "" ""
        if (( stg_pct >= STAGING_COVERAGE_MIN )); then
            result "staging/raw" PASS "${stg_pct}% (SLA: >${STAGING_COVERAGE_MIN}%)"
        else
            result "staging/raw" FAIL "${stg_pct}% (SLA: >${STAGING_COVERAGE_MIN}%)"
            log_breach "staging_coverage" "${stg_pct}%" ">${STAGING_COVERAGE_MIN}%"
        fi

        local posts_pct=$(( stg_posts_c * 100 / raw ))
        metric_emit ratios posts_raw_pct "$posts_pct" ok "" ""
        if (( posts_pct >= POSTS_RAW_MIN && posts_pct <= POSTS_RAW_MAX )); then
            result "posts/raw" PASS "${posts_pct}% (expected: ${POSTS_RAW_MIN}-${POSTS_RAW_MAX}%)"
        else
            result "posts/raw" WARN "${posts_pct}% (expected: ${POSTS_RAW_MIN}-${POSTS_RAW_MAX}%)"
        fi

        local likes_pct=$(( stg_likes_c * 100 / raw ))
        metric_emit ratios likes_raw_pct "$likes_pct" ok "" ""
        if (( likes_pct >= LIKES_RAW_MIN && likes_pct <= LIKES_RAW_MAX )); then
            result "likes/raw" PASS "${likes_pct}% (expected: ${LIKES_RAW_MIN}-${LIKES_RAW_MAX}%)"
        else
            result "likes/raw" WARN "${likes_pct}% (expected: ${LIKES_RAW_MIN}-${LIKES_RAW_MAX}%)"
        fi
    fi

    if (( core_posts_c > 0 )); then
        local sent_pct=$(( core_sentiment_c * 100 / core_posts_c ))
        metric_emit ratios sentiment_coverage_pct "$sent_pct" \
            "$( (( sent_pct >= SENTIMENT_COVERAGE_MIN )) && echo ok || echo breach )" "" ""
        if (( sent_pct >= SENTIMENT_COVERAGE_MIN )); then
            result "sentiment coverage" PASS "${sent_pct}% (SLA: >${SENTIMENT_COVERAGE_MIN}%)"
        else
            result "sentiment coverage" FAIL "${sent_pct}% (SLA: >${SENTIMENT_COVERAGE_MIN}%)"
            log_breach "sentiment_coverage" "${sent_pct}%" ">${SENTIMENT_COVERAGE_MIN}%"
        fi
    else
        result "sentiment coverage" SKIP "no core_posts data"
    fi
}
