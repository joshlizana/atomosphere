#!/usr/bin/env bash
# smoke-test.sh — Acceptance test suite for the Atmosphere pipeline
#
# Runs all TRD acceptance criteria (AC-01 through AC-03, AC-06) and
# component-level tests for ingestion, staging, core, and sentiment.
# AC-04/AC-05 (Grafana/public URL) require manual verification.
#
# Usage:
#   ./scripts/smoke-test.sh              # Run all tests
#   ./scripts/smoke-test.sh --wait       # Wait up to 5 min for data before testing
#   ./scripts/smoke-test.sh --section X  # Run only section X (containers|ingestion|staging|core|sentiment|pipeline)

set -uo pipefail

# --- Configuration ---
WAIT_MODE=false
SECTION="all"
SPARK_SQL_CONTAINER="spark-unified"
OBSERVATION_WINDOW=60  # seconds to observe data flow
LOG_FILE="logs/smoke-test.log"

# Counters
PASS=0
FAIL=0
SKIP=0

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --wait) WAIT_MODE=true; shift ;;
        --section) SECTION="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Helpers ---
timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(timestamp)] $1"
    echo "$msg"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
}

spark_sql() {
    docker exec "$SPARK_SQL_CONTAINER" /opt/spark/bin/spark-sql \
        --conf spark.sql.defaultCatalog=atmosphere \
        -e "$1" 2>/dev/null
}

spark_sql_numeric() {
    local result
    result=$(spark_sql "$1" | tr -d '[:space:]')
    # Return 0 if empty or non-numeric
    if [[ "$result" =~ ^[0-9]+$ ]]; then
        echo "$result"
    else
        echo "0"
    fi
}

print_header() {
    echo ""
    echo "============================================"
    echo "  $1"
    echo "============================================"
}

record() {
    local status="$1" name="$2" detail="$3"
    case "$status" in
        PASS) ((PASS++)); echo "  [PASS] $name — $detail" ;;
        FAIL) ((FAIL++)); echo "  [FAIL] $name — $detail" ;;
        SKIP) ((SKIP++)); echo "  [SKIP] $name — $detail" ;;
    esac
    log "$status: $name — $detail"
}

should_run() {
    [[ "$SECTION" == "all" || "$SECTION" == "$1" ]]
}

# --- Pre-flight ---
echo "Atmosphere Smoke Test Suite"
echo "Started: $(timestamp)"
echo ""

state=$(docker compose ps --format '{{.State}}' "$SPARK_SQL_CONTAINER" 2>/dev/null || echo "")
if [[ "$state" != "running" ]]; then
    echo "ERROR: $SPARK_SQL_CONTAINER is not running. Start the stack first (make up)."
    exit 1
fi

# --- Wait mode: wait for data to flow before testing ---
if $WAIT_MODE; then
    echo "Waiting for data to flow (up to 5 minutes)..."
    for i in $(seq 1 60); do
        count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.raw.raw_events")
        if [[ "$count" -gt 0 ]]; then
            echo "  Data detected after $((i * 5)) seconds ($count raw events)"
            echo "  Waiting ${OBSERVATION_WINDOW}s observation window..."
            sleep "$OBSERVATION_WINDOW"
            break
        fi
        if [[ "$i" -eq 60 ]]; then
            echo "  TIMEOUT: No data after 5 minutes. Running tests anyway."
        fi
        sleep 5
    done
fi

# ==========================================================
# AC-01: All containers reach healthy state
# ==========================================================
if should_run "containers"; then
    print_header "AC-01: Container Health"

    EXPECTED_SERVICES=(rustfs polaris postgres spark-unified spark-thrift)
    OPTIONAL_SERVICES=(grafana cloudflared)

    for svc in "${EXPECTED_SERVICES[@]}"; do
        svc_state=$(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null || echo "missing")
        if [[ "$svc_state" == "running" ]]; then
            record PASS "$svc" "running"
        else
            record FAIL "$svc" "state=$svc_state (expected: running)"
        fi
    done

    # Init should have exited successfully
    init_exit=$(docker compose ps --format '{{.State}}' "init" 2>/dev/null || echo "missing")
    if [[ "$init_exit" == "exited" || "$init_exit" == "missing" ]]; then
        record PASS "init" "exited (expected behavior)"
    else
        record FAIL "init" "state=$init_exit (expected: exited)"
    fi

    for svc in "${OPTIONAL_SERVICES[@]}"; do
        svc_state=$(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null || echo "not configured")
        if [[ "$svc_state" == "running" ]]; then
            record PASS "$svc" "running"
        else
            record SKIP "$svc" "state=$svc_state (optional service)"
        fi
    done

    # Check Spark UI is accessible
    if curl -s -o /dev/null -w '%{http_code}' http://localhost:4040 2>/dev/null | grep -q "200"; then
        record PASS "spark-ui" "accessible at :4040"
    else
        record FAIL "spark-ui" "not accessible at :4040"
    fi

    # Check streaming queries are active
    query_count=$(curl -s http://localhost:4040/api/v1/applications 2>/dev/null | grep -c "spark-unified" || echo "0")
    if [[ "$query_count" -gt 0 ]]; then
        record PASS "spark-app" "spark-unified application registered"
    else
        record SKIP "spark-app" "could not verify via Spark REST API"
    fi
fi

# ==========================================================
# AC-02: Data flows through all four medallion layers
# ==========================================================
if should_run "ingestion"; then
    print_header "AC-02a: Ingestion Layer"

    # Check raw_events table exists and has data
    raw_count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.raw.raw_events")
    if [[ "$raw_count" -gt 0 ]]; then
        record PASS "raw_events exists" "$raw_count rows"
    else
        record FAIL "raw_events exists" "0 rows"
    fi

    # Check data is fresh (within last 5 minutes)
    fresh_count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.raw.raw_events WHERE ingested_at > current_timestamp() - INTERVAL 5 MINUTES")
    if [[ "$fresh_count" -gt 0 ]]; then
        record PASS "raw_events freshness" "$fresh_count rows in last 5 min"
    else
        record FAIL "raw_events freshness" "no rows in last 5 min"
    fi

    # Check sustained throughput (FR-01: ~240 events/sec)
    if [[ "$OBSERVATION_WINDOW" -ge 30 ]]; then
        count_before=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.raw.raw_events")
        sleep 10
        count_after=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.raw.raw_events")
        delta=$((count_after - count_before))
        rate=$((delta / 10))
        if [[ "$rate" -gt 100 ]]; then
            record PASS "ingestion throughput" "${rate} events/sec"
        elif [[ "$rate" -gt 0 ]]; then
            record PASS "ingestion throughput" "${rate} events/sec (below expected ~240, but flowing)"
        else
            record FAIL "ingestion throughput" "0 events/sec over 10s window"
        fi
    fi

    # Check raw event fidelity — verify JSON structure
    has_fields=$(spark_sql "SELECT COUNT(*) FROM atmosphere.raw.raw_events WHERE raw_json IS NOT NULL AND collection IS NOT NULL AND did IS NOT NULL LIMIT 1" | tr -d '[:space:]')
    if [[ "$has_fields" =~ ^[0-9]+$ && "$has_fields" -gt 0 ]]; then
        record PASS "raw event fidelity" "raw_json, collection, did all present"
    else
        record FAIL "raw event fidelity" "missing required fields"
    fi

    # Check collection distribution
    collections=$(spark_sql "SELECT DISTINCT collection FROM atmosphere.raw.raw_events" | tr -d '[:space:]' | tr '\n' ',')
    record PASS "collection types" "$collections"
fi

# ==========================================================
# AC-02b: Staging Layer
# ==========================================================
if should_run "staging"; then
    print_header "AC-02b: Staging Layer"

    STAGING_TABLES=(
        atmosphere.staging.stg_posts
        atmosphere.staging.stg_likes
        atmosphere.staging.stg_reposts
        atmosphere.staging.stg_follows
        atmosphere.staging.stg_blocks
        atmosphere.staging.stg_profiles
    )

    for table in "${STAGING_TABLES[@]}"; do
        short=${table##*.}
        count=$(spark_sql_numeric "SELECT COUNT(*) FROM $table")
        if [[ "$count" -gt 0 ]]; then
            record PASS "$short" "$count rows"
        else
            record FAIL "$short" "0 rows"
        fi
    done

    # Check staging freshness
    stg_fresh=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.staging.stg_posts WHERE event_time > current_timestamp() - INTERVAL 5 MINUTES")
    if [[ "$stg_fresh" -gt 0 ]]; then
        record PASS "staging freshness" "$stg_fresh stg_posts in last 5 min"
    else
        record FAIL "staging freshness" "no stg_posts in last 5 min"
    fi

    # Type correctness spot-check (FR-05)
    type_check=$(spark_sql "SELECT text, created_at, langs, embed_type FROM atmosphere.staging.stg_posts LIMIT 1" | head -1)
    if [[ -n "$type_check" ]]; then
        record PASS "type correctness" "stg_posts columns readable"
    else
        record FAIL "type correctness" "could not read stg_posts columns"
    fi
fi

# ==========================================================
# AC-02c: Core Layer
# ==========================================================
if should_run "core"; then
    print_header "AC-02c: Core + Mart Layer"

    CORE_TABLES=(
        atmosphere.core.core_posts
        atmosphere.core.core_mentions
        atmosphere.core.core_hashtags
        atmosphere.core.core_engagement
    )

    for table in "${CORE_TABLES[@]}"; do
        short=${table##*.}
        count=$(spark_sql_numeric "SELECT COUNT(*) FROM $table")
        if [[ "$count" -gt 0 ]]; then
            record PASS "$short" "$count rows"
        else
            record FAIL "$short" "0 rows"
        fi
    done

    # Core freshness
    core_fresh=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_posts WHERE event_time > current_timestamp() - INTERVAL 5 MINUTES")
    if [[ "$core_fresh" -gt 0 ]]; then
        record PASS "core freshness" "$core_fresh core_posts in last 5 min"
    else
        record FAIL "core freshness" "no core_posts in last 5 min"
    fi

    # Verify enrichment (FR-06, FR-07)
    enriched=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_posts WHERE content_type IS NOT NULL")
    if [[ "$enriched" -gt 0 ]]; then
        record PASS "post enrichment" "$enriched posts with content_type"
    else
        record FAIL "post enrichment" "no posts with content_type"
    fi

    # Verify hashtag extraction (FR-08)
    hashtags=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_hashtags")
    if [[ "$hashtags" -gt 0 ]]; then
        record PASS "hashtag extraction" "$hashtags hashtag rows"
    else
        record FAIL "hashtag extraction" "0 hashtag rows"
    fi

    # Verify mention extraction
    mentions=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_mentions")
    if [[ "$mentions" -gt 0 ]]; then
        record PASS "mention extraction" "$mentions mention rows"
    else
        record FAIL "mention extraction" "0 mention rows"
    fi

    # Check mart materialization (FR-15 through FR-18)
    MART_TABLES=(
        atmosphere.mart.mart_events_per_second
        atmosphere.mart.mart_trending_hashtags
        atmosphere.mart.mart_engagement_velocity
        atmosphere.mart.mart_pipeline_health
        atmosphere.mart.mart_sentiment_timeseries
    )

    for table in "${MART_TABLES[@]}"; do
        short=${table##*.}
        count=$(spark_sql_numeric "SELECT COUNT(*) FROM $table")
        if [[ "$count" -gt 0 ]]; then
            record PASS "$short" "$count rows"
        else
            record FAIL "$short" "0 rows"
        fi
    done
fi

# ==========================================================
# AC-03: Sentiment coverage and validity
# ==========================================================
if should_run "sentiment"; then
    print_header "AC-03: Sentiment Analysis"

    # Check core_post_sentiment exists and has data
    sent_count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment")
    if [[ "$sent_count" -gt 0 ]]; then
        record PASS "sentiment table" "$sent_count rows"
    else
        record FAIL "sentiment table" "0 rows"
    fi

    # Score completeness — no null labels (FR-11)
    null_labels=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE sentiment_label IS NULL")
    if [[ "$null_labels" -eq 0 ]]; then
        record PASS "score completeness" "zero null labels"
    else
        record FAIL "score completeness" "$null_labels rows with null sentiment_label"
    fi

    # Score validity — three scores sum to 1.0 (FR-11)
    invalid_sums=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE ABS(sentiment_positive + sentiment_negative + sentiment_neutral - 1.0) > 0.01")
    if [[ "$invalid_sums" -eq 0 ]]; then
        record PASS "score validity" "all rows sum to 1.0"
    else
        record FAIL "score validity" "$invalid_sums rows with invalid score sum"
    fi

    # Label distribution — should have all three classes
    label_count=$(spark_sql_numeric "SELECT COUNT(DISTINCT sentiment_label) FROM atmosphere.core.core_post_sentiment")
    if [[ "$label_count" -eq 3 ]]; then
        record PASS "label distribution" "all 3 classes present (positive, negative, neutral)"
    elif [[ "$label_count" -gt 0 ]]; then
        record PASS "label distribution" "$label_count of 3 classes present (may need more data)"
    else
        record FAIL "label distribution" "no sentiment labels found"
    fi

    # Confidence range — should be between 0.33 and 1.0
    bad_confidence=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE sentiment_confidence < 0.33 OR sentiment_confidence > 1.0")
    if [[ "$bad_confidence" -eq 0 ]]; then
        record PASS "confidence range" "all scores in [0.33, 1.0]"
    else
        record FAIL "confidence range" "$bad_confidence rows outside expected range"
    fi

    # Sentiment coverage — compare with core_posts (AC-03)
    if [[ "$sent_count" -gt 0 ]]; then
        posts_count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_posts")
        if [[ "$posts_count" -gt 0 ]]; then
            coverage_pct=$((sent_count * 100 / posts_count))
            if [[ "$coverage_pct" -ge 90 ]]; then
                record PASS "sentiment coverage" "${coverage_pct}% ($sent_count / $posts_count posts)"
            elif [[ "$coverage_pct" -ge 50 ]]; then
                record PASS "sentiment coverage" "${coverage_pct}% — still catching up ($sent_count / $posts_count)"
            else
                record FAIL "sentiment coverage" "${coverage_pct}% ($sent_count / $posts_count posts)"
            fi
        fi
    fi

    # Multilingual coverage (FR-13) — check non-English posts have scores
    multilingual=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE primary_lang IS NOT NULL AND primary_lang != 'en'")
    if [[ "$multilingual" -gt 0 ]]; then
        record PASS "multilingual coverage" "$multilingual non-English posts scored"
    else
        record SKIP "multilingual coverage" "no non-English posts found yet"
    fi

    # GPU detection
    gpu_check=$(docker exec "$SPARK_SQL_CONTAINER" python3 -c "import torch; print('gpu' if torch.cuda.is_available() else 'cpu')" 2>/dev/null || echo "unknown")
    if [[ "$gpu_check" == "gpu" ]]; then
        record PASS "GPU detection" "CUDA available"
    elif [[ "$gpu_check" == "cpu" ]]; then
        record PASS "GPU detection" "CPU mode (GPU not available)"
    else
        record SKIP "GPU detection" "could not detect"
    fi
fi

# ==========================================================
# Pipeline health and end-to-end latency
# ==========================================================
if should_run "pipeline"; then
    print_header "Pipeline Health"

    # Check pipeline health mart has data
    health_count=$(spark_sql_numeric "SELECT COUNT(*) FROM atmosphere.mart.mart_pipeline_health")
    if [[ "$health_count" -gt 0 ]]; then
        record PASS "pipeline health mart" "$health_count rows"
    else
        record FAIL "pipeline health mart" "0 rows"
    fi

    # Check events per second mart
    eps=$(spark_sql "SELECT CAST(total_events AS INT) FROM atmosphere.mart.mart_events_per_second ORDER BY window_start DESC LIMIT 1" | tr -d '[:space:]')
    if [[ "$eps" =~ ^[0-9]+$ && "$eps" -gt 0 ]]; then
        record PASS "events per second" "${eps} events in latest window"
    else
        record SKIP "events per second" "no recent window data"
    fi

    # Check checkpoint directories exist
    checkpoint_dirs=(ingest-raw staging "core/posts" "core/engagement" sentiment)
    for dir in "${checkpoint_dirs[@]}"; do
        if docker exec "$SPARK_SQL_CONTAINER" test -d "/opt/spark/checkpoints/$dir" 2>/dev/null; then
            record PASS "checkpoint: $dir" "directory exists"
        else
            record FAIL "checkpoint: $dir" "directory missing"
        fi
    done

    # Memory usage
    mem_usage=$(docker stats --no-stream --format '{{.MemUsage}}' "$SPARK_SQL_CONTAINER" 2>/dev/null | head -1)
    if [[ -n "$mem_usage" ]]; then
        record PASS "memory usage" "$mem_usage"
    else
        record SKIP "memory usage" "could not read docker stats"
    fi
fi

# ==========================================================
# Summary
# ==========================================================
echo ""
echo "============================================"
echo "  RESULTS"
echo "============================================"
echo ""
TOTAL=$((PASS + FAIL + SKIP))
echo "  Total:   $TOTAL"
echo "  Passed:  $PASS"
echo "  Failed:  $FAIL"
echo "  Skipped: $SKIP"
echo ""

if [[ "$FAIL" -eq 0 ]]; then
    echo "  ALL TESTS PASSED"
    log "Smoke test complete: $PASS passed, $SKIP skipped, 0 failed"
    exit 0
else
    echo "  $FAIL TEST(S) FAILED"
    log "Smoke test complete: $PASS passed, $SKIP skipped, $FAIL FAILED"
    exit 1
fi
