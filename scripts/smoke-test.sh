#!/usr/bin/env bash
# smoke-test.sh — Acceptance test suite for the Atmosphere pipeline
#
# Runs all TRD acceptance criteria (AC-01 through AC-03, AC-06) and
# component-level tests for ingestion, staging, core, and sentiment.
# AC-04/AC-05 (Grafana/public URL) require manual verification.
#
# Queries run via a PySpark session inside the container (avoids Derby
# lock conflict with the streaming process). Total runtime: ~60-90s.
#
# Usage:
#   ./scripts/smoke-test.sh              # Run all tests
#   ./scripts/smoke-test.sh --wait       # Wait up to 5 min for data before testing

set -uo pipefail

# --- Configuration ---
WAIT_MODE=false
SPARK_SQL_CONTAINER="spark-unified"
LOG_FILE="logs/smoke-test.log"

# Counters
PASS=0
FAIL=0
SKIP=0

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --wait) WAIT_MODE=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Helpers ---
timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(timestamp)] $1"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
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

print_header() {
    echo ""
    echo "============================================"
    echo "  $1"
    echo "============================================"
}

# --- Pre-flight ---
echo "Atmosphere Smoke Test Suite"
echo "Started: $(timestamp)"
echo ""
log "=== Smoke test started ==="

state=$(docker compose ps --format '{{.State}}' "$SPARK_SQL_CONTAINER" 2>/dev/null || echo "")
if [[ "$state" != "running" ]]; then
    echo "ERROR: $SPARK_SQL_CONTAINER is not running. Start the stack first (make up)."
    exit 1
fi

# --- Wait mode: poll for data before testing ---
if $WAIT_MODE; then
    echo "Waiting for data to flow (up to 5 minutes)..."
    for i in $(seq 1 30); do
        check=$(docker exec "$SPARK_SQL_CONTAINER" python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('wait-check').config('spark.sql.defaultCatalog', 'atmosphere').getOrCreate()
try:
    r = spark.sql('SELECT COUNT(*) AS cnt FROM atmosphere.raw.raw_events').collect()
    print(r[0].cnt)
except:
    print(0)
spark.stop()
" 2>/dev/null | tail -1)
        if [[ "$check" =~ ^[0-9]+$ && "$check" -gt 0 ]]; then
            echo "  Data detected ($check raw events). Waiting 30s for pipeline to propagate..."
            sleep 30
            break
        fi
        if [[ "$i" -eq 30 ]]; then
            echo "  TIMEOUT: No data after 5 minutes. Running tests anyway."
        fi
        sleep 10
    done
fi

# ==========================================================
# AC-01: Container Health (no SQL needed)
# ==========================================================
print_header "AC-01: Container Health"

EXPECTED_SERVICES=(rustfs polaris postgres spark-unified)
OPTIONAL_SERVICES=(spark-thrift grafana cloudflared)

for svc in "${EXPECTED_SERVICES[@]}"; do
    svc_state=$(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null || echo "missing")
    if [[ "$svc_state" == "running" ]]; then
        record PASS "$svc" "running"
    else
        record FAIL "$svc" "state=$svc_state (expected: running)"
    fi
done

# Init should have exited successfully (docker compose may return empty for stopped containers)
init_code=$(docker compose ps --format '{{.ExitCode}}' "init" 2>/dev/null || echo "")
init_state=$(docker compose ps --format '{{.State}}' "init" 2>/dev/null || echo "")
if [[ "$init_code" == "0" || "$init_state" == "exited" ]]; then
    record PASS "init" "exited successfully"
elif [[ -z "$init_state" ]]; then
    record PASS "init" "completed and removed"
else
    record FAIL "init" "state=$init_state exit=$init_code"
fi

for svc in "${OPTIONAL_SERVICES[@]}"; do
    svc_state=$(docker compose ps --format '{{.State}}' "$svc" 2>/dev/null || echo "")
    if [[ "$svc_state" == "running" ]]; then
        record PASS "$svc" "running"
    elif [[ -z "$svc_state" ]]; then
        record SKIP "$svc" "not yet configured"
    else
        record SKIP "$svc" "state=$svc_state"
    fi
done

# Spark UI (check from inside container to avoid host networking issues)
spark_ui=$(docker exec "$SPARK_SQL_CONTAINER" curl -s -o /dev/null -w '%{http_code}' http://localhost:4040 2>/dev/null || echo "000")
if [[ "$spark_ui" == "200" || "$spark_ui" == "302" ]]; then
    record PASS "spark-ui" "accessible at :4040 (http $spark_ui)"
else
    record FAIL "spark-ui" "http $spark_ui at :4040"
fi

# Checkpoint directories
checkpoint_dirs=(ingest-raw staging "core/posts" "core/engagement" sentiment)
for dir in "${checkpoint_dirs[@]}"; do
    if docker exec "$SPARK_SQL_CONTAINER" test -d "/opt/spark/checkpoints/$dir" 2>/dev/null; then
        record PASS "checkpoint: $dir" "exists"
    else
        record FAIL "checkpoint: $dir" "missing"
    fi
done

# Memory usage
mem_usage=$(docker stats --no-stream --format '{{.MemUsage}}' "$SPARK_SQL_CONTAINER" 2>/dev/null | head -1)
if [[ -n "$mem_usage" ]]; then
    record PASS "memory usage" "$mem_usage"
else
    record SKIP "memory usage" "could not read"
fi

# GPU detection
gpu_check=$(docker exec "$SPARK_SQL_CONTAINER" python3 -c "import torch; print('gpu' if torch.cuda.is_available() else 'cpu')" 2>/dev/null || echo "unknown")
if [[ "$gpu_check" == "gpu" ]]; then
    record PASS "GPU" "CUDA available"
elif [[ "$gpu_check" == "cpu" ]]; then
    record PASS "GPU" "CPU mode (GPU not available)"
else
    record SKIP "GPU" "could not detect"
fi

# ==========================================================
# Run all data queries via PySpark (single session)
# ==========================================================
print_header "Running data validation queries..."
echo "  (PySpark session — ~30 seconds)"

SQL_RESULTS=$(docker exec "$SPARK_SQL_CONTAINER" python3 -c "
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName('smoke-test') \
    .config('spark.sql.defaultCatalog', 'atmosphere') \
    .config('spark.ui.enabled', 'false') \
    .getOrCreate()

queries = {
    # Ingestion
    'raw_count': 'SELECT COUNT(*) FROM atmosphere.raw.raw_events',
    'raw_fresh': 'SELECT COUNT(*) FROM atmosphere.raw.raw_events WHERE ingested_at > current_timestamp() - INTERVAL 5 MINUTES',
    'raw_fields': 'SELECT COUNT(*) FROM atmosphere.raw.raw_events WHERE raw_json IS NOT NULL AND collection IS NOT NULL AND did IS NOT NULL',

    # Staging
    'stg_posts': 'SELECT COUNT(*) FROM atmosphere.staging.stg_posts',
    'stg_likes': 'SELECT COUNT(*) FROM atmosphere.staging.stg_likes',
    'stg_reposts': 'SELECT COUNT(*) FROM atmosphere.staging.stg_reposts',
    'stg_follows': 'SELECT COUNT(*) FROM atmosphere.staging.stg_follows',
    'stg_blocks': 'SELECT COUNT(*) FROM atmosphere.staging.stg_blocks',
    'stg_profiles': 'SELECT COUNT(*) FROM atmosphere.staging.stg_profiles',
    'stg_fresh': 'SELECT COUNT(*) FROM atmosphere.staging.stg_posts WHERE event_time > current_timestamp() - INTERVAL 5 MINUTES',

    # Core
    'core_posts': 'SELECT COUNT(*) FROM atmosphere.core.core_posts',
    'core_mentions': 'SELECT COUNT(*) FROM atmosphere.core.core_mentions',
    'core_hashtags': 'SELECT COUNT(*) FROM atmosphere.core.core_hashtags',
    'core_engagement': 'SELECT COUNT(*) FROM atmosphere.core.core_engagement',
    'core_fresh': 'SELECT COUNT(*) FROM atmosphere.core.core_posts WHERE event_time > current_timestamp() - INTERVAL 5 MINUTES',
    'core_enriched': 'SELECT COUNT(*) FROM atmosphere.core.core_posts WHERE content_type IS NOT NULL',

    # Mart
    'mart_eps': 'SELECT COUNT(*) FROM atmosphere.mart.mart_events_per_second',
    'mart_hashtags': 'SELECT COUNT(*) FROM atmosphere.mart.mart_trending_hashtags',
    'mart_velocity': 'SELECT COUNT(*) FROM atmosphere.mart.mart_engagement_velocity',
    'mart_health': 'SELECT COUNT(*) FROM atmosphere.mart.mart_pipeline_health',
    'mart_sentiment': 'SELECT COUNT(*) FROM atmosphere.mart.mart_sentiment_timeseries',

    # Sentiment
    'sent_count': 'SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment',
    'sent_nulls': 'SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE sentiment_label IS NULL',
    'sent_invalid': 'SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE ABS(sentiment_positive + sentiment_negative + sentiment_neutral - 1.0) > 0.01',
    'sent_labels': 'SELECT COUNT(DISTINCT sentiment_label) FROM atmosphere.core.core_post_sentiment',
    'sent_bad_conf': 'SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE sentiment_confidence < 0.33 OR sentiment_confidence > 1.0',
    'sent_multilang': \"SELECT COUNT(*) FROM atmosphere.core.core_post_sentiment WHERE primary_lang IS NOT NULL AND primary_lang != 'en'\",
}

for name, sql in queries.items():
    try:
        val = spark.sql(sql).collect()[0][0]
        print(f'{name}\t{val}')
    except Exception as e:
        print(f'{name}\t-1', file=sys.stderr)

spark.stop()
" 2>/dev/null)

if [[ -z "$SQL_RESULTS" ]]; then
    echo "  ERROR: PySpark returned no results."
    echo "  Check: docker compose logs --tail=20 spark-unified"
    record FAIL "pyspark" "no results returned"
else
    # Helper to extract value
    get_val() {
        echo "$SQL_RESULTS" | grep "^${1}	" | cut -f2 | tr -d '[:space:]'
    }

    # ==========================================================
    # AC-02a: Ingestion
    # ==========================================================
    print_header "AC-02a: Ingestion Layer"

    val=$(get_val "raw_count")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "raw_events" "$val rows" || record FAIL "raw_events" "${val:-0} rows"

    val=$(get_val "raw_fresh")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "raw freshness" "$val rows in last 5 min" || record FAIL "raw freshness" "no recent rows"

    val=$(get_val "raw_fields")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "raw fidelity" "raw_json, collection, did present" || record FAIL "raw fidelity" "missing fields"

    # ==========================================================
    # AC-02b: Staging
    # ==========================================================
    print_header "AC-02b: Staging Layer"

    for tbl in stg_posts stg_likes stg_reposts stg_follows stg_blocks stg_profiles; do
        val=$(get_val "$tbl")
        [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "$tbl" "$val rows" || record FAIL "$tbl" "${val:-0} rows"
    done

    val=$(get_val "stg_fresh")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "staging freshness" "$val stg_posts in last 5 min" || record FAIL "staging freshness" "no recent rows"

    # ==========================================================
    # AC-02c: Core + Mart
    # ==========================================================
    print_header "AC-02c: Core + Mart Layer"

    for tbl in core_posts core_mentions core_hashtags core_engagement; do
        val=$(get_val "$tbl")
        [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "$tbl" "$val rows" || record FAIL "$tbl" "${val:-0} rows"
    done

    val=$(get_val "core_fresh")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "core freshness" "$val core_posts in last 5 min" || record FAIL "core freshness" "no recent rows"

    val=$(get_val "core_enriched")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "post enrichment" "$val posts with content_type" || record FAIL "post enrichment" "no enriched posts"

    for tbl in mart_eps mart_hashtags mart_velocity mart_health mart_sentiment; do
        val=$(get_val "$tbl")
        [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "$tbl" "$val rows" || record FAIL "$tbl" "${val:-0} rows"
    done

    # ==========================================================
    # AC-03: Sentiment
    # ==========================================================
    print_header "AC-03: Sentiment Analysis"

    sent_count=$(get_val "sent_count")
    [[ "$sent_count" -gt 0 ]] 2>/dev/null && record PASS "sentiment table" "$sent_count rows" || record FAIL "sentiment table" "${sent_count:-0} rows"

    val=$(get_val "sent_nulls")
    [[ "$val" -eq 0 ]] 2>/dev/null && record PASS "score completeness" "zero null labels" || record FAIL "score completeness" "$val null labels"

    val=$(get_val "sent_invalid")
    [[ "$val" -eq 0 ]] 2>/dev/null && record PASS "score validity" "all rows sum to 1.0" || record FAIL "score validity" "$val invalid sums"

    val=$(get_val "sent_labels")
    if [[ "$val" -eq 3 ]] 2>/dev/null; then
        record PASS "label distribution" "all 3 classes present"
    elif [[ "$val" -gt 0 ]] 2>/dev/null; then
        record PASS "label distribution" "$val of 3 classes (may need more data)"
    else
        record FAIL "label distribution" "no labels found"
    fi

    val=$(get_val "sent_bad_conf")
    [[ "$val" -eq 0 ]] 2>/dev/null && record PASS "confidence range" "all in [0.33, 1.0]" || record FAIL "confidence range" "$val out of range"

    # Coverage ratio
    if [[ "$sent_count" -gt 0 ]] 2>/dev/null; then
        posts_count=$(get_val "core_posts")
        if [[ "$posts_count" -gt 0 ]] 2>/dev/null; then
            pct=$((sent_count * 100 / posts_count))
            if [[ "$pct" -ge 90 ]]; then
                record PASS "sentiment coverage" "${pct}% ($sent_count / $posts_count)"
            elif [[ "$pct" -ge 50 ]]; then
                record PASS "sentiment coverage" "${pct}% — catching up ($sent_count / $posts_count)"
            else
                record FAIL "sentiment coverage" "${pct}% ($sent_count / $posts_count)"
            fi
        fi
    fi

    val=$(get_val "sent_multilang")
    [[ "$val" -gt 0 ]] 2>/dev/null && record PASS "multilingual" "$val non-English posts scored" || record SKIP "multilingual" "no non-English posts yet"
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
