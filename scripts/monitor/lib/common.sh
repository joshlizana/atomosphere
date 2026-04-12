# monitor/lib/common.sh — shared helpers for checks and orchestrator.
# Sourced, not executed. Depends on bash 4+ (associative arrays).

# --- Configuration ----------------------------------------------------------

SPARK_SQL_CONTAINER="${SPARK_SQL_CONTAINER:-spark-unified}"
QUERY_API_URL="${QUERY_API_URL:-http://localhost:8000}"
GRAFANA_URL="${GRAFANA_URL:-http://localhost:3000}"
SPARK_UI_URL="${SPARK_UI_URL:-http://localhost:4040}"

SERVICES=(spark-unified query-api grafana)

# Freshness SLA thresholds (seconds)
INGEST_LAG_MAX=15
STAGING_LAG_MAX=20
CORE_LAG_MAX=25
SENTIMENT_LAG_MAX=60

# Data validation thresholds
SENTIMENT_COVERAGE_MIN=95
STAGING_COVERAGE_MIN=85
POSTS_RAW_MIN=10
POSTS_RAW_MAX=30
LIKES_RAW_MIN=35
LIKES_RAW_MAX=65

E2E_LATENCY_MAX=90
DUPLICATE_WINDOW=300
DATA_LOSS_MAX_PCT=5
MEMORY_WARN_PCT=80
LOG_TAIL_LINES=100
THROUGHPUT_WINDOW=60

# New-check thresholds
DRIFT_ROW_THRESHOLD=1000        # writer-vs-reader count delta
MART_SLA_SECONDS=5              # NFR-04 per-mart read ceiling
MAINT_FILE_COUNT_MAX=500        # /api/maintenance/table-stats threshold
MAINT_AVG_KB_MIN=30
STUCK_QUERY_BATCH_AGE_MAX=120   # seconds since last batchId advance

# Self-heal policy
RESTART_WINDOW=300
MAX_RESTARTS=3
FRESHNESS_CONSECUTIVE_BREACHES=3
DRIFT_CONSECUTIVE_BREACHES=1

# Paths
LOG_DIR="${LOG_DIR:-logs}"
LOG_FILE="${LOG_DIR}/monitor.log"
BREACH_LOG="${LOG_DIR}/sla-breaches.log"
ALERT_LOG="${LOG_DIR}/alerts.log"
METRICS_DB="${LOG_DIR}/metrics.db"

# --- Runtime state ----------------------------------------------------------
#
# These globals are populated by the orchestrator and mutated by checks.
# Declared here so `set -u` doesn't trip on first reference.

declare -gi PASS_COUNT=0
declare -gi WARN_COUNT=0
declare -gi FAIL_COUNT=0
declare -g HEALED_THIS_CYCLE=false
declare -g DRY_RUN=${DRY_RUN:-false}
declare -gi CYCLE_ID=0
declare -gi CYCLE_NUM=0

# Consecutive-breach tracking (in-memory, resets on monitor restart)
declare -gA BREACH_STREAK

# Heal queue — populated by checks via heal_request, drained by orchestrator
declare -ga HEAL_QUEUE

# --- Logging helpers --------------------------------------------------------

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

log() {
    local msg="[$(timestamp)] $1"
    echo "$msg"
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
}

log_quiet() {
    # Only to file, not stdout — use for verbose debug lines.
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "[$(timestamp)] $1" >> "$LOG_FILE"
}

trace() {
    # Low-level activity stream: emits one line per thing the monitor does.
    # Writes to stderr so that $(spark_sql ...) captures in subshells don't
    # accidentally swallow trace lines as data.
    echo "[$(date '+%H:%M:%S')] · $1" >&2
}

tick() {
    # Between-cycle heartbeat. Shows countdown to next cycle so the monitor
    # never looks idle on the terminal. Also to stderr for consistency.
    local secs_remaining=$1
    echo "[$(date '+%H:%M:%S')] · idle — next cycle in ${secs_remaining}s" >&2
}

result() {
    # Pretty-prints a check outcome + bumps counters.
    # Usage: result <name> <PASS|WARN|FAIL|SKIP> <detail>
    local name=$1 status=$2 detail=$3
    local line
    line=$(printf "  %-28s [%s] %s" "$name" "$status" "$detail")
    echo "$line"
    log_quiet "$line"
    case $status in
        PASS) ((PASS_COUNT++)) ;;
        FAIL) ((FAIL_COUNT++)) ;;
        WARN) ((WARN_COUNT++)) ;;
    esac
}

log_breach() {
    local sla_name=$1 actual=$2 threshold=$3
    local msg="[$(timestamp)] BREACH: $sla_name — actual=$actual threshold=$threshold"
    mkdir -p "$(dirname "$BREACH_LOG")"
    echo "$msg" >> "$BREACH_LOG"
    log_quiet "$msg"
}

log_alert() {
    local msg="[$(timestamp)] ALERT: $1"
    mkdir -p "$(dirname "$ALERT_LOG")"
    echo "$msg" >> "$ALERT_LOG"
    log "$msg"
}

section() {
    echo ""
    echo "$1"
    trace "check: $1"
}

# --- Data access ------------------------------------------------------------

spark_sql() {
    # Run a SQL statement against spark-unified's writer session. Always fresh
    # (writer invalidates its own catalog cache on commit).
    local preview=${1//$'\n'/ }
    preview=${preview:0:80}
    trace "spark-sql: ${preview}"
    docker exec "$SPARK_SQL_CONTAINER" /opt/spark/bin/spark-sql \
        --conf spark.sql.defaultCatalog=atmosphere \
        -e "$1" 2>/dev/null | grep -v "^$" | tail -n +1
}

spark_sql_scalar() {
    # Run SQL and strip whitespace from the result. Returns "-1" on failure.
    local val
    val=$(spark_sql "$1" 2>/dev/null | tr -d '[:space:]')
    [[ -z "$val" || "$val" == "NULL" ]] && { echo "-1"; return; }
    echo "$val"
}

query_api_get() {
    # GET against query-api. Usage: query_api_get <path> [curl args...]
    local path=$1; shift
    curl -sf --max-time 15 "$@" "${QUERY_API_URL}${path}"
}

query_api_sql_scalar() {
    # Run SQL via /api/sql and extract first column's first row. Returns "-1" on failure.
    local sql=$1
    local preview=${sql//$'\n'/ }
    preview=${preview:0:80}
    trace "query-api sql (scalar): ${preview}"
    local json
    json=$(curl -sfG --max-time 15 "${QUERY_API_URL}/api/sql?limit=1" --data-urlencode "q=$sql" 2>/dev/null) || { echo "-1"; return; }
    echo "$json" | python3 -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    rows = d.get('data', [])
    if not rows:
        print('-1')
    else:
        first = rows[0]
        v = next(iter(first.values()))
        print(v if v is not None else '-1')
except Exception:
    print('-1')
" 2>/dev/null || echo "-1"
}

query_api_sql_tsv() {
    # Run SQL via /api/sql and return all rows as tab-separated lines.
    # Column order matches the SELECT list. Returns empty string on failure.
    local sql=$1 limit=${2:-1000}
    local preview=${sql//$'\n'/ }
    preview=${preview:0:80}
    trace "query-api sql (tsv): ${preview}"
    local json
    json=$(curl -sfG --max-time 30 "${QUERY_API_URL}/api/sql?limit=${limit}" --data-urlencode "q=$sql" 2>/dev/null) || return
    echo "$json" | python3 -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    for row in d.get('data', []):
        print('\t'.join('' if v is None else str(v) for v in row.values()))
except Exception:
    pass
" 2>/dev/null
}

query_api_refresh() {
    # Invalidate query-api's catalog cache for the given tables. Called once
    # per cycle so subsequent /api/sql reads see fresh snapshots.
    local tables=("$@")
    (( ${#tables[@]} == 0 )) && return
    trace "query-api refresh: ${#tables[@]} tables"
    local json_tables
    json_tables=$(printf '"%s",' "${tables[@]}")
    json_tables="[${json_tables%,}]"
    curl -sf --max-time 10 -X POST "${QUERY_API_URL}/api/ops/refresh" \
        -H 'Content-Type: application/json' \
        -d "{\"tables\": ${json_tables}}" > /dev/null 2>&1 || trace "query-api refresh FAILED (continuing)"
}

# --- Docker helpers ---------------------------------------------------------

container_state() {
    docker compose ps --format '{{.State}}' "$1" 2>/dev/null
}

stack_is_down() {
    local any_running=false
    for service in "${SERVICES[@]}"; do
        if [[ "$(container_state "$service")" == "running" ]]; then
            any_running=true
            break
        fi
    done
    ! $any_running
}
