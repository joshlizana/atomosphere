#!/usr/bin/env bash
# maintain.sh — Iceberg table maintenance (snapshot expiry + compaction)
#
# Runs expire_snapshots and rewrite_data_files on all Atmosphere tables
# to prevent metadata bloat and small-file accumulation during long runs.
#
# Usage:
#   ./scripts/maintain.sh              # Run maintenance on all tables
#   ./scripts/maintain.sh --dry-run    # Show what would be done

set -uo pipefail

# --- Configuration ---
DRY_RUN=false
SPARK_SQL_CONTAINER="spark-core"
SNAPSHOT_RETAIN_DAYS=1
LOG_FILE="logs/maintain.log"

# All Iceberg tables in dependency order
TABLES=(
    atmosphere.raw.raw_events
    atmosphere.staging.stg_posts
    atmosphere.staging.stg_likes
    atmosphere.staging.stg_reposts
    atmosphere.staging.stg_follows
    atmosphere.staging.stg_blocks
    atmosphere.staging.stg_profiles
    atmosphere.core.core_posts
    atmosphere.core.core_mentions
    atmosphere.core.core_hashtags
    atmosphere.core.core_engagement
    atmosphere.core.core_post_sentiment
)

# --- Parse args ---
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN=true; shift ;;
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

# --- Pre-flight ---
state=$(docker compose ps --format '{{.State}}' "$SPARK_SQL_CONTAINER" 2>/dev/null || echo "")
if [[ "$state" != "running" ]]; then
    echo "ERROR: $SPARK_SQL_CONTAINER is not running. Start the stack first."
    exit 1
fi

# --- Maintenance ---
log "Starting Iceberg maintenance (dry-run=$DRY_RUN)"
echo ""

expire_ts=$(date -u -d "-${SNAPSHOT_RETAIN_DAYS} days" '+%Y-%m-%d %H:%M:%S')

for table in "${TABLES[@]}"; do
    short=${table##*.}
    echo "--- $short ---"

    # Get snapshot count before
    snap_count=$(spark_sql "SELECT COUNT(*) FROM $table.snapshots" 2>/dev/null | tr -d '[:space:]' || echo "?")
    file_count=$(spark_sql "SELECT COUNT(*) FROM $table.files" 2>/dev/null | tr -d '[:space:]' || echo "?")
    echo "  Before: $snap_count snapshots, $file_count data files"

    if $DRY_RUN; then
        echo "  [DRY-RUN] Would expire snapshots older than $expire_ts"
        echo "  [DRY-RUN] Would rewrite data files (compaction)"
    else
        # Expire old snapshots
        log "Expiring snapshots for $table older than $expire_ts"
        spark_sql "CALL atmosphere.system.expire_snapshots(table => '$table', older_than => TIMESTAMP '$expire_ts')" 2>/dev/null

        # Compact small files
        log "Compacting data files for $table"
        spark_sql "CALL atmosphere.system.rewrite_data_files(table => '$table')" 2>/dev/null

        # Get counts after
        snap_after=$(spark_sql "SELECT COUNT(*) FROM $table.snapshots" 2>/dev/null | tr -d '[:space:]' || echo "?")
        file_after=$(spark_sql "SELECT COUNT(*) FROM $table.files" 2>/dev/null | tr -d '[:space:]' || echo "?")
        echo "  After:  $snap_after snapshots, $file_after data files"
    fi
    echo ""
done

log "Maintenance complete"
