# monitor/lib/state.sh — SQLite schema + cursor management.
#
# Schema:
#   metrics       — every check emits rows here; flushed=0 until pushed to Iceberg
#   heal_history  — restart_service audit log, used for rate limiting

state_init() {
    mkdir -p "$(dirname "$METRICS_DB")"
    sqlite3 "$METRICS_DB" <<'SQL'
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS metrics (
    ts         INTEGER NOT NULL,
    cycle_id   INTEGER NOT NULL,
    check_name TEXT    NOT NULL,
    metric_key TEXT    NOT NULL,
    value_num  REAL,
    value_str  TEXT,
    status     TEXT    NOT NULL,
    service    TEXT,
    flushed    INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS metrics_ts      ON metrics(ts);
CREATE INDEX IF NOT EXISTS metrics_flushed ON metrics(flushed, ts);

CREATE TABLE IF NOT EXISTS heal_history (
    ts      INTEGER NOT NULL,
    service TEXT    NOT NULL,
    reason  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS heal_ts ON heal_history(service, ts);
SQL
}

state_prune() {
    # Delete already-flushed metrics older than 30 days.
    local cutoff
    cutoff=$(( $(date +%s) - 30*86400 ))
    sqlite3 "$METRICS_DB" "DELETE FROM metrics WHERE flushed=1 AND ts < $cutoff;" 2>/dev/null || true
    sqlite3 "$METRICS_DB" "DELETE FROM heal_history WHERE ts < $cutoff;" 2>/dev/null || true
}
