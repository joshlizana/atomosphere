"""
maintenance: Iceberg table stats + on-demand compaction.

Two responsibilities:
  1. collect_table_stats(spark) — read .files metadata for all atmosphere
     tables in parallel and evaluate the compaction threshold per table.
  2. compact_table(spark, table) — run rewrite_data_files + expire_snapshots
     against a single table, with an in-memory per-table rate limit.

Threshold (approved 2026-04-12):
  needs_compaction = file_count > 500 OR avg_file_kb < 30

Rate limit (approved 2026-04-12):
  10 minutes per table. Compact requests for a table that ran less than
  10 minutes ago are reported as 'rate_limited' and skipped.

Snapshot retention: 1 day (matches the original scripts/maintain.sh).

Used by query_api.py to back the /api/maintenance/{table-stats,run} endpoints.
The compaction itself runs inside query-api's own SparkSession — there's no
subprocess or docker-exec round-trip. FAIR scheduling lets ad-hoc dashboard
queries continue running while a rewrite is in progress.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

from pyspark.sql import SparkSession

logger = logging.getLogger("maintenance")

# All atmosphere.* tables eligible for maintenance, in dependency order.
TABLES: list[str] = [
    "atmosphere.raw.raw_events",
    "atmosphere.staging.stg_posts",
    "atmosphere.staging.stg_likes",
    "atmosphere.staging.stg_reposts",
    "atmosphere.staging.stg_follows",
    "atmosphere.staging.stg_blocks",
    "atmosphere.staging.stg_profiles",
    "atmosphere.core.core_posts",
    "atmosphere.core.core_mentions",
    "atmosphere.core.core_hashtags",
    "atmosphere.core.core_engagement",
    "atmosphere.core.core_post_sentiment",
    "atmosphere.mart.mart_events_per_second",
    "atmosphere.mart.mart_engagement_velocity",
    "atmosphere.mart.mart_sentiment_timeseries",
    "atmosphere.mart.mart_trending_hashtags",
    "atmosphere.mart.mart_most_mentioned",
    "atmosphere.mart.mart_language_distribution",
    "atmosphere.mart.mart_content_breakdown",
    "atmosphere.mart.mart_embed_usage",
    "atmosphere.mart.mart_top_posts",
    "atmosphere.mart.mart_firehose_stats",
]

# Compaction threshold (approved)
THRESHOLD_FILE_COUNT = 500
THRESHOLD_AVG_FILE_KB = 30.0

# Rate limit window (approved)
RATE_LIMIT = timedelta(minutes=10)

# Snapshot retention for expire_snapshots
SNAPSHOT_RETAIN_DAYS = 1

# Parallelism for stats collection
MAX_WORKERS = 12

# In-memory per-table last-compacted timestamps. Process-local; resets when
# query-api restarts. That's acceptable: a fresh process compacting once on
# startup is harmless.
_last_compacted: dict[str, datetime] = {}
_last_compacted_lock = Lock()


def _evaluate_threshold(file_count: int, avg_file_kb: float) -> tuple[bool, str | None]:
    """Apply the compaction threshold to a single table's stats."""
    reasons: list[str] = []
    if file_count > THRESHOLD_FILE_COUNT:
        reasons.append(f"file_count={file_count}>{THRESHOLD_FILE_COUNT}")
    if 0 < avg_file_kb < THRESHOLD_AVG_FILE_KB:
        reasons.append(f"avg_file_kb={avg_file_kb:.1f}<{THRESHOLD_AVG_FILE_KB}")
    if reasons:
        return True, "; ".join(reasons)
    return False, None


def _collect_one(spark: SparkSession, table: str) -> dict[str, Any]:
    """Read .files metadata for a single table and evaluate the threshold."""
    try:
        row = spark.sql(
            f"SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes "
            f"FROM {table}.files"
        ).collect()[0]
        file_count = int(row["file_count"])
        total_bytes = int(row["total_bytes"])
        avg_file_kb = (total_bytes / file_count / 1024.0) if file_count > 0 else 0.0
        needs, reason = _evaluate_threshold(file_count, avg_file_kb)

        with _last_compacted_lock:
            last = _last_compacted.get(table)
        rate_limited = False
        if last is not None and datetime.now(timezone.utc) - last < RATE_LIMIT:
            rate_limited = True

        return {
            "table": table,
            "file_count": file_count,
            "total_bytes": total_bytes,
            "avg_file_kb": round(avg_file_kb, 2),
            "needs_compaction": needs,
            "reason": reason,
            "last_compacted_at": last.isoformat() if last else None,
            "rate_limited": rate_limited,
        }
    except Exception as e:
        logger.exception("Failed to collect stats for %s", table)
        return {"table": table, "error": f"{type(e).__name__}: {e}"}


def collect_table_stats(spark: SparkSession) -> dict[str, Any]:
    """Collect file stats for every atmosphere.* table in parallel.

    Returns a dict with `collected_at` and a per-table list. Each entry is
    safe to JSON-serialize.
    """
    collected_at = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_collect_one, spark, t): t for t in TABLES}
        for fut in as_completed(futures):
            results.append(fut.result())
    # Stable order matches TABLES so dashboards / scripts get a predictable layout
    by_name = {r["table"]: r for r in results}
    ordered = [by_name[t] for t in TABLES if t in by_name]
    return {"collected_at": collected_at, "tables": ordered}


def compact_table(spark: SparkSession, table: str) -> dict[str, Any]:
    """Run rewrite_data_files + expire_snapshots on a single table.

    Honors the per-table rate limit. Returns a dict describing what happened
    so the caller (HTTP endpoint, monitor.sh) can render a result.
    """
    if table not in TABLES:
        return {"table": table, "status": "error", "error": "unknown table"}

    with _last_compacted_lock:
        last = _last_compacted.get(table)
        if last is not None and datetime.now(timezone.utc) - last < RATE_LIMIT:
            return {
                "table": table,
                "status": "rate_limited",
                "last_compacted_at": last.isoformat(),
                "next_eligible_at": (last + RATE_LIMIT).isoformat(),
            }

    expire_before = (datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETAIN_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    started_at = datetime.now(timezone.utc)

    try:
        before = spark.sql(
            f"SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes "
            f"FROM {table}.files"
        ).collect()[0]

        # min-input-files=2: default is 5, which means small mart tables
        #   (often 2–4 files) would never be compacted. We need them rewritten.
        # partial-progress.enabled=true: lets a single failed file group
        #   commit independently instead of rolling back the whole job — safer
        #   on the big raw/staging tables.
        # max-concurrent-file-group-rewrites=1: default is 5. On tables with
        #   an active streaming writer (raw_events, stg_*, core_*), 5 parallel
        #   group commits race each other AND the streaming micro-batch commit
        #   for the same `main` branch CAS, producing frequent
        #   CommitFailedException. Serializing group commits removes intra-job
        #   contention; only the 5s streaming commit still competes, and that
        #   one either wins cleanly or the rewrite retries per partial-progress.
        logger.info("Compacting %s (rewrite_data_files)", table)
        rewrite_row = spark.sql(
            f"CALL atmosphere.system.rewrite_data_files("
            f"table => '{table}', "
            f"options => map("
            f"'min-input-files', '2', "
            f"'partial-progress.enabled', 'true', "
            f"'max-concurrent-file-group-rewrites', '1'))"
        ).collect()[0]

        logger.info("Expiring snapshots for %s older than %s", table, expire_before)
        expire_row = spark.sql(
            f"CALL atmosphere.system.expire_snapshots("
            f"table => '{table}', older_than => TIMESTAMP '{expire_before}', "
            f"stream_results => true)"
        ).collect()[0]

        after = spark.sql(
            f"SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes "
            f"FROM {table}.files"
        ).collect()[0]

        finished_at = datetime.now(timezone.utc)
        with _last_compacted_lock:
            _last_compacted[table] = finished_at

        return {
            "table": table,
            "status": "ok",
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "elapsed_seconds": round((finished_at - started_at).total_seconds(), 2),
            "files_before": int(before["file_count"]),
            "files_after": int(after["file_count"]),
            "bytes_before": int(before["total_bytes"]),
            "bytes_after": int(after["total_bytes"]),
            "rewrite": {
                "rewritten_data_files_count": int(rewrite_row["rewritten_data_files_count"]),
                "added_data_files_count":     int(rewrite_row["added_data_files_count"]),
                "rewritten_bytes_count":      int(rewrite_row["rewritten_bytes_count"]),
                "failed_data_files_count":    int(rewrite_row["failed_data_files_count"]),
                "removed_delete_files_count": int(rewrite_row["removed_delete_files_count"]),
            },
            "expire": {
                "deleted_data_files_count":             int(expire_row["deleted_data_files_count"]),
                "deleted_manifest_files_count":         int(expire_row["deleted_manifest_files_count"]),
                "deleted_manifest_lists_count":         int(expire_row["deleted_manifest_lists_count"]),
                "deleted_position_delete_files_count":  int(expire_row["deleted_position_delete_files_count"]),
                "deleted_equality_delete_files_count":  int(expire_row["deleted_equality_delete_files_count"]),
                "deleted_statistics_files_count":       int(expire_row["deleted_statistics_files_count"]),
            },
        }
    except Exception as e:
        logger.exception("Compaction failed for %s", table)
        return {
            "table": table,
            "status": "error",
            "error": f"{type(e).__name__}: {e}",
            "started_at": started_at.isoformat(),
        }


def run_maintenance(
    spark: SparkSession,
    tables: list[str] | None = None,
    progress_cb=None,
) -> dict[str, Any]:
    """Run compaction across a set of tables.

    If `tables` is None, picks every table where collect_table_stats reports
    needs_compaction=True (and is not rate-limited). Otherwise compacts the
    explicitly named tables.

    progress_cb(stage, done, total) is called as each unit completes.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    if tables is None:
        stats = collect_table_stats(spark)
        targets = [
            t["table"] for t in stats["tables"]
            if t.get("needs_compaction") and not t.get("rate_limited") and "error" not in t
        ]
        skipped_rate_limited = [
            t["table"] for t in stats["tables"]
            if t.get("needs_compaction") and t.get("rate_limited")
        ]
    else:
        targets = list(tables)
        skipped_rate_limited = []

    total = len(targets)
    if progress_cb:
        progress_cb("compact", 0, total)

    results: list[dict[str, Any]] = []
    # Compaction is sequential per table — concurrent rewrites on the same
    # warehouse can fight for files. Cheap to do serially since each table
    # rewrite is fast on the small mart tables and bounded on the big ones.
    for i, table in enumerate(targets, start=1):
        results.append(compact_table(spark, table))
        if progress_cb:
            progress_cb("compact", i, total)

    return {
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "targeted_tables": targets,
        "skipped_rate_limited": skipped_rate_limited,
        "results": results,
    }
