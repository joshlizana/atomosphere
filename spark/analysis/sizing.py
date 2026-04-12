"""
sizing: Iceberg table sizing and growth-projection analysis.

Collects per-table metrics from Iceberg metadata tables (snapshots, files,
partitions) and projects growth based on observed ingestion rate. Used to
inform the materialized mart design — we need to know how much data each
mart query touches before deciding which patterns to materialize and at
what cadence.

The collection runs across all known atmosphere.* tables in parallel via
ThreadPoolExecutor — Spark's FAIR scheduler interleaves the underlying
jobs across executor slots. Each metric is gathered with one cold-cache
read and three warm-cache reads so we can report both first-call latency
(realistic for dashboard cold start) and steady-state latency.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Callable

from pyspark.sql import SparkSession

logger = logging.getLogger("sizing")

# Tables to profile, grouped by namespace.
TABLES: dict[str, list[str]] = {
    "raw": ["raw_events"],
    "staging": [
        "stg_posts",
        "stg_likes",
        "stg_reposts",
        "stg_follows",
        "stg_blocks",
        "stg_profiles",
    ],
    "core": [
        "core_posts",
        "core_mentions",
        "core_hashtags",
        "core_engagement",
        "core_post_sentiment",
    ],
}

# Mart SQL files mapped to the tables they touch — used to estimate the
# read cost of each dashboard panel under the current view-based design.
MART_QUERIES: dict[str, str] = {
    "events_per_second": "mart_events_per_second",
    "trending_hashtags": "mart_trending_hashtags",
    "engagement_velocity": "mart_engagement_velocity",
    "pipeline_health": "mart_pipeline_health",
    "sentiment_timeseries": "mart_sentiment_timeseries",
    "language_distribution": "v_language_distribution",
    "top_posts": "v_top_posts",
    "most_mentioned": "v_most_mentioned",
    "content_breakdown": "v_content_breakdown",
    "embed_usage": "v_embed_usage",
}

WARMUP_PASSES = 1
TIMED_PASSES = 3
MAX_WORKERS = 20


def _fqn(namespace: str, table: str) -> str:
    return f"atmosphere.{namespace}.{table}"


def _time_call(fn: Callable[[], Any]) -> tuple[Any, float]:
    """Run fn() once and return (result, elapsed_seconds)."""
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def _measure(fn: Callable[[], Any]) -> dict[str, Any]:
    """Run fn() once cold and TIMED_PASSES warm; report both latencies."""
    timings: list[float] = []
    cold_value, cold_elapsed = _time_call(fn)
    timings.append(cold_elapsed)
    warm_timings: list[float] = []
    last_value = cold_value
    for _ in range(TIMED_PASSES):
        last_value, elapsed = _time_call(fn)
        warm_timings.append(elapsed)
    return {
        "value": last_value,
        "cold_seconds": round(cold_elapsed, 4),
        "warm_seconds_avg": round(sum(warm_timings) / len(warm_timings), 4),
        "warm_seconds_min": round(min(warm_timings), 4),
        "warm_seconds_max": round(max(warm_timings), 4),
    }


# ----- Per-table collectors --------------------------------------------------


def collect_row_count(spark: SparkSession, fqn: str) -> dict[str, Any]:
    return _measure(lambda: spark.sql(f"SELECT count(*) AS c FROM {fqn}").collect()[0]["c"])


def collect_files(spark: SparkSession, fqn: str) -> dict[str, Any]:
    """Iceberg .files metadata table — file count + total bytes + record count."""

    def _q():
        row = spark.sql(
            f"SELECT count(*) AS file_count, "
            f"coalesce(sum(file_size_in_bytes), 0) AS total_bytes, "
            f"coalesce(sum(record_count), 0) AS record_count "
            f"FROM {fqn}.files"
        ).collect()[0]
        return {
            "file_count": int(row["file_count"]),
            "total_bytes": int(row["total_bytes"]),
            "record_count": int(row["record_count"]),
        }

    return _measure(_q)


def collect_partitions(spark: SparkSession, fqn: str) -> dict[str, Any]:
    """Iceberg .partitions metadata table — per-partition rollup."""

    def _q():
        rows = spark.sql(
            f"SELECT partition, record_count, file_count "
            f"FROM {fqn}.partitions ORDER BY partition"
        ).collect()
        return [
            {
                "partition": str(r["partition"]),
                "record_count": int(r["record_count"]),
                "file_count": int(r["file_count"]),
            }
            for r in rows
        ]

    return _measure(_q)


def collect_snapshots(spark: SparkSession, fqn: str) -> dict[str, Any]:
    """Iceberg .snapshots history — committed_at + summary metrics."""

    def _q():
        rows = spark.sql(
            f"SELECT committed_at, snapshot_id, operation, summary "
            f"FROM {fqn}.snapshots ORDER BY committed_at"
        ).collect()
        out = []
        for r in rows:
            summary = dict(r["summary"]) if r["summary"] else {}
            out.append(
                {
                    "committed_at": r["committed_at"].isoformat() if r["committed_at"] else None,
                    "snapshot_id": int(r["snapshot_id"]),
                    "operation": r["operation"],
                    "added_records": int(summary.get("added-records", 0) or 0),
                    "added_files_size": int(summary.get("added-files-size", 0) or 0),
                    "total_records": int(summary.get("total-records", 0) or 0),
                    "total_files_size": int(summary.get("total-files-size", 0) or 0),
                }
            )
        return out

    return _measure(_q)


def project_growth(snapshots_value: list[dict[str, Any]]) -> dict[str, Any]:
    """Project record-count and byte growth from snapshot history.

    Uses (last total - first total) / elapsed_seconds for both records and
    bytes, then scales to per-minute / per-hour / per-day.
    """
    if len(snapshots_value) < 2:
        return {"insufficient_history": True, "snapshot_count": len(snapshots_value)}

    first = snapshots_value[0]
    last = snapshots_value[-1]
    if not first["committed_at"] or not last["committed_at"]:
        return {"missing_timestamps": True}

    t0 = datetime.fromisoformat(first["committed_at"])
    t1 = datetime.fromisoformat(last["committed_at"])
    elapsed_seconds = (t1 - t0).total_seconds()
    if elapsed_seconds <= 0:
        return {"zero_elapsed": True}

    rec_delta = last["total_records"] - first["total_records"]
    bytes_delta = last["total_files_size"] - first["total_files_size"]
    rec_per_sec = rec_delta / elapsed_seconds
    bytes_per_sec = bytes_delta / elapsed_seconds

    return {
        "snapshot_count": len(snapshots_value),
        "history_span_seconds": round(elapsed_seconds, 1),
        "history_span_hours": round(elapsed_seconds / 3600, 3),
        "records_added": rec_delta,
        "records_per_second": round(rec_per_sec, 2),
        "records_per_minute": int(rec_per_sec * 60),
        "records_per_hour": int(rec_per_sec * 3600),
        "records_per_day": int(rec_per_sec * 86400),
        "bytes_added": bytes_delta,
        "bytes_per_second": int(bytes_per_sec),
        "mb_per_hour": round(bytes_per_sec * 3600 / (1024 * 1024), 2),
        "gb_per_day": round(bytes_per_sec * 86400 / (1024 * 1024 * 1024), 3),
    }


def profile_table(spark: SparkSession, namespace: str, table: str) -> dict[str, Any]:
    fqn = _fqn(namespace, table)
    logger.info("Profiling %s", fqn)
    out: dict[str, Any] = {"table": fqn}
    try:
        out["row_count"] = collect_row_count(spark, fqn)
        out["files"] = collect_files(spark, fqn)
        out["partitions"] = collect_partitions(spark, fqn)
        out["snapshots"] = collect_snapshots(spark, fqn)
        out["growth_projection"] = project_growth(out["snapshots"]["value"])
    except Exception as e:
        logger.exception("Failed to profile %s", fqn)
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ----- Per-mart-query collector ----------------------------------------------


def _load_mart_sql(sql_name: str) -> str:
    """Load a mart SQL file from the standard location."""
    from pathlib import Path

    sql_dir = Path("/opt/spark/work-dir/spark/transforms/sql/mart")
    path = sql_dir / f"{sql_name}.sql"
    return path.read_text()


def profile_mart_query(spark: SparkSession, name: str, sql_name: str) -> dict[str, Any]:
    logger.info("Profiling mart query %s (%s)", name, sql_name)
    out: dict[str, Any] = {"name": name, "sql_file": sql_name}
    try:
        sql = _load_mart_sql(sql_name)

        def _q():
            df = spark.sql(sql)
            rows = df.collect()
            return {"row_count": len(rows), "column_count": len(df.columns)}

        out["latency"] = _measure(_q)
    except Exception as e:
        logger.exception("Failed to profile mart query %s", name)
        out["error"] = f"{type(e).__name__}: {e}"
    return out


# ----- Top-level orchestration ------------------------------------------------


def run_sizing(spark: SparkSession, progress_cb: Callable[[str, int, int], None] | None = None) -> dict[str, Any]:
    """Run the full sizing analysis. Returns a nested dict by category.

    progress_cb(stage, done, total) is called as each unit completes so the
    HTTP endpoint can expose live progress.
    """
    started_at = datetime.now(timezone.utc).isoformat()
    result: dict[str, Any] = {
        "started_at": started_at,
        "config": {
            "warmup_passes": WARMUP_PASSES,
            "timed_passes": TIMED_PASSES,
            "max_workers": MAX_WORKERS,
        },
        "tables": {},
        "mart_queries": {},
    }

    # Phase 1: profile every table in parallel
    table_units = [(ns, t) for ns, ts in TABLES.items() for t in ts]
    total_tables = len(table_units)
    done_tables = 0
    if progress_cb:
        progress_cb("tables", 0, total_tables)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(profile_table, spark, ns, t): (ns, t) for ns, t in table_units}
        for fut in as_completed(futures):
            ns, t = futures[fut]
            result["tables"].setdefault(ns, {})[t] = fut.result()
            done_tables += 1
            if progress_cb:
                progress_cb("tables", done_tables, total_tables)

    # Phase 2: profile every mart query in parallel
    total_marts = len(MART_QUERIES)
    done_marts = 0
    if progress_cb:
        progress_cb("mart_queries", 0, total_marts)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(profile_mart_query, spark, name, sql_name): name
            for name, sql_name in MART_QUERIES.items()
        }
        for fut in as_completed(futures):
            name = futures[fut]
            result["mart_queries"][name] = fut.result()
            done_marts += 1
            if progress_cb:
                progress_cb("mart_queries", done_marts, total_marts)

    # Phase 3: rollup totals
    total_bytes = 0
    total_records = 0
    total_files = 0
    for ns_tables in result["tables"].values():
        for tinfo in ns_tables.values():
            f = tinfo.get("files", {}).get("value")
            if f:
                total_bytes += f["total_bytes"]
                total_records += f["record_count"]
                total_files += f["file_count"]
    result["totals"] = {
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2),
        "total_gb": round(total_bytes / (1024 * 1024 * 1024), 3),
        "total_records": total_records,
        "total_files": total_files,
    }

    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    return result
