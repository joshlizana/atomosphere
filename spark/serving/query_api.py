"""
query_api: REST API for serving Iceberg mart queries to Grafana.

Reads Iceberg tables via PySpark + Polaris REST catalog and serves
query results as JSON over HTTP. Grafana connects via the Infinity
datasource plugin.

Replaces the originally-planned Spark Thrift Server — no Hive
datasource plugin exists for Grafana.

Requirements: FR-19, NFR-04
"""

import json
import logging
import os
import time
import uuid
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, TimestampType, LongType, StringType, DoubleType,
)

from spark.analysis.sizing import run_sizing
from spark.analysis.maintenance import collect_table_stats, run_maintenance

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("query-api")

# SQL directory — mart queries are loaded from files
SQL_DIR = Path("/opt/spark/work-dir/spark/transforms/sql/mart")

# Cache for loaded SQL templates (read-once, immutable after warmup)
_sql_cache: dict[str, str] = {}

# Global SparkSession reference. SparkSession is thread-safe; concurrent
# requests submit jobs to a shared SparkContext under FAIR scheduling.
spark: SparkSession = None


def get_spark() -> SparkSession:
    """Get or create the SparkSession."""
    global spark
    if spark is None or spark._jsc.sc().isStopped():
        logger.info("Creating SparkSession for query-api")
        spark = SparkSession.builder \
            .appName("query-api") \
            .getOrCreate()
        logger.info("SparkSession created")
    return spark


def load_sql(filename: str) -> str:
    """Load a SQL file from SQL_DIR by exact filename, with caching."""
    if filename not in _sql_cache:
        path = SQL_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"No SQL file found: {path}")
        _sql_cache[filename] = path.read_text()
        logger.info("Loaded SQL: %s", path.name)
    return _sql_cache[filename]


def execute_query(sql: str, limit: int = 1000) -> list[dict]:
    """Execute a SQL query and return results as a list of dicts.

    Called from FastAPI's threadpool — concurrent calls submit Spark jobs
    from different threads, which interleave under FAIR scheduling.
    """
    s = get_spark()
    df = s.sql(sql)
    if limit:
        df = df.limit(limit)
    rows = df.collect()
    columns = df.columns
    return [{col: _serialize(row[col]) for col in columns} for row in rows]


def _serialize(value):
    """Convert PySpark values to JSON-safe types."""
    if value is None:
        return None
    import datetime
    from decimal import Decimal
    if isinstance(value, Decimal):
        # Spark DECIMAL → JSON number (not string) so Grafana can plot it
        return float(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# Available mart queries — endpoint name → (sql_filename, source_table)
# `source_table` is substituted into the SQL's {source} placeholder at runtime.
# pipeline_health is special: it's an unmaterialized view registered in core.py,
# so its SQL file has no {source} placeholder and source_table is unused.
MART_QUERIES: dict[str, tuple[str, str]] = {
    "events_per_second":     ("read_events_per_second.sql",     "atmosphere.mart.mart_events_per_second"),
    "engagement_velocity":   ("read_engagement_velocity.sql",   "atmosphere.mart.mart_engagement_velocity"),
    "sentiment_timeseries":  ("read_sentiment_timeseries.sql",  "atmosphere.mart.mart_sentiment_timeseries"),
    "trending_hashtags":     ("read_trending_hashtags.sql",     "atmosphere.mart.mart_trending_hashtags"),
    "most_mentioned":        ("read_most_mentioned.sql",        "atmosphere.mart.mart_most_mentioned"),
    "language_distribution": ("read_language_distribution.sql", "atmosphere.mart.mart_language_distribution"),
    "content_breakdown":     ("read_content_breakdown.sql",     "atmosphere.mart.mart_content_breakdown"),
    "embed_usage":           ("read_embed_usage.sql",           "atmosphere.mart.mart_embed_usage"),
    "top_posts":             ("read_top_posts.sql",             "atmosphere.mart.mart_top_posts"),
    "firehose_stats":        ("read_firehose_stats.sql",        "atmosphere.mart.mart_firehose_stats"),
    "pipeline_health":       ("mart_pipeline_health.sql",       ""),
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize SparkSession on startup, stop on shutdown."""
    logger.info("Starting query-api — initializing SparkSession")
    get_spark()
    logger.info("query-api ready")
    yield
    logger.info("Shutting down query-api")
    if spark is not None:
        spark.stop()


app = FastAPI(
    title="Atmosphere Query API",
    description="REST API serving Iceberg mart queries for Grafana",
    lifespan=lifespan,
)


@app.get("/health")
def health():
    """Health check endpoint for Docker Compose."""
    try:
        s = get_spark()
        s.sql("SELECT 1").collect()
        return {"status": "healthy", "spark": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/marts")
def list_marts():
    """List all available mart query endpoints."""
    return {
        "marts": list(MART_QUERIES.keys()),
        "usage": "GET /api/mart/{name}?limit=1000",
    }


@app.get("/api/mart/{name}")
def query_mart(
    name: str,
    limit: int = Query(default=1000, ge=1, le=10000),
    window: int = Query(default=5, ge=1, le=1440, description="Window size in minutes (only honored by trending_hashtags)"),
):
    """Execute a mart query and return results as JSON.

    Sync endpoint — FastAPI runs it in a threadpool so concurrent dashboard
    panel requests don't block the asyncio event loop.
    """
    if name not in MART_QUERIES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown mart: {name}. Available: {list(MART_QUERIES.keys())}",
        )

    sql_file, source_table = MART_QUERIES[name]
    try:
        sql = load_sql(sql_file)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Substitute {source} for materialized marts. pipeline_health has no
    # placeholder so the format() call is a no-op for it.
    fmt_kwargs: dict[str, object] = {}
    if "{source}" in sql:
        fmt_kwargs["source"] = source_table
    if "{window}" in sql:
        fmt_kwargs["window"] = window
    if fmt_kwargs:
        sql = sql.format(**fmt_kwargs)

    start = time.time()
    try:
        data = execute_query(sql, limit=limit)
    except Exception as e:
        logger.error("Query failed for %s: %s", name, e)
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    elapsed = round(time.time() - start, 3)
    logger.info("Query %s returned %d rows in %.3fs", name, len(data), elapsed)

    return {
        "mart": name,
        "row_count": len(data),
        "query_time_seconds": elapsed,
        "data": data,
    }


# --- Sizing analysis (async background job) ---------------------------------
#
# POST /api/analysis/sizing       → kicks off background job, returns job_id
# GET  /api/analysis/sizing/{id}  → poll status / fetch result
#
# Job store is an in-memory OrderedDict capped at SIZING_JOB_CAPACITY entries.
# Oldest entries evict on insert. Thread-safe via _sizing_lock.

SIZING_JOB_CAPACITY = 50
_sizing_jobs: "OrderedDict[str, dict]" = OrderedDict()
_sizing_lock = Lock()


def _sizing_set(job_id: str, **fields):
    with _sizing_lock:
        job = _sizing_jobs.get(job_id, {})
        job.update(fields)
        _sizing_jobs[job_id] = job
        _sizing_jobs.move_to_end(job_id)
        while len(_sizing_jobs) > SIZING_JOB_CAPACITY:
            _sizing_jobs.popitem(last=False)


def _sizing_get(job_id: str) -> dict | None:
    with _sizing_lock:
        return _sizing_jobs.get(job_id)


def _run_sizing_job(job_id: str):
    """Background task — runs the sizing analysis and stores result on the job."""
    logger.info("Sizing job %s starting", job_id)
    _sizing_set(job_id, status="running", started_at=datetime.now(timezone.utc).isoformat())

    def progress(stage: str, done: int, total: int):
        _sizing_set(job_id, progress={"stage": stage, "done": done, "total": total})

    try:
        result = run_sizing(get_spark(), progress_cb=progress)
        _sizing_set(
            job_id,
            status="done",
            finished_at=datetime.now(timezone.utc).isoformat(),
            result=result,
        )
        logger.info("Sizing job %s complete", job_id)
    except Exception as e:
        logger.exception("Sizing job %s failed", job_id)
        _sizing_set(
            job_id,
            status="error",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error=f"{type(e).__name__}: {e}",
        )


@app.post("/api/analysis/sizing")
def start_sizing(background_tasks: BackgroundTasks):
    """Start a sizing analysis job. Returns the job_id to poll."""
    job_id = str(uuid.uuid4())
    _sizing_set(job_id, status="queued", queued_at=datetime.now(timezone.utc).isoformat())
    background_tasks.add_task(_run_sizing_job, job_id)
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/analysis/sizing/{job_id}")
def get_sizing(job_id: str):
    """Fetch the status and (if complete) result of a sizing job."""
    job = _sizing_get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown sizing job: {job_id}")
    return {"job_id": job_id, **job}


# --- Maintenance (table stats + on-demand compaction) ----------------------
#
# GET  /api/maintenance/table-stats   → per-table file_count / avg_kb / needs_compaction
# POST /api/maintenance/run           → kick off compaction (background); returns job_id
#                                       optional JSON body: {"tables": ["fqn", ...]}
# GET  /api/maintenance/run/{job_id}  → poll status / fetch result
#
# Reuses the OrderedDict-cap pattern from sizing. Compaction runs inside this
# process's SparkSession via the maintenance module — no docker exec, no
# subprocess. Per-table 10-min rate limit lives in maintenance._last_compacted.

MAINT_JOB_CAPACITY = 50
_maint_jobs: "OrderedDict[str, dict]" = OrderedDict()
_maint_lock = Lock()


def _maint_set(job_id: str, **fields):
    with _maint_lock:
        job = _maint_jobs.get(job_id, {})
        job.update(fields)
        _maint_jobs[job_id] = job
        _maint_jobs.move_to_end(job_id)
        while len(_maint_jobs) > MAINT_JOB_CAPACITY:
            _maint_jobs.popitem(last=False)


def _maint_get(job_id: str) -> dict | None:
    with _maint_lock:
        return _maint_jobs.get(job_id)


def _run_maint_job(job_id: str, tables: list[str] | None):
    logger.info("Maintenance job %s starting (tables=%s)", job_id, tables or "auto")
    _maint_set(job_id, status="running", started_at=datetime.now(timezone.utc).isoformat())

    def progress(stage: str, done: int, total: int):
        _maint_set(job_id, progress={"stage": stage, "done": done, "total": total})

    try:
        result = run_maintenance(get_spark(), tables=tables, progress_cb=progress)
        _maint_set(
            job_id,
            status="done",
            finished_at=datetime.now(timezone.utc).isoformat(),
            result=result,
        )
        logger.info("Maintenance job %s complete", job_id)
    except Exception as e:
        logger.exception("Maintenance job %s failed", job_id)
        _maint_set(
            job_id,
            status="error",
            finished_at=datetime.now(timezone.utc).isoformat(),
            error=f"{type(e).__name__}: {e}",
        )


@app.get("/api/maintenance/table-stats")
def maintenance_table_stats():
    """Return per-table file stats and the threshold evaluation."""
    try:
        return collect_table_stats(get_spark())
    except Exception as e:
        logger.exception("table-stats failed")
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


@app.post("/api/maintenance/run")
def maintenance_run(
    background_tasks: BackgroundTasks,
    body: dict | None = None,
):
    """Start a compaction job. Returns job_id to poll.

    Optional JSON body `{"tables": ["atmosphere.x.y", ...]}` targets specific
    tables. Without a body, the job auto-selects every table where
    needs_compaction=true and rate_limited=false.
    """
    tables = (body or {}).get("tables") if body else None
    if tables is not None and not isinstance(tables, list):
        raise HTTPException(status_code=400, detail="'tables' must be a list of strings")

    job_id = str(uuid.uuid4())
    _maint_set(job_id, status="queued", queued_at=datetime.now(timezone.utc).isoformat())
    background_tasks.add_task(_run_maint_job, job_id, tables)
    return {"job_id": job_id, "status": "queued", "tables_requested": tables}


@app.get("/api/maintenance/run/{job_id}")
def maintenance_get(job_id: str):
    """Fetch status and (if complete) result of a maintenance job."""
    job = _maint_get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown maintenance job: {job_id}")
    return {"job_id": job_id, **job}


@app.get("/api/sql")
def custom_query(
    q: str = Query(..., description="SQL query to execute"),
    limit: int = Query(default=100, ge=1, le=10000),
):
    """Execute a custom SQL query (for ad-hoc exploration).

    Limited to SELECT statements only.
    """
    normalized = q.strip().upper()
    if not normalized.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="Only SELECT statements allowed")

    start = time.time()
    try:
        data = execute_query(q, limit=limit)
    except Exception as e:
        logger.error("Custom query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    elapsed = round(time.time() - start, 3)
    return {
        "row_count": len(data),
        "query_time_seconds": elapsed,
        "data": data,
    }


# --- Ops metrics ingestion (monitor.sh → Iceberg) --------------------------
#
# POST /api/ops/metrics  (Content-Type: application/x-ndjson)
#
# Body: one JSON object per line, schema:
#   { ts, cycle_id, check_name, metric_key, value_num, value_str, status, service }
#
# Used by scripts/monitor.sh to durably persist SQLite-buffered metrics into
# atmosphere.ops.monitor_metrics. The namespace + table are auto-created on
# first call so the monitor is self-bootstrapping.

_ops_bootstrapped = False
_ops_lock = Lock()

_METRICS_SCHEMA = StructType([
    StructField("ts",         TimestampType(), False),
    StructField("cycle_id",   LongType(),      False),
    StructField("check_name", StringType(),    False),
    StructField("metric_key", StringType(),    False),
    StructField("value_num",  DoubleType(),    True),
    StructField("value_str",  StringType(),    True),
    StructField("status",     StringType(),    False),
    StructField("service",    StringType(),    True),
])


def _bootstrap_ops_table():
    """Create atmosphere.ops namespace + monitor_metrics table if missing."""
    global _ops_bootstrapped
    if _ops_bootstrapped:
        return
    with _ops_lock:
        if _ops_bootstrapped:
            return
        s = get_spark()
        s.sql("CREATE NAMESPACE IF NOT EXISTS atmosphere.ops")
        s.sql("""
            CREATE TABLE IF NOT EXISTS atmosphere.ops.monitor_metrics (
                ts         TIMESTAMP,
                cycle_id   BIGINT,
                check_name STRING,
                metric_key STRING,
                value_num  DOUBLE,
                value_str  STRING,
                status     STRING,
                service    STRING
            ) USING iceberg
            PARTITIONED BY (days(ts))
        """)
        _ops_bootstrapped = True
        logger.info("ops.monitor_metrics table bootstrapped")


@app.post("/api/ops/refresh")
def refresh_tables(body: dict | None = None):
    """Invalidate query-api's Iceberg CachingCatalog entries for the given tables.

    Monitor calls this once per cycle before reading Iceberg state, so every
    subsequent /api/sql call in the cycle sees the latest snapshot regardless
    of the CachingCatalog TTL. Without a body, refreshes nothing (no-op).

    Body: {"tables": ["atmosphere.raw.raw_events", ...]}
    """
    tables = (body or {}).get("tables") or []
    if not isinstance(tables, list):
        raise HTTPException(status_code=400, detail="'tables' must be a list of strings")

    s = get_spark()
    refreshed = []
    errors = {}
    for t in tables:
        try:
            s.catalog.refreshTable(t)
            refreshed.append(t)
        except Exception as e:
            errors[t] = f"{type(e).__name__}: {e}"

    return {"refreshed": refreshed, "errors": errors}


@app.post("/api/ops/metrics")
async def ingest_metrics(request: Request):
    """Append a batch of monitor metric rows to atmosphere.ops.monitor_metrics.

    Accepts NDJSON (one JSON object per line). Each row is appended; the
    monitor's SQLite buffer is the source of truth until the append succeeds.
    """
    body = await request.body()
    if not body:
        return {"rows_inserted": 0}

    rows = []
    for i, line in enumerate(body.decode("utf-8").splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"line {i}: {e}")
        rows.append((
            datetime.fromtimestamp(int(obj["ts"]), tz=timezone.utc),
            int(obj["cycle_id"]),
            str(obj["check_name"]),
            str(obj["metric_key"]),
            float(obj["value_num"]) if obj.get("value_num") is not None else None,
            obj.get("value_str"),
            str(obj["status"]),
            obj.get("service"),
        ))

    if not rows:
        return {"rows_inserted": 0}

    _bootstrap_ops_table()
    s = get_spark()
    df = s.createDataFrame(rows, schema=_METRICS_SCHEMA)
    df.writeTo("atmosphere.ops.monitor_metrics").append()

    logger.info("ops metrics: appended %d rows", len(rows))
    return {"rows_inserted": len(rows)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("QUERY_API_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
