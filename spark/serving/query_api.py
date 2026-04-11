"""
query_api: REST API for serving Iceberg mart queries to Grafana.

Reads Iceberg tables via PySpark + Polaris REST catalog and serves
query results as JSON over HTTP. Grafana connects via the Infinity
datasource plugin.

Replaces the originally-planned Spark Thrift Server — no Hive
datasource plugin exists for Grafana.

Requirements: FR-19, NFR-04
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("query-api")

# SQL directory — mart queries are loaded from files
SQL_DIR = Path("/opt/spark/work-dir/spark/transforms/sql/mart")

# Cache for loaded SQL templates
_sql_cache: dict[str, str] = {}

# Global SparkSession reference
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


def load_sql(name: str) -> str:
    """Load a SQL query from the mart directory, with caching."""
    if name not in _sql_cache:
        # Try both naming conventions: mart_*.sql and v_*.sql
        for prefix in ["mart_", "v_"]:
            path = SQL_DIR / f"{prefix}{name}.sql"
            if path.exists():
                _sql_cache[name] = path.read_text()
                logger.info("Loaded SQL: %s", path.name)
                break
        else:
            # Try exact filename
            path = SQL_DIR / f"{name}.sql"
            if path.exists():
                _sql_cache[name] = path.read_text()
                logger.info("Loaded SQL: %s", path.name)
            else:
                raise FileNotFoundError(f"No SQL file found for: {name}")
    return _sql_cache[name]


def execute_query(sql: str, limit: int = 1000) -> list[dict]:
    """Execute a SQL query and return results as a list of dicts."""
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
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (int, float, str, bool)):
        return value
    return str(value)


# Available mart queries — maps endpoint names to SQL file names
MART_QUERIES = {
    # Materialized mart tables (updated each micro-batch)
    "events_per_second": "mart_events_per_second",
    "trending_hashtags": "mart_trending_hashtags",
    "engagement_velocity": "mart_engagement_velocity",
    "pipeline_health": "mart_pipeline_health",
    "sentiment_timeseries": "mart_sentiment_timeseries",
    # On-demand views
    "language_distribution": "v_language_distribution",
    "top_posts": "v_top_posts",
    "most_mentioned": "v_most_mentioned",
    "content_breakdown": "v_content_breakdown",
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
async def health():
    """Health check endpoint for Docker Compose."""
    try:
        s = get_spark()
        # Quick check that Spark is responsive
        s.sql("SELECT 1").collect()
        return {"status": "healthy", "spark": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/api/marts")
async def list_marts():
    """List all available mart query endpoints."""
    return {
        "marts": list(MART_QUERIES.keys()),
        "usage": "GET /api/mart/{name}?limit=1000",
    }


@app.get("/api/mart/{name}")
async def query_mart(
    name: str,
    limit: int = Query(default=1000, ge=1, le=10000),
):
    """Execute a mart query and return results as JSON.

    Grafana Infinity datasource calls this endpoint with JSONPath
    parsing to extract the `data` array.
    """
    if name not in MART_QUERIES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown mart: {name}. Available: {list(MART_QUERIES.keys())}",
        )

    sql_file = MART_QUERIES[name]
    try:
        sql = load_sql(sql_file)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e))

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


@app.get("/api/sql")
async def custom_query(
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("QUERY_API_PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
