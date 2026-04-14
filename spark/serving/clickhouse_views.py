"""
ClickHouse mart-view bootstrap.

Creates the 10 mart aggregation views + pipeline_health view inside
ClickHouse's `atmosphere` database. Each view aggregates live over an
upstream Iceberg table read through the polaris_catalog DataLakeCatalog
engine, with the filesystem cache enabled per-view (transparent to all
consumers including Grafana).

Called from spark/unified.py after the core/sentiment streaming queries
have been started, which is when the underlying Iceberg tables are
guaranteed to exist in Polaris. A small retry loop tolerates the
DataLakeCatalog metadata cache lag between Spark CREATE TABLE and CH
visibility.

Idempotent — every view uses CREATE OR REPLACE.
"""

import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger("clickhouse-views")

VIEWS_DIR = Path(__file__).parent / "views"

# View files in dependency-friendly order: tables that always exist first,
# joins last. Order doesn't actually matter for CREATE OR REPLACE but it
# makes the bootstrap log easier to scan.
BASE_VIEWS = [
    "mart_events_per_second.sql",
    "mart_engagement_velocity.sql",
    "mart_sentiment_timeseries.sql",
    "mart_trending_hashtags.sql",
    "mart_most_mentioned.sql",
    "mart_language_distribution.sql",
    "mart_content_breakdown.sql",
    "mart_embed_usage.sql",
    "mart_firehose_stats.sql",
    "mart_top_posts.sql",
    "mart_pipeline_health.sql",
]

PANEL_VIEWS = [
    "panel_sentiment_timeseries.sql",
    "panel_current_sentiment.sql",
    "panel_top_posts_positive.sql",
    "panel_top_posts_negative.sql",
    "panel_events_per_second_by_collection.sql",
    "panel_operations_breakdown.sql",
    "panel_active_users.sql",
    "panel_total_events.sql",
    "panel_language_distribution.sql",
    "panel_content_breakdown.sql",
    "panel_embed_usage.sql",
    "panel_trending_hashtags.sql",
    "panel_likes_per_second.sql",
    "panel_reposts_per_second.sql",
    "panel_follows_per_second.sql",
    "panel_most_mentioned.sql",
    "panel_pipeline_health.sql",
]

VIEW_FILES = BASE_VIEWS + PANEL_VIEWS

DATABASE_NAME = "atmosphere"


def _ch_post(host, user, password, sql, timeout=30):
    return requests.post(
        host,
        params={"user": user, "password": password},
        data=sql,
        timeout=timeout,
    )


def _execute(host, user, password, sql, label, retries=12, delay=5):
    """Execute a CH statement with retry on transient catalog-cache misses.

    DataLakeCatalog lazily refreshes its view of Polaris metadata, so a
    CREATE VIEW issued moments after Spark registers a new table can fail
    with UNKNOWN_TABLE. Retry until visible or budget exhausted.
    """
    for attempt in range(1, retries + 1):
        resp = _ch_post(host, user, password, sql)
        if resp.status_code == 200:
            logger.info("  view ready: %s", label)
            return
        body = resp.text
        transient = (
            "UNKNOWN_TABLE" in body
            or "Unknown table expression identifier" in body
            or "doesn't exist" in body
        )
        if transient and attempt < retries:
            logger.info(
                "  %s not visible yet (attempt %d/%d) — retrying in %ss",
                label, attempt, retries, delay,
            )
            time.sleep(delay)
            continue
        logger.error("  failed to create %s (HTTP %d): %s", label, resp.status_code, body[:500])
        raise RuntimeError(f"ClickHouse view creation failed: {label}")


def create_all():
    """Create the `atmosphere` database and all mart views in ClickHouse."""
    host = os.environ["CLICKHOUSE_HOST"]
    user = os.environ["CLICKHOUSE_ADMIN_USER"]
    password = os.environ["CLICKHOUSE_ADMIN_PASSWORD"]

    logger.info("Creating ClickHouse database '%s'", DATABASE_NAME)
    _execute(host, user, password, f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}", DATABASE_NAME)

    logger.info("Creating %d ClickHouse mart views", len(VIEW_FILES))
    for filename in VIEW_FILES:
        sql = (VIEWS_DIR / filename).read_text()
        view_name = filename.removesuffix(".sql")
        _execute(host, user, password, sql, view_name)

    logger.info("All ClickHouse mart views created")
