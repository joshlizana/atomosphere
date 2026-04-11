"""
spark-staging: Structured Streaming job that reads raw_events from Iceberg,
routes events by collection, and writes to six staging tables.

Reads:  atmosphere.raw.raw_events (Iceberg readStream)
Writes: atmosphere.staging.stg_{posts,likes,reposts,follows,blocks,profiles}

Partitioning: days(event_time) for all staging tables (TDD §5.6)
Sort order:   event_time ASC

Requirements: FR-05, FR-10, FR-25, NFR-07, NFR-15
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-staging")

RAW_TABLE = "atmosphere.raw.raw_events"
CHECKPOINT = "/opt/spark/checkpoints/staging"
SQL_DIR = Path(os.path.dirname(__file__)) / "sql" / "staging"

# Collection → staging table mapping
COLLECTION_MAP = {
    "app.bsky.feed.post":      "atmosphere.staging.stg_posts",
    "app.bsky.feed.like":      "atmosphere.staging.stg_likes",
    "app.bsky.feed.repost":    "atmosphere.staging.stg_reposts",
    "app.bsky.graph.follow":   "atmosphere.staging.stg_follows",
    "app.bsky.graph.block":    "atmosphere.staging.stg_blocks",
    "app.bsky.actor.profile":  "atmosphere.staging.stg_profiles",
}

# Table DDL for CREATE TABLE IF NOT EXISTS (FR-25)
# All staging tables share envelope columns + collection-specific columns
CREATE_TABLE_SQLS = {
    "atmosphere.staging.stg_posts": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_posts (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            text            STRING,
            created_at      TIMESTAMP,
            langs           ARRAY<STRING>,
            has_embed       BOOLEAN,
            embed_type      STRING,
            is_reply        BOOLEAN,
            reply_root_uri  STRING,
            reply_parent_uri STRING,
            facets_json     STRING,
            labels_json     STRING,
            tags            ARRAY<STRING>
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    "atmosphere.staging.stg_likes": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_likes (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            subject_uri     STRING,
            subject_cid     STRING,
            created_at      TIMESTAMP,
            has_via         BOOLEAN
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    "atmosphere.staging.stg_reposts": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_reposts (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            subject_uri     STRING,
            subject_cid     STRING,
            created_at      TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    "atmosphere.staging.stg_follows": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_follows (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            subject_did     STRING,
            created_at      TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    "atmosphere.staging.stg_blocks": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_blocks (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            subject_did     STRING,
            created_at      TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    "atmosphere.staging.stg_profiles": """
        CREATE TABLE IF NOT EXISTS atmosphere.staging.stg_profiles (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            operation       STRING,
            display_name    STRING,
            description     STRING
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
}


def load_sql(name):
    """Load a SQL transform file and return its contents."""
    sql_path = SQL_DIR / f"{name}.sql"
    return sql_path.read_text()


def process_batch(spark, batch_df, batch_id):
    """Route a micro-batch of raw_events to staging tables by collection (FR-05, NFR-15).

    Uses foreachBatch to write to multiple Iceberg tables from a single stream.
    Each collection is filtered and transformed via its SQL file, then appended
    to the corresponding staging table.
    """
    if batch_df.isEmpty():
        return

    # Register the batch as a temp view for SQL transforms
    batch_df.createOrReplaceGlobalTempView("raw_batch")

    for collection, table in COLLECTION_MAP.items():
        sql_name = table.split(".")[-1]  # e.g., "stg_posts"
        sql_template = load_sql(sql_name)
        sql = sql_template.format(source="global_temp.raw_batch")

        result_df = spark.sql(sql)

        if not result_df.isEmpty():
            result_df.writeTo(table).append()
            logger.info(
                "Batch %d: wrote %d rows to %s",
                batch_id, result_df.count(), table,
            )


def start_queries(spark):
    """Set up staging DDL and start the streaming query.

    Returns a list of started StreamingQuery objects (does not block).
    Can be called from the unified entrypoint or from main() for standalone use.
    """
    # Create all staging tables on first run (FR-25)
    logger.info("Ensuring staging tables exist")
    for table, ddl in CREATE_TABLE_SQLS.items():
        spark.sql(ddl)
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY event_time ASC")
        logger.info("Table ready: %s", table)

    # Read raw_events as a streaming Iceberg source
    raw_stream = spark.readStream \
        .format("iceberg") \
        .load(RAW_TABLE)

    # Process each micro-batch: route by collection → staging tables
    query = raw_stream.writeStream \
        .foreachBatch(lambda df, batch_id: process_batch(spark, df, batch_id)) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", CHECKPOINT) \
        .start()

    logger.info("Staging streaming query started — reading from %s", RAW_TABLE)
    return [query]


def main():
    spark = SparkSession.builder \
        .appName("spark-staging") \
        .getOrCreate()

    queries = start_queries(spark)
    queries[0].awaitTermination()


if __name__ == "__main__":
    main()
