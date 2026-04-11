"""
spark-core: Structured Streaming job for core enrichment and mart views.

Reads:  atmosphere.staging.stg_{posts,likes,reposts}
Writes: atmosphere.core.{core_posts, core_mentions, core_hashtags, core_engagement}
Views:  atmosphere.mart.{mart_*, v_*}

Stream 1: stg_posts → core_posts + core_mentions + core_hashtags (foreachBatch)
Stream 2: stg_likes ∪ stg_reposts → core_engagement (direct write)

Requirements: FR-06–FR-09, FR-15–FR-18, FR-25
"""

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-core")

CHECKPOINT_BASE = "/opt/spark/checkpoints/core"
SQL_DIR = Path(os.path.dirname(__file__)) / "sql"

# --- Staging source tables ---
STG_POSTS = "atmosphere.staging.stg_posts"
STG_LIKES = "atmosphere.staging.stg_likes"
STG_REPOSTS = "atmosphere.staging.stg_reposts"

# --- Core target tables ---
CORE_POSTS = "atmosphere.core.core_posts"
CORE_MENTIONS = "atmosphere.core.core_mentions"
CORE_HASHTAGS = "atmosphere.core.core_hashtags"
CORE_ENGAGEMENT = "atmosphere.core.core_engagement"
CORE_POST_SENTIMENT = "atmosphere.core.core_post_sentiment"

# --- DDL for core tables (FR-25) ---
CREATE_CORE_TABLES = {
    CORE_POSTS: """
        CREATE TABLE IF NOT EXISTS atmosphere.core.core_posts (
            did             STRING      NOT NULL,
            time_us         BIGINT      NOT NULL,
            event_time      TIMESTAMP   NOT NULL,
            rkey            STRING,
            text            STRING,
            created_at      TIMESTAMP,
            primary_lang    STRING,
            is_reply        BOOLEAN,
            has_embed       BOOLEAN,
            embed_type      STRING,
            content_type    STRING,
            hashtags        ARRAY<STRING>,
            mention_dids    ARRAY<STRING>,
            link_urls       ARRAY<STRING>
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    CORE_MENTIONS: """
        CREATE TABLE IF NOT EXISTS atmosphere.core.core_mentions (
            author_did      STRING      NOT NULL,
            mentioned_did   STRING      NOT NULL,
            post_rkey       STRING,
            event_time      TIMESTAMP   NOT NULL
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    CORE_HASHTAGS: """
        CREATE TABLE IF NOT EXISTS atmosphere.core.core_hashtags (
            tag             STRING      NOT NULL,
            author_did      STRING      NOT NULL,
            post_rkey       STRING,
            event_time      TIMESTAMP   NOT NULL
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    CORE_ENGAGEMENT: """
        CREATE TABLE IF NOT EXISTS atmosphere.core.core_engagement (
            event_type      STRING      NOT NULL,
            actor_did       STRING      NOT NULL,
            subject_uri     STRING,
            event_time      TIMESTAMP   NOT NULL
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
    # Stub table for M5 (Sentiment) — created here so mart views can reference it
    CORE_POST_SENTIMENT: """
        CREATE TABLE IF NOT EXISTS atmosphere.core.core_post_sentiment (
            did                 STRING      NOT NULL,
            time_us             BIGINT      NOT NULL,
            event_time          TIMESTAMP   NOT NULL,
            rkey                STRING,
            text                STRING,
            sentiment_positive  DOUBLE,
            sentiment_negative  DOUBLE,
            sentiment_neutral   DOUBLE,
            sentiment_label     STRING,
            sentiment_confidence DOUBLE
        ) USING iceberg
        PARTITIONED BY (days(event_time))
    """,
}

# --- Mart views: catalog view name → SQL file path (relative to SQL_DIR) ---
MART_VIEWS = {
    "atmosphere.mart.mart_events_per_second":   "mart/mart_events_per_second.sql",
    "atmosphere.mart.mart_trending_hashtags":    "mart/mart_trending_hashtags.sql",
    "atmosphere.mart.mart_engagement_velocity":  "mart/mart_engagement_velocity.sql",
    "atmosphere.mart.mart_pipeline_health":      "mart/mart_pipeline_health.sql",
    "atmosphere.mart.mart_sentiment_timeseries": "mart/mart_sentiment_timeseries.sql",
    "atmosphere.mart.v_language_distribution":   "mart/v_language_distribution.sql",
    "atmosphere.mart.v_top_posts":              "mart/v_top_posts.sql",
    "atmosphere.mart.v_most_mentioned":         "mart/v_most_mentioned.sql",
    "atmosphere.mart.v_content_breakdown":      "mart/v_content_breakdown.sql",
}


def load_sql(relative_path):
    """Load a SQL file relative to SQL_DIR."""
    return (SQL_DIR / relative_path).read_text()


def process_posts_batch(spark, batch_df, batch_id):
    """Transform stg_posts micro-batch → core_posts, core_mentions, core_hashtags (FR-06–FR-08)."""
    if batch_df.isEmpty():
        return

    batch_df.createOrReplaceGlobalTempView("posts_batch")

    # core_posts — enriched posts with derived fields
    sql = load_sql("core/core_posts.sql").format(source="global_temp.posts_batch")
    result = spark.sql(sql)
    if not result.isEmpty():
        result.writeTo(CORE_POSTS).append()
        logger.info("Batch %d: wrote %d rows to %s", batch_id, result.count(), CORE_POSTS)

    # core_mentions — exploded mention edges
    sql = load_sql("core/core_mentions.sql").format(source="global_temp.posts_batch")
    result = spark.sql(sql)
    if not result.isEmpty():
        result.writeTo(CORE_MENTIONS).append()
        logger.info("Batch %d: wrote %d rows to %s", batch_id, result.count(), CORE_MENTIONS)

    # core_hashtags — exploded hashtag rows
    sql = load_sql("core/core_hashtags.sql").format(source="global_temp.posts_batch")
    result = spark.sql(sql)
    if not result.isEmpty():
        result.writeTo(CORE_HASHTAGS).append()
        logger.info("Batch %d: wrote %d rows to %s", batch_id, result.count(), CORE_HASHTAGS)


def main():
    spark = SparkSession.builder \
        .appName("spark-core") \
        .getOrCreate()

    # --- Ensure namespaces exist ---
    spark.sql("CREATE NAMESPACE IF NOT EXISTS atmosphere.core")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS atmosphere.mart")

    # --- Create core tables (FR-25) ---
    logger.info("Ensuring core tables exist")
    for table, ddl in CREATE_CORE_TABLES.items():
        spark.sql(ddl)
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY event_time ASC")
        logger.info("Table ready: %s", table)

    # --- Register mart views (FR-15–FR-18) ---
    logger.info("Registering mart views")
    for view_name, sql_path in MART_VIEWS.items():
        sql = load_sql(sql_path)
        spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {sql}")
        logger.info("View ready: %s", view_name)

    # --- Stream 1: stg_posts → core_posts + core_mentions + core_hashtags ---
    posts_stream = spark.readStream \
        .format("iceberg") \
        .load(STG_POSTS)

    posts_query = posts_stream.writeStream \
        .foreachBatch(lambda df, bid: process_posts_batch(spark, df, bid)) \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/posts") \
        .start()

    logger.info("Posts stream started — reading from %s", STG_POSTS)

    # --- Stream 2: stg_likes ∪ stg_reposts → core_engagement (FR-09) ---
    likes_stream = spark.readStream \
        .format("iceberg") \
        .load(STG_LIKES) \
        .filter("operation = 'create'") \
        .selectExpr(
            "'like' AS event_type",
            "did AS actor_did",
            "subject_uri",
            "event_time",
        )

    reposts_stream = spark.readStream \
        .format("iceberg") \
        .load(STG_REPOSTS) \
        .filter("operation = 'create'") \
        .selectExpr(
            "'repost' AS event_type",
            "did AS actor_did",
            "subject_uri",
            "event_time",
        )

    engagement_stream = likes_stream.union(reposts_stream)

    engagement_query = engagement_stream.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/engagement") \
        .toTable(CORE_ENGAGEMENT)

    logger.info("Engagement stream started — reading from %s + %s", STG_LIKES, STG_REPOSTS)

    # Block until any query terminates (container restarts on failure)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
