"""
spark-marts: Materialized mart layer.

Each mart is its own streaming query that reads an upstream Iceberg table,
windows by 1-minute tumbling buckets, and appends pre-aggregated rows to
its mart table. Dashboards then read these tiny mart tables instead of
running aggregations against the full source on every panel refresh.

Architecture:
  - 10 streaming queries (1 per materialized mart)
  - All triggered every 5 seconds (FR-10 alignment)
  - All use 15-minute event-time watermarks for late-arriving tolerance
  - Output mode: append (closed windows are emitted once watermark passes)
  - Sort: bucket_min DESC (Iceberg WRITE ORDERED BY)

Why streaming + tumbling 1-min windows + no top-N at write time:
  - Streaming gives parallelism and freshness; foreachBatch was rejected
  - 1-min granularity is universal across all marts so dashboards can re-bucket
  - Top-N (`ORDER BY count DESC LIMIT N`) is brittle in streaming, so we
    store every (bucket, key) pair and let the dashboard query do final
    sort/limit at read time. Storage cost is trivial (~16 MB stack-wide).

mart_pipeline_health is NOT materialized — it stays as a query-time view
because it's already <4s warm and is metadata, not data.

Requirements: FR-15–FR-18, FR-25, NFR-04
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, col, count, expr, window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-marts")

CHECKPOINT_BASE = "/opt/spark/checkpoints/marts"

# --- Source tables ---
RAW_EVENTS = "atmosphere.raw.raw_events"
CORE_POSTS = "atmosphere.core.core_posts"
CORE_HASHTAGS = "atmosphere.core.core_hashtags"
CORE_MENTIONS = "atmosphere.core.core_mentions"
CORE_ENGAGEMENT = "atmosphere.core.core_engagement"
CORE_POST_SENTIMENT = "atmosphere.core.core_post_sentiment"

# --- Mart target tables ---
M_EVENTS_PER_SECOND = "atmosphere.mart.mart_events_per_second"
M_ENGAGEMENT_VELOCITY = "atmosphere.mart.mart_engagement_velocity"
M_SENTIMENT_TIMESERIES = "atmosphere.mart.mart_sentiment_timeseries"
M_TRENDING_HASHTAGS = "atmosphere.mart.mart_trending_hashtags"
M_MOST_MENTIONED = "atmosphere.mart.mart_most_mentioned"
M_LANGUAGE_DISTRIBUTION = "atmosphere.mart.mart_language_distribution"
M_CONTENT_BREAKDOWN = "atmosphere.mart.mart_content_breakdown"
M_EMBED_USAGE = "atmosphere.mart.mart_embed_usage"
M_TOP_POSTS = "atmosphere.mart.mart_top_posts"
M_FIREHOSE_STATS = "atmosphere.mart.mart_firehose_stats"

WATERMARK = "15 minutes"
WINDOW_SIZE = "1 minute"
TRIGGER = "5 seconds"

# --- DDL for mart tables (FR-25) ---
# Time-series marts get hours(bucket_min) — they're the ones end users query
# directly with time predicates. Top-N marts are unpartitioned (small).
# language_distribution gets days() because of its 24-hour retention.
CREATE_MART_TABLES = {
    M_EVENTS_PER_SECOND: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_events_per_second (
            bucket_min  TIMESTAMP,
            count       BIGINT
        ) USING iceberg
        PARTITIONED BY (hours(bucket_min))
    """,
    M_ENGAGEMENT_VELOCITY: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_engagement_velocity (
            bucket_min  TIMESTAMP,
            event_type  STRING,
            count       BIGINT
        ) USING iceberg
        PARTITIONED BY (hours(bucket_min))
    """,
    M_SENTIMENT_TIMESERIES: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_sentiment_timeseries (
            bucket_min       TIMESTAMP,
            sentiment_label  STRING,
            count            BIGINT
        ) USING iceberg
        PARTITIONED BY (hours(bucket_min))
    """,
    M_TRENDING_HASHTAGS: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_trending_hashtags (
            bucket_min  TIMESTAMP,
            tag         STRING,
            count       BIGINT
        ) USING iceberg
    """,
    M_MOST_MENTIONED: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_most_mentioned (
            bucket_min     TIMESTAMP,
            mentioned_did  STRING,
            count          BIGINT
        ) USING iceberg
    """,
    M_LANGUAGE_DISTRIBUTION: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_language_distribution (
            bucket_min    TIMESTAMP,
            primary_lang  STRING,
            count         BIGINT
        ) USING iceberg
        PARTITIONED BY (days(bucket_min))
    """,
    M_CONTENT_BREAKDOWN: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_content_breakdown (
            bucket_min    TIMESTAMP,
            content_type  STRING,
            count         BIGINT
        ) USING iceberg
    """,
    M_EMBED_USAGE: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_embed_usage (
            bucket_min  TIMESTAMP,
            embed_type  STRING,
            count       BIGINT
        ) USING iceberg
    """,
    M_FIREHOSE_STATS: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_firehose_stats (
            bucket_min          TIMESTAMP,
            collection          STRING,
            count               BIGINT,
            unique_dids_approx  BIGINT
        ) USING iceberg
        PARTITIONED BY (hours(bucket_min))
    """,
    M_TOP_POSTS: """
        CREATE TABLE IF NOT EXISTS atmosphere.mart.mart_top_posts (
            bucket_min           TIMESTAMP,
            did                  STRING,
            time_us              BIGINT,
            rkey                 STRING,
            text                 STRING,
            primary_lang         STRING,
            content_type         STRING,
            sentiment_positive   DOUBLE,
            sentiment_negative   DOUBLE,
            sentiment_neutral    DOUBLE,
            sentiment_label      STRING,
            sentiment_confidence DOUBLE
        ) USING iceberg
        PARTITIONED BY (hours(bucket_min))
    """,
}


def _read_stream(spark, table):
    """Read an Iceberg table as a streaming source."""
    return spark.readStream.format("iceberg").load(table)


def _windowed_count(src, time_col, group_cols):
    """Apply 15-minute watermark + 1-minute tumbling window groupBy(...).count().

    Returns a DataFrame with columns: bucket_min, *group_cols, count.
    """
    df = src.withWatermark(time_col, WATERMARK)
    group_keys = [window(col(time_col), WINDOW_SIZE)] + [col(c) for c in group_cols]
    agg = df.groupBy(*group_keys).count()
    select_exprs = ["window.start AS bucket_min"] + group_cols + ["count"]
    return agg.selectExpr(*select_exprs)


def _start_append(name, df, target_table):
    """Start an append-mode streaming query writing to an Iceberg table."""
    q = (df.writeStream
         .format("iceberg")
         .outputMode("append")
         .trigger(processingTime=TRIGGER)
         .option("checkpointLocation", f"{CHECKPOINT_BASE}/{name}")
         .toTable(target_table))
    logger.info("Started mart query: %s → %s", name, target_table)
    return q


def start_queries(spark: SparkSession):
    """Create mart tables, start all 10 streaming mart queries.

    Returns a list of started StreamingQuery objects (does not block).
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS atmosphere.mart")

    logger.info("Ensuring mart tables exist")
    for table, ddl in CREATE_MART_TABLES.items():
        spark.sql(ddl)
        spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY bucket_min DESC")
        logger.info("Mart table ready: %s", table)

    queries = []

    # --- 1. events_per_second: total events received by the pipeline ---
    # Source uses ingested_at because raw_events has no event_time column.
    # Semantic: pipeline throughput per minute (more accurate than "events
    # per second" for a 1-minute bucket — dashboard re-buckets if needed).
    src = _read_stream(spark, RAW_EVENTS)
    df = (src.withWatermark("ingested_at", WATERMARK)
              .groupBy(window(col("ingested_at"), WINDOW_SIZE))
              .count()
              .selectExpr("window.start AS bucket_min", "count"))
    queries.append(_start_append("events_per_second", df, M_EVENTS_PER_SECOND))

    # --- 2. engagement_velocity: likes + reposts per minute, by event_type ---
    src = _read_stream(spark, CORE_ENGAGEMENT)
    df = _windowed_count(src, "event_time", ["event_type"])
    queries.append(_start_append("engagement_velocity", df, M_ENGAGEMENT_VELOCITY))

    # --- 3. sentiment_timeseries: sentiment counts per minute ---
    src = _read_stream(spark, CORE_POST_SENTIMENT)
    df = _windowed_count(src, "event_time", ["sentiment_label"])
    queries.append(_start_append("sentiment_timeseries", df, M_SENTIMENT_TIMESERIES))

    # --- 4. trending_hashtags: hashtag counts per minute ---
    src = _read_stream(spark, CORE_HASHTAGS)
    df = _windowed_count(src, "event_time", ["tag"])
    queries.append(_start_append("trending_hashtags", df, M_TRENDING_HASHTAGS))

    # --- 5. most_mentioned: mention counts per minute ---
    src = _read_stream(spark, CORE_MENTIONS)
    df = _windowed_count(src, "event_time", ["mentioned_did"])
    queries.append(_start_append("most_mentioned", df, M_MOST_MENTIONED))

    # --- 6. language_distribution: posts per language per minute ---
    src = _read_stream(spark, CORE_POSTS)
    df = _windowed_count(src, "event_time", ["primary_lang"])
    queries.append(_start_append("language_distribution", df, M_LANGUAGE_DISTRIBUTION))

    # --- 7. content_breakdown: posts per content_type per minute ---
    src = _read_stream(spark, CORE_POSTS)
    df = _windowed_count(src, "event_time", ["content_type"])
    queries.append(_start_append("content_breakdown", df, M_CONTENT_BREAKDOWN))

    # --- 8. embed_usage: posts per embed_type per minute ---
    src = _read_stream(spark, CORE_POSTS)
    df = _windowed_count(src, "event_time", ["embed_type"])
    queries.append(_start_append("embed_usage", df, M_EMBED_USAGE))

    # --- 9. firehose_stats: per-collection event count + approx unique DIDs ---
    # Feeds the dashboard 'Total Events' and 'Active Users' stat panels.
    # approx_count_distinct uses HLL under the hood; summing across minutes
    # at read time is a slight upper bound (a DID active in N minutes is
    # counted N times). Hence the panel is labeled 'Active Users', not
    # 'Unique Users'.
    src = _read_stream(spark, RAW_EVENTS)
    df = (src.withWatermark("ingested_at", WATERMARK)
              .groupBy(window(col("ingested_at"), WINDOW_SIZE), col("collection"))
              .agg(count("*").alias("count"),
                   approx_count_distinct("did").alias("unique_dids_approx"))
              .selectExpr("window.start AS bucket_min", "collection", "count", "unique_dids_approx"))
    queries.append(_start_append("firehose_stats", df, M_FIREHOSE_STATS))

    # --- 10. top_posts: stream-stream inner join (core_posts ⨝ core_post_sentiment) ---
    # No groupBy — each (did, time_us) is unique. bucket_min is a derived
    # column for read-time filtering. Watermarks on both sides bound join
    # state retention. Explicit aliases disambiguate the surviving columns.
    posts = (_read_stream(spark, CORE_POSTS)
             .withWatermark("event_time", WATERMARK))
    sent = (_read_stream(spark, CORE_POST_SENTIMENT)
            .withWatermark("event_time", WATERMARK))
    posts_a = posts.alias("p")
    sent_a = sent.alias("s")
    joined = posts_a.join(
        sent_a,
        expr("p.did = s.did AND p.time_us = s.time_us"),
        "inner",
    )
    top_posts_df = joined.selectExpr(
        "date_trunc('minute', p.event_time) AS bucket_min",
        "p.did AS did",
        "p.time_us AS time_us",
        "p.rkey AS rkey",
        "p.text AS text",
        "p.primary_lang AS primary_lang",
        "p.content_type AS content_type",
        "s.sentiment_positive AS sentiment_positive",
        "s.sentiment_negative AS sentiment_negative",
        "s.sentiment_neutral AS sentiment_neutral",
        "s.sentiment_label AS sentiment_label",
        "s.sentiment_confidence AS sentiment_confidence",
    )
    queries.append(_start_append("top_posts", top_posts_df, M_TOP_POSTS))

    logger.info("All %d mart streaming queries started", len(queries))
    return queries


def main():
    spark = SparkSession.builder.appName("spark-marts").getOrCreate()
    start_queries(spark)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
