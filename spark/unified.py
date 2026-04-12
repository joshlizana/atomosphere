"""
spark-unified: Single-process consolidated streaming pipeline.

Runs all 5 streaming layers in one SparkSession / one JVM:
  1. Ingest:    Jetstream WebSocket → raw_events
  2. Staging:   raw_events → 6 stg_* tables
  3. Core:      stg_posts → core_* tables
  4. Sentiment: core_posts → core_post_sentiment (GPU)
  5. Marts:     core_* / raw_events → atmosphere.mart.mart_* (10 streaming queries)

Total: 16 streaming queries (ingest 1 + staging 6 + core 2 + sentiment 1 + marts 10).
All queries use trigger(processingTime="5 seconds").
On any query failure, the process exits and Docker restarts the container.

Requirements: FR-01–FR-14, FR-15–FR-18, FR-25, NFR-03, NFR-04, NFR-07, NFR-09, NFR-11
"""

import logging

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-unified")


def main():
    spark = SparkSession.builder \
        .appName("spark-unified") \
        .getOrCreate()

    # Ensure top-level namespaces exist before any DDL
    for ns in ["atmosphere.raw", "atmosphere.staging", "atmosphere.core", "atmosphere.mart"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

    queries = []

    # --- Layer 1: Ingest (Jetstream → raw_events) ---
    logger.info("Starting ingest layer")
    from spark.ingestion.ingest_raw import start_queries as start_ingest
    queries.extend(start_ingest(spark))

    # --- Layer 2: Staging (raw_events → 6 stg_* tables) ---
    logger.info("Starting staging layer")
    from spark.transforms.staging import start_queries as start_staging
    queries.extend(start_staging(spark))

    # --- Layer 3: Core (stg_posts → core_* tables + mart views) ---
    logger.info("Starting core layer")
    from spark.transforms.core import start_queries as start_core
    queries.extend(start_core(spark))

    # --- Layer 4: Sentiment (core_posts → core_post_sentiment) ---
    logger.info("Starting sentiment layer")
    from spark.transforms.sentiment import start_queries as start_sentiment
    queries.extend(start_sentiment(spark))

    # --- Layer 5: Marts (core_* / raw_events → mart_*) ---
    logger.info("Starting marts layer")
    from spark.transforms.marts import start_queries as start_marts
    queries.extend(start_marts(spark))

    logger.info("All %d streaming queries started", len(queries))

    # Block until any query terminates — Docker restart handles recovery
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
