"""
spark-ingest: Structured Streaming job that reads from the Bluesky Jetstream
WebSocket via JetstreamDataSource and writes raw events to Iceberg.

Output table: atmosphere.raw.raw_events
Partitioning: days(ingested_at), collection  (TDD §5.2)
Sort order:   ingested_at ASC

Requirements: FR-01, FR-02, FR-03, FR-10, FR-25, NFR-07
"""

import logging

from pyspark.sql import SparkSession

from spark.sources.jetstream_source import JetstreamDataSource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-ingest")

TABLE = "atmosphere.raw.raw_events"
CHECKPOINT = "/opt/spark/checkpoints/ingest-raw"

# DDL for raw_events table — CREATE TABLE IF NOT EXISTS (FR-25)
# Partition: days(ingested_at), collection (TDD §5.6)
# Sort: ingested_at ASC
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    did             STRING      NOT NULL,
    time_us         BIGINT      NOT NULL,
    kind            STRING      NOT NULL,
    collection      STRING,
    operation       STRING,
    raw_json        STRING      NOT NULL,
    ingested_at     TIMESTAMP   NOT NULL
)
USING iceberg
PARTITIONED BY (days(ingested_at), collection)
"""


def main():
    spark = SparkSession.builder \
        .appName("spark-ingest") \
        .getOrCreate()

    # Register the custom DataSource V2
    spark.dataSource.register(JetstreamDataSource)

    # Create table with partitioning on first run (FR-25)
    logger.info("Ensuring table exists: %s", TABLE)
    spark.sql(CREATE_TABLE_SQL)

    # Set sort order via ALTER TABLE (Iceberg DDL)
    spark.sql(f"ALTER TABLE {TABLE} WRITE ORDERED BY ingested_at ASC")

    # Read from Jetstream WebSocket
    stream_df = spark.readStream \
        .format("jetstream") \
        .load()

    # Write to Iceberg with 5-second micro-batch trigger (FR-10)
    query = stream_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", CHECKPOINT) \
        .toTable(TABLE)

    logger.info("Ingestion streaming query started — writing to %s", TABLE)
    query.awaitTermination()


if __name__ == "__main__":
    main()
