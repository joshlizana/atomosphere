"""
spark-sentiment: Structured Streaming job for GPU-accelerated sentiment analysis.

Reads:  atmosphere.core.core_posts
Writes: atmosphere.core.core_post_sentiment

Uses cardiffnlp/twitter-xlm-roberta-base-sentiment via HuggingFace pipeline
with mapInPandas for vectorized batch inference (batch_size=64).

Requirements: FR-11, FR-12, FR-13, FR-14, NFR-03, NFR-11, NFR-16
"""

import logging
import re

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("spark-sentiment")

MODEL_PATH = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
SOURCE_TABLE = "atmosphere.core.core_posts"
TARGET_TABLE = "atmosphere.core.core_post_sentiment"
CHECKPOINT = "/opt/spark/checkpoints/sentiment"

# Output schema for mapInPandas — matches core_post_sentiment DDL
SENTIMENT_SCHEMA = StructType([
    StructField("did", StringType(), nullable=False),
    StructField("time_us", LongType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("rkey", StringType(), nullable=True),
    StructField("text", StringType(), nullable=True),
    StructField("sentiment_positive", DoubleType(), nullable=True),
    StructField("sentiment_negative", DoubleType(), nullable=True),
    StructField("sentiment_neutral", DoubleType(), nullable=True),
    StructField("sentiment_label", StringType(), nullable=True),
    StructField("sentiment_confidence", DoubleType(), nullable=True),
])

# Preprocess pattern: replace @mentions and URLs per model docs
_MENTION_RE = re.compile(r"@\S+")
_URL_RE = re.compile(r"https?://\S+")


def preprocess(text):
    """Replace @mentions with @user and URLs with http (model preprocessing)."""
    if not text:
        return ""
    text = _MENTION_RE.sub("@user", text)
    text = _URL_RE.sub("http", text)
    return text


def predict_sentiment(iterator):
    """mapInPandas function: load model once per worker, score batches.

    The model is loaded outside the loop so it persists in GPU/CPU memory
    across all batches processed by this worker (FR-12, NFR-03).

    Args:
        iterator: Iterator of pandas DataFrames with core_posts columns

    Yields:
        pandas DataFrames matching SENTIMENT_SCHEMA
    """
    import pandas as pd
    import torch
    from transformers import pipeline

    # GPU/CPU adaptation (NFR-11)
    device = 0 if torch.cuda.is_available() else -1
    device_name = "GPU" if device == 0 else "CPU"
    logger.info("Loading sentiment model on %s (device=%d)", device_name, device)

    pipe = pipeline(
        "sentiment-analysis",
        model=MODEL_PATH,
        tokenizer=MODEL_PATH,
        device=device,
        top_k=None,
        truncation=True,
        max_length=512,
    )

    logger.info("Sentiment model loaded on %s", device_name)

    for batch_df in iterator:
        if batch_df.empty:
            yield pd.DataFrame(columns=[f.name for f in SENTIMENT_SCHEMA.fields])
            continue

        # Preprocess text for the model
        texts = batch_df["text"].fillna("").apply(preprocess).tolist()

        # Run inference with batch_size=64 (FR-12)
        results = pipe(texts, batch_size=64)

        # Extract scores per row (FR-11)
        positives = []
        negatives = []
        neutrals = []
        labels = []
        confidences = []

        for result in results:
            scores = {r["label"]: r["score"] for r in result}
            pos = scores.get("Positive", 0.0)
            neg = scores.get("Negative", 0.0)
            neu = scores.get("Neutral", 0.0)

            positives.append(pos)
            negatives.append(neg)
            neutrals.append(neu)

            # Argmax label + confidence (max score)
            best = max(result, key=lambda r: r["score"])
            labels.append(best["label"].lower())
            confidences.append(best["score"])

        # Build output DataFrame matching SENTIMENT_SCHEMA
        output = pd.DataFrame({
            "did": batch_df["did"].values,
            "time_us": batch_df["time_us"].values,
            "event_time": batch_df["event_time"].values,
            "rkey": batch_df["rkey"].values,
            "text": batch_df["text"].values,
            "sentiment_positive": positives,
            "sentiment_negative": negatives,
            "sentiment_neutral": neutrals,
            "sentiment_label": labels,
            "sentiment_confidence": confidences,
        })

        yield output


# DDL for core_post_sentiment — CREATE TABLE IF NOT EXISTS (FR-25)
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
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
"""


def start_queries(spark):
    """Set up sentiment DDL and start the streaming query.

    Returns a list of started StreamingQuery objects (does not block).
    Can be called from the unified entrypoint or from main() for standalone use.
    """
    # Ensure table exists (FR-25)
    logger.info("Ensuring table exists: %s", TARGET_TABLE)
    spark.sql(CREATE_TABLE_SQL)
    spark.sql(f"ALTER TABLE {TARGET_TABLE} WRITE ORDERED BY event_time ASC")

    # Read core_posts as streaming Iceberg source
    posts_stream = spark.readStream \
        .format("iceberg") \
        .load(SOURCE_TABLE)

    # Select only needed columns for inference
    input_df = posts_stream.select("did", "time_us", "event_time", "rkey", "text")

    # Apply sentiment inference via mapInPandas (FR-12)
    scored_df = input_df.mapInPandas(predict_sentiment, schema=SENTIMENT_SCHEMA)

    # Write to Iceberg with 5-second trigger (FR-10)
    query = scored_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", CHECKPOINT) \
        .toTable(TARGET_TABLE)

    logger.info("Sentiment streaming query started — %s → %s", SOURCE_TABLE, TARGET_TABLE)
    return [query]


def main():
    spark = SparkSession.builder \
        .appName("spark-sentiment") \
        .getOrCreate()

    queries = start_queries(spark)
    queries[0].awaitTermination()


if __name__ == "__main__":
    main()
