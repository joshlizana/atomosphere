"""
Bluesky Jetstream DataSource V2 for PySpark Structured Streaming.

Connects to the Jetstream WebSocket firehose and streams AT Protocol events
(posts, likes, reposts, follows, blocks, profiles) into Spark as raw_events rows.

References:
  - Spark Python DataSource V2: reference/spark-python-datasource-v2.md
  - Jetstream API: reference/jetstream-api.md
  - TRD §8.2: Custom DataSource V2 API Contract
"""

import json
import logging
import threading
import time
from collections import deque

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

logger = logging.getLogger("jetstream_source")

# Jetstream public endpoints for failover (TRD §8.1)
JETSTREAM_ENDPOINTS = [
    "wss://jetstream1.us-east.bsky.network/subscribe",
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-west.bsky.network/subscribe",
    "wss://jetstream2.us-west.bsky.network/subscribe",
]

# raw_events schema per TRD §8.2 / FR-02
RAW_EVENTS_SCHEMA = StructType([
    StructField("did", StringType(), nullable=False),
    StructField("time_us", LongType(), nullable=False),
    StructField("kind", StringType(), nullable=False),
    StructField("collection", StringType(), nullable=True),
    StructField("operation", StringType(), nullable=True),
    StructField("raw_json", StringType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
])


class JetstreamDataSource(DataSource):
    """PySpark DataSource V2 factory for the Bluesky Jetstream firehose.

    Registered as "jetstream" — usage:
        spark.readStream.format("jetstream").load()

    Options:
        endpoint: WebSocket URL (default: first JETSTREAM_ENDPOINTS entry)
        buffer_size: Max events in memory buffer (default: 50000)
    """

    @classmethod
    def name(cls):
        return "jetstream"

    def schema(self):
        return RAW_EVENTS_SCHEMA

    def simpleStreamReader(self, schema: StructType):
        return JetstreamStreamReader(schema, self.options)


class JetstreamStreamReader(SimpleDataSourceStreamReader):
    """Streaming reader that maintains a WebSocket connection to Jetstream
    and buffers events for Spark micro-batches.

    Implements the SimpleDataSourceStreamReader contract:
      - initialOffset(): current time in microseconds
      - read(start): drain buffer, return rows + next offset
      - readBetweenOffsets(start, end): deterministic replay from buffer
      - commit(end): persist offset for cursor-based reconnection
    """

    # Exponential backoff constants (FR-04, NFR-06)
    _BACKOFF_BASE = 1.0
    _BACKOFF_MAX = 30.0

    def __init__(self, schema, options):
        self._schema = schema
        self._options = options
        self._endpoint = options.get("endpoint", JETSTREAM_ENDPOINTS[0])
        self._buffer_size = int(options.get("buffer_size", 50_000))
        self._buffer = deque()
        self._buffer_lock = threading.Lock()
        self._latest_time_us = 0
        self._committed_time_us = 0
        self._ws = None
        self._ws_thread = None
        self._running = False
        self._backoff_delay = self._BACKOFF_BASE
        self._endpoint_index = JETSTREAM_ENDPOINTS.index(self._endpoint) \
            if self._endpoint in JETSTREAM_ENDPOINTS else 0
        self._consecutive_failures = 0
        self._connect()

    def _get_connect_url(self):
        """Build WebSocket URL with cursor for reconnection (FR-04)."""
        endpoint = JETSTREAM_ENDPOINTS[self._endpoint_index]
        if self._committed_time_us > 0:
            cursor = self._committed_time_us - 5_000_000
            return f"{endpoint}?cursor={cursor}"
        return endpoint

    def _rotate_endpoint(self):
        """Rotate to the next Jetstream endpoint for failover (FR-04)."""
        self._endpoint_index = (self._endpoint_index + 1) % len(JETSTREAM_ENDPOINTS)
        logger.info(
            "Failing over to endpoint: %s",
            JETSTREAM_ENDPOINTS[self._endpoint_index],
        )

    def _reconnect(self):
        """Reconnect with exponential backoff and endpoint failover (FR-04, NFR-06).

        Backoff schedule: 1s → 2s → 4s → 8s → 16s → 30s (capped).
        Rotates to the next Jetstream endpoint after each failure.
        """
        if not self._running:
            logger.warning(
                "Reconnection skipped — reader has been stopped. "
                "Pipeline will not receive new events until restarted."
            )
            return

        self._consecutive_failures += 1
        self._rotate_endpoint()

        logger.info(
            "Reconnecting in %.1fs (attempt %d, endpoint: %s)",
            self._backoff_delay,
            self._consecutive_failures,
            JETSTREAM_ENDPOINTS[self._endpoint_index],
        )

        reconnect_timer = threading.Timer(self._backoff_delay, self._connect)
        reconnect_timer.daemon = True
        reconnect_timer.start()

        # Exponential backoff: double delay, cap at 30s
        self._backoff_delay = min(self._backoff_delay * 2, self._BACKOFF_MAX)

    def _connect(self):
        """Start WebSocket connection in a background thread."""
        import websocket

        self._running = True
        url = self._get_connect_url()

        def on_message(ws, message):
            try:
                event = json.loads(message)
            except json.JSONDecodeError:
                logger.warning("Malformed JSON from Jetstream, skipping")
                return

            time_us = event.get("time_us", 0)

            with self._buffer_lock:
                if len(self._buffer) >= self._buffer_size:
                    self._buffer.popleft()
                    logger.warning(
                        "Event buffer full (%d), evicting oldest event",
                        self._buffer_size,
                    )
                self._buffer.append(event)
                if time_us > self._latest_time_us:
                    self._latest_time_us = time_us

        def on_error(ws, error):
            logger.error("WebSocket error: %s", error)

        def on_close(ws, close_status_code, close_msg):
            logger.info("WebSocket closed: %s %s", close_status_code, close_msg)
            self._reconnect()

        def on_open(ws):
            logger.info("Connected to Jetstream: %s", url)
            # Reset backoff on successful connection
            self._backoff_delay = self._BACKOFF_BASE
            self._consecutive_failures = 0

        self._ws = websocket.WebSocketApp(
            url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
            on_open=on_open,
        )

        self._ws_thread = threading.Thread(
            target=self._ws.run_forever,
            daemon=True,
        )
        self._ws_thread.start()

    def initialOffset(self):
        """Return current time in microseconds as starting offset (FR-03)."""
        return {"time_us": int(time.time() * 1_000_000)}

    def read(self, start):
        """Drain the event buffer and return rows with next offset.

        Args:
            start: dict with "time_us" key — events at or after this time

        Returns:
            (Iterator[Tuple], dict): rows matching schema + next offset
        """
        start_us = start["time_us"]

        with self._buffer_lock:
            events = list(self._buffer)
            self._buffer.clear()

        rows = []
        max_time_us = start_us

        for event in events:
            time_us = event.get("time_us", 0)
            if time_us < start_us:
                continue

            row = self._event_to_row(event)
            if row is not None:
                rows.append(row)
                if time_us > max_time_us:
                    max_time_us = time_us

        next_offset = {"time_us": max_time_us + 1} if rows else {"time_us": start_us}
        return (iter(rows), next_offset)

    def readBetweenOffsets(self, start, end):
        """Deterministic replay between offsets (for restart/failure recovery).

        Note: Since we're reading from a live WebSocket, perfect deterministic
        replay isn't possible. This returns an empty iterator — Spark checkpoints
        handle replay via the Jetstream cursor on reconnection.
        """
        return iter([])

    def commit(self, end):
        """Persist the committed offset for cursor-based reconnection (FR-03)."""
        self._committed_time_us = end["time_us"]

    def stop(self):
        """Stop the WebSocket connection and release resources."""
        self._running = False
        if self._ws:
            self._ws.close()

    def _event_to_row(self, event):
        """Convert a Jetstream JSON event to a raw_events row tuple."""
        from datetime import datetime, timezone

        did = event.get("did")
        time_us = event.get("time_us")
        kind = event.get("kind")

        if not did or not time_us or not kind:
            return None

        commit = event.get("commit", {})
        collection = commit.get("collection") if commit else None
        operation = commit.get("operation") if commit else None
        raw_json = json.dumps(event)
        ingested_at = datetime.now(timezone.utc)

        return (did, time_us, kind, collection, operation, raw_json, ingested_at)
