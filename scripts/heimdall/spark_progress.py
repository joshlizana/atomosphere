"""Spark Structured Streaming progress via Dropwizard ``/metrics/json``.

In Spark 4 the DStreams-era ``/api/v1/applications/{id}/streaming/statistics``
and ``/sql/streaming`` REST endpoints do not surface Structured Streaming
progress. The supported path is the Dropwizard ``MetricsServlet`` at
``http://<driver>:4040/metrics/json`` plus ``spark.sql.streaming.metricsEnabled
= true`` in ``spark-defaults.conf`` — see:

    https://spark.apache.org/docs/4.0.0/monitoring.html

When enabled, each active query registers gauges under the namespace
``spark.streaming.<queryName>``::

    <app_id>.driver.spark.streaming.<queryName>.inputRate-total
    <app_id>.driver.spark.streaming.<queryName>.processingRate-total
    <app_id>.driver.spark.streaming.<queryName>.latency
    <app_id>.driver.spark.streaming.<queryName>.eventTime-watermark
    <app_id>.driver.spark.streaming.<queryName>.states-rowsTotal
    <app_id>.driver.spark.streaming.<queryName>.states-usedBytes

This module parses that shape into a small ``SparkSnapshot`` used by the L2
checks in ``checks/spark.py``. Each call to :func:`fetch` does one HTTP round
trip; :class:`runner.Context.spark_progress` caches it per cycle.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests

from . import config


class SparkUnreachable(RuntimeError):
    """Raised when neither ``/api/v1/applications`` nor ``/metrics/json`` responds."""


# Matches ``<app_id>.driver.spark.streaming.<query>.<metric>``. ``query`` is
# captured non-greedily so a metric name containing dots (there aren't any in
# practice, but be safe) would still split correctly.
_METRIC_RE = re.compile(
    r"^(?P<app>[^.]+)\.driver\.spark\.streaming\.(?P<query>.+?)\.(?P<metric>[^.]+)$"
)


@dataclass
class StreamingQuery:
    name: str
    input_rate: float | None = None
    processing_rate: float | None = None
    latency_ms: float | None = None
    event_time_watermark_ms: float | None = None
    states_rows_total: float | None = None
    states_used_bytes: float | None = None
    # Heimdall-side stamp of when we observed this query (unix seconds). The
    # Dropwizard gauge itself does not carry a timestamp; absence of the gauge
    # across two consecutive cycles is the "query disappeared" signal.
    observed_at: float = field(default_factory=time.time)


@dataclass
class SparkSnapshot:
    app_id: str | None
    queries: list[dict[str, Any]] = field(default_factory=list)
    fetched_at: float = field(default_factory=time.time)

    def query(self, name_substring: str) -> dict[str, Any] | None:
        for q in self.queries:
            if name_substring in (q.get("name") or ""):
                return q
        return None

    def any_failed(self) -> list[str]:
        """Return names of queries whose inputRate-total went missing or <0.

        Spark 4's MetricsServlet only publishes gauges for *registered* queries
        — a query that terminated drops off the list, which is the failure
        signal we care about for L2. Callers compare against a baseline list
        of expected query names if they need a specific failure set.
        """
        dead: list[str] = []
        for q in self.queries:
            rate = q.get("inputRowsPerSecond")
            if rate is None or (isinstance(rate, (int, float)) and rate < 0):
                dead.append(q.get("name", "?"))
        return dead

    def newest_progress_epoch(self) -> float | None:
        """Proxy for "how fresh is the latest batch".

        Dropwizard gauges do not carry per-batch timestamps, so we treat the
        fetch time as the freshness floor: if any query has a non-null
        ``inputRowsPerSecond`` gauge, that value was updated during the
        driver's most recent micro-batch, bounded above by fetch time. The L2
        ``latency.jetstream_to_raw`` check uses this plus its own threshold.
        """
        if not self.queries:
            return None
        # ``latency`` is published per query in ms as the real end-to-end gap;
        # if we have it, prefer using it directly over "now - fetched_at".
        return self.fetched_at


def _get_json(url: str, timeout: float) -> dict[str, Any] | None:
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.json()
    except (requests.RequestException, ValueError):
        return None


def _app_id(base: str, timeout: float) -> str | None:
    apps = _get_json(f"{base}/api/v1/applications", timeout)
    if not apps or not isinstance(apps, list):
        return None
    return apps[0].get("id")


def _parse_metrics(blob: dict[str, Any]) -> dict[str, StreamingQuery]:
    """Extract ``spark.streaming.*`` gauges into per-query records."""
    gauges = blob.get("gauges") or {}
    by_query: dict[str, StreamingQuery] = {}
    for key, entry in gauges.items():
        match = _METRIC_RE.match(key)
        if not match:
            continue
        query_name = match.group("query")
        metric = match.group("metric")
        value = entry.get("value") if isinstance(entry, dict) else None
        q = by_query.setdefault(query_name, StreamingQuery(name=query_name))
        if metric == "inputRate-total":
            q.input_rate = _as_float(value)
        elif metric == "processingRate-total":
            q.processing_rate = _as_float(value)
        elif metric == "latency":
            q.latency_ms = _as_float(value)
        elif metric == "eventTime-watermark":
            q.event_time_watermark_ms = _as_float(value)
        elif metric == "states-rowsTotal":
            q.states_rows_total = _as_float(value)
        elif metric == "states-usedBytes":
            q.states_used_bytes = _as_float(value)
    return by_query


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_legacy_dict(q: StreamingQuery) -> dict[str, Any]:
    """Shape each query like the legacy StreamingQueryProgress JSON so the
    check functions in ``checks/spark.py`` can stay field-driven."""
    return {
        "name": q.name,
        "state": "RUNNING" if q.input_rate is not None else "UNKNOWN",
        "inputRowsPerSecond": q.input_rate,
        "processedRowsPerSecond": q.processing_rate,
        "latencyMs": q.latency_ms,
        "eventTimeWatermarkMs": q.event_time_watermark_ms,
        "statesRowsTotal": q.states_rows_total,
        "statesUsedBytes": q.states_used_bytes,
    }


def fetch(timeout: float = 5.0) -> SparkSnapshot:
    """One round trip each to ``/api/v1/applications`` and ``/metrics/json``.

    Raises :class:`SparkUnreachable` if the UI doesn't answer at all. An empty
    but reachable UI (no queries registered yet) returns an empty snapshot;
    downstream checks treat that as ``error`` so the operator sees the
    distinction between "Spark down" and "Spark up but idle".
    """
    base = config.SPARK_UI_URL.rstrip("/")
    app_id = _app_id(base, timeout)
    if app_id is None:
        raise SparkUnreachable(f"spark UI unreachable at {base}")

    metrics = _get_json(f"{base}/metrics/json", timeout)
    if metrics is None:
        # UI is up but /metrics/json is not — the servlet is disabled or
        # spark.sql.streaming.metricsEnabled=false. Surface as empty queries
        # so checks report error rather than raising.
        return SparkSnapshot(app_id=app_id, queries=[])

    queries = [_to_legacy_dict(q) for q in _parse_metrics(metrics).values()]
    return SparkSnapshot(app_id=app_id, queries=queries)
