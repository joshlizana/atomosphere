"""Sentiment-stage L2 checks — lag, throughput, backlog.

Sentiment is its own stage after ``core_posts`` because it's the only layer
that runs GPU inference, and it's the one that can silently stall without
upstream breaking. These three checks isolate that stage:

- ``latency.sentiment.lag``       — how stale the newest sentiment rows are
  relative to their core_posts row (inference queue depth in time)
- ``traffic.sentiment_rps``       — rows/s landing in core_post_sentiment
  over the last 10s (mirrors traffic.jetstream_input_rps)
- ``saturation.sentiment_backlog`` — unmatched ratio of the newest 1000
  core_posts rows against core_post_sentiment (inference queue depth in rows)

All three run against the process-wide DuckDB instance via ``engine.cursor()``.
Lag + backlog are terminal-first joins in opposite directions — lag starts
from sentiment (so unmatched=0 by construction) and backlog starts from
core_posts (so unmatched>0 = things sentiment hasn't caught up to yet).
"""
from __future__ import annotations

from .. import engine
from ..config import THRESHOLDS
from ..registry import check
from ..result import Result, breach, error, ok


# --------------------------------------------------------------------------- lag
@check(
    "latency.sentiment.lag",
    layer="l2",
    tier="fast",
    tolerance=2,
    service="spark",
    zones=((0.0, 15.0, "green"), (15.0, 60.0, "orange"), (60.0, 86400.0, "red")),
)
def sentiment_lag(ctx) -> Result:
    """Avg ``sentiment.ingested_at - core_posts.ingested_at`` over the newest
    1000 sentiment rows. Terminal-first so every row is guaranteed to have a
    core_posts match — matched rows should equal 1000 in steady state."""
    name = "latency.sentiment.lag"
    max_s = THRESHOLDS[f"{name}.max_s"]
    sql = """
        WITH recent AS (
            SELECT did, time_us, ingested_at
            FROM polaris.core.core_post_sentiment
            ORDER BY ingested_at DESC
            LIMIT 1000
        )
        SELECT
            count(*)                                                    AS matched,
            avg(EXTRACT(EPOCH FROM (s.ingested_at - p.ingested_at)))    AS avg_s
        FROM recent s
        INNER JOIN polaris.core.core_posts p
          ON p.did = s.did AND p.time_us = s.time_us
    """
    try:
        con = engine.cursor()
        row = con.execute(sql).fetchone()
    except Exception as exc:  # noqa: BLE001
        return error(name, f"duckdb error: {exc.__class__.__name__}: {exc}")

    if row is None or not row[0]:
        return ok(name, "no sentiment rows yet", value=None)
    lag_s = float(row[1])
    if lag_s > max_s:
        return breach(name, f"sentiment avg lag {lag_s:.1f}s > {max_s}s", value=lag_s)
    return ok(name, f"sentiment avg lag {lag_s:.1f}s", value=lag_s)


# --------------------------------------------------------------------------- throughput
@check(
    "traffic.sentiment_rps",
    layer="l2",
    tier="fast",
    tolerance=2,
    service="spark",
    zones=(
        (0.0, 5.0, "red"),
        (5.0, 20.0, "orange"),
        (20.0, 10_000.0, "green"),
    ),
)
def sentiment_rps(ctx) -> Result:
    """Rolling 60s rate of rows landing in ``core_post_sentiment``.

    The sentiment stream runs a 5s trigger but is inference-bound: each
    micro-batch pulls up to 10k core_posts rows and the commit doesn't
    happen until GPU inference finishes, which can take ~30s per batch
    under load. So commits arrive in widely-spaced bursts — not at a
    steady 5s cadence — and ``ingested_at`` reflects commit time, not
    processing time. A 10s window might sample entirely inside one
    inference burst and read 0; a 60s window always straddles at least
    one commit and averages it out to the true throughput. Below ``min``
    means the GPU worker has genuinely starved or the stream is broken.
    """
    name = "traffic.sentiment_rps"
    min_rps = THRESHOLDS[f"{name}.min"]
    try:
        con = engine.cursor()
        row = con.execute(
            """
            SELECT count(*) / 60.0
            FROM polaris.core.core_post_sentiment
            WHERE ingested_at >= now() - INTERVAL 60 SECONDS
            """
        ).fetchone()
    except Exception as exc:  # noqa: BLE001
        return error(name, f"duckdb error: {exc.__class__.__name__}: {exc}")

    rps = float(row[0] if row and row[0] is not None else 0.0)
    if rps < min_rps:
        return breach(name, f"sentiment rows/s {rps:.1f} (60s) < {min_rps}", value=rps)
    return ok(name, f"sentiment rows/s {rps:.1f} (60s)", value=rps)


# --------------------------------------------------------------------------- backlog
@check(
    "saturation.sentiment_backlog",
    layer="l2",
    tier="fast",
    tolerance=2,
    service="spark",
    zones=((0.0, 200.0, "green"), (200.0, 500.0, "orange"), (500.0, 1000.0, "red")),
)
def sentiment_backlog(ctx) -> Result:
    """Unmatched row count in the newest 1000 ``core_posts`` rows against
    ``core_post_sentiment``.

    Runs from core_posts outward (the opposite direction of ``latency.*``).
    A steady-state value near zero means sentiment is keeping up; a growing
    count means the GPU stage is queueing rows faster than it clears them.
    Reported as raw rows (0..1000) so the operator sees absolute queue depth.
    """
    name = "saturation.sentiment_backlog"
    max_rows = THRESHOLDS[f"{name}.max"]
    sql = """
        WITH recent AS (
            SELECT did, time_us
            FROM polaris.core.core_posts
            ORDER BY ingested_at DESC
            LIMIT 1000
        )
        SELECT
            count(*)                                     AS total,
            count(*) FILTER (WHERE s.did IS NULL)        AS unmatched
        FROM recent p
        LEFT JOIN polaris.core.core_post_sentiment s
          ON s.did = p.did AND s.time_us = p.time_us
    """
    try:
        con = engine.cursor()
        row = con.execute(sql).fetchone()
    except Exception as exc:  # noqa: BLE001
        return error(name, f"duckdb error: {exc.__class__.__name__}: {exc}")

    if row is None or not row[0]:
        return ok(name, "no core_posts rows yet", value=None)
    total = int(row[0])
    unmatched = int(row[1])
    if unmatched > max_rows:
        return breach(
            name,
            f"sentiment backlog {unmatched}/{total} > {int(max_rows)}",
            value=float(unmatched),
        )
    return ok(name, f"sentiment backlog {unmatched}/{total}", value=float(unmatched))
