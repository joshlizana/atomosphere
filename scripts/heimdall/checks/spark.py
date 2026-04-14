"""Spark streaming health checks — traffic, latency, query state.

All three read from :mod:`spark_progress`, so they share one HTTP round-trip
per cycle (see :class:`runner.Context.spark_progress`). v2 splits traffic /
latency / errors into separate files once per-layer checks land.
"""
from __future__ import annotations

from .. import engine
from ..config import THRESHOLDS
from ..registry import check
from ..result import Result, breach, error, ok, warn
from ..spark_progress import SparkUnreachable


# --------------------------------------------------------------------------- traffic
@check(
    "traffic.jetstream_input_rps",
    layer="l2",
    tier="fast",
    tolerance=2,
    service="spark",
    zones=(
        (0.0, 100.0, "red"),
        (100.0, 200.0, "orange"),
        (200.0, 1_000_000.0, "green"),
    ),
)
def jetstream_input_rps(ctx) -> Result:
    """Rolling 10s arrival rate on ``raw.raw_events`` via DuckDB + Iceberg.

    Counts what actually landed in Iceberg, not what Spark's internal
    ``inputRowsPerSecond`` gauge thinks it processed. Runs against the
    process-wide DuckDB instance managed by :mod:`heimdall.engine`.
    """
    name = "traffic.jetstream_input_rps"
    min_rps = THRESHOLDS[f"{name}.min"]
    try:
        con = engine.cursor()
        row = con.execute(
            """
            SELECT count(*) / 10.0
            FROM polaris.raw.raw_events
            WHERE ingested_at >= now() - INTERVAL 10 SECONDS
            """
        ).fetchone()
    except Exception as exc:  # noqa: BLE001
        return error(name, f"duckdb error: {exc.__class__.__name__}: {exc}")

    rps = float(row[0] if row and row[0] is not None else 0.0)
    if rps < min_rps:
        return breach(name, f"raw_events/s {rps:.1f} < {min_rps}", value=rps)
    return ok(name, f"raw_events/s {rps:.1f}", value=rps)


# --------------------------------------------------------------------------- errors
@check(
    "errors.streaming_query_state",
    layer="l2",
    tier="fast",
    tolerance=1,
    service="spark",
    zones=((0.0, 0.5, "green"), (0.5, 100.0, "red")),
)
def streaming_query_state(ctx) -> Result:
    name = "errors.streaming_query_state"
    try:
        snap = ctx.spark_progress
    except SparkUnreachable as exc:
        return error(name, str(exc))
    failed = snap.any_failed()
    if failed:
        return breach(
            name,
            f"{len(failed)} failed queries: {', '.join(failed)}",
            value=len(failed),
        )
    # Value is always "number of failed queries" so the L2 band zone coloring
    # (0 green, >0 red) is semantically consistent. Total-running count lives
    # in the message instead.
    return ok(
        name,
        f"0 failed / {len(snap.queries)} running",
        value=0,
    )


# --------------------------------------------------------------------------- e2e pipeline lag
# One check per event type measures the full pipeline: raw → terminal table.
# Shape: take the latest 1000 rows from the terminal table, join back to raw
# on (did, time_us), and average ``terminal.ingested_at - raw.ingested_at``.
# Terminal-first avoids the dead-zone problem where the newest raw rows have
# not yet been processed — every terminal row is guaranteed to exist in raw.
_PIPELINE_STAGES: dict[str, dict[str, str | None]] = {
    "post": {
        "collection": "app.bsky.feed.post",
        "terminal_table": "polaris.core.core_posts",
        "did_col": "did",
        "event_type_filter": None,
    },
    "like": {
        "collection": "app.bsky.feed.like",
        "terminal_table": "polaris.core.core_engagement",
        "did_col": "actor_did",
        "event_type_filter": "like",
    },
    "repost": {
        "collection": "app.bsky.feed.repost",
        "terminal_table": "polaris.core.core_engagement",
        "did_col": "actor_did",
        "event_type_filter": "repost",
    },
    "follow": {
        "collection": "app.bsky.graph.follow",
        "terminal_table": "polaris.staging.stg_follows",
        "did_col": "did",
        "event_type_filter": None,
    },
    "block": {
        "collection": "app.bsky.graph.block",
        "terminal_table": "polaris.staging.stg_blocks",
        "did_col": "did",
        "event_type_filter": None,
    },
    "profile": {
        "collection": "app.bsky.actor.profile",
        "terminal_table": "polaris.staging.stg_profiles",
        "did_col": "did",
        "event_type_filter": None,
    },
}


def _pipeline_check(event_type: str, stage: dict[str, str | None]):
    cname = f"latency.pipeline.{event_type}"
    term_filter = (
        f"WHERE event_type = '{stage['event_type_filter']}'"
        if stage["event_type_filter"]
        else ""
    )
    lag_sql = f"""
        WITH recent AS (
            SELECT {stage['did_col']} AS did, time_us, ingested_at
            FROM {stage['terminal_table']}
            {term_filter}
            ORDER BY ingested_at DESC
            LIMIT 1000
        )
        SELECT
            count(*)                                                   AS matched,
            avg(EXTRACT(EPOCH FROM (t.ingested_at - raw.ingested_at))) AS avg_s
        FROM recent t
        INNER JOIN polaris.raw.raw_events raw
          ON raw.did = t.did AND raw.time_us = t.time_us
        WHERE raw.collection = '{stage['collection']}'
    """

    @check(
        cname,
        layer="l2",
        tier="fast",
        tolerance=2,
        service="spark",
        zones=((0.0, 15.0, "green"), (15.0, 60.0, "orange"), (60.0, 86400.0, "red")),
    )
    def _impl(ctx) -> Result:
        max_s = THRESHOLDS[f"{cname}.max_s"]
        try:
            con = engine.cursor()
            row = con.execute(lag_sql).fetchone()
        except Exception as exc:  # noqa: BLE001
            return error(cname, f"duckdb error: {exc.__class__.__name__}: {exc}")

        if row is None or not row[0]:
            return ok(cname, f"{event_type}: no matches yet", value=None)
        lag_s = float(row[1])
        if lag_s > max_s:
            return breach(cname, f"{event_type} avg lag {lag_s:.1f}s > {max_s}s", value=lag_s)
        return ok(cname, f"{event_type} avg lag {lag_s:.1f}s", value=lag_s)

    _impl.__name__ = f"pipeline_lag_{event_type}"
    return _impl


for _event_type, _stage in _PIPELINE_STAGES.items():
    _pipeline_check(_event_type, _stage)


@check(
    "latency.jetstream_to_raw",
    layer="l2",
    tier="fast",
    tolerance=2,
    depends_on=("containers.spark.state",),
    service="spark",
    zones=((0.0, 5.0, "green"), (5.0, 10.0, "orange"), (10.0, 3600.0, "red")),
)
def jetstream_to_raw_latency(ctx) -> Result:
    """Micro-batch staleness proxy: ``now - max(progress.timestamp)``.

    The Spark UI publishes one ``timestamp`` per StreamingQueryProgress batch.
    If the newest across all queries is more than ``max_s`` old, something is
    stuck. This replaces the earlier Iceberg-snapshot approximation so L2
    checks stay self-contained to the Spark UI.
    """
    import time

    name = "latency.jetstream_to_raw"
    max_s = THRESHOLDS[f"{name}.max_s"]
    try:
        snap = ctx.spark_progress
    except SparkUnreachable as exc:
        return error(name, str(exc))
    newest = snap.newest_progress_epoch()
    if newest is None:
        return breach(name, "no streaming progress yet", value=None)
    age = time.time() - newest
    if age > max_s:
        return breach(name, f"latency {age:.1f}s > {max_s}s", value=age)
    return ok(name, f"latency {age:.1f}s", value=age)
