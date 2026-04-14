"""Data-quality checks via the shared SqlEngine.

v1 ships three signals covering completeness and timeliness. The rest of the
DQ taxonomy (validity ranges, uniqueness, integrity, accuracy) is stubbed in
the TODO list at the bottom of this module.
"""
from __future__ import annotations

import time

from ..config import THRESHOLDS
from ..registry import check
from ..result import Result, breach, error, ok


# --------------------------------------------------------------------------- freshness
@check("quality.raw_events.freshness_s", layer="l3", tier="fast")
def raw_events_freshness(ctx) -> Result:
    name = "quality.raw_events.freshness_s"
    max_s = THRESHOLDS[f"{name}.max"]
    try:
        latest = ctx.engine.max_timestamp("atmosphere.raw.raw_events", "ingested_at")
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if latest is None:
        return breach(name, "raw_events has no snapshot", value=None)
    age = time.time() - latest
    if age > max_s:
        return breach(name, f"raw_events stale by {age:.1f}s (> {max_s}s)", value=age)
    return ok(name, f"raw_events fresh ({age:.1f}s)", value=age)


@check("quality.mart_trending_hashtags.freshness_s", layer="l3", tier="fast")
def mart_trending_freshness(ctx) -> Result:
    name = "quality.mart_trending_hashtags.freshness_s"
    max_s = THRESHOLDS["quality.mart_trending.freshness_s.max"]
    try:
        latest = ctx.engine.max_timestamp(
            "atmosphere.mart.mart_trending_hashtags", "bucket_min"
        )
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if latest is None:
        return breach(name, "mart_trending_hashtags has no snapshot", value=None)
    age = time.time() - latest
    if age > max_s:
        return breach(name, f"mart stale by {age:.1f}s (> {max_s}s)", value=age)
    return ok(name, f"mart fresh ({age:.1f}s)", value=age)


# --------------------------------------------------------------------------- completeness
@check("quality.raw_events.null_did_rate", layer="l3", tier="fast")
def raw_events_null_did_rate(ctx) -> Result:
    name = "quality.raw_events.null_did_rate"
    max_ratio = THRESHOLDS[f"{name}.max"]
    try:
        ratio = ctx.engine.null_ratio("atmosphere.raw.raw_events", "did", sample_limit=10_000)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if ratio > max_ratio:
        return breach(name, f"did null-rate {ratio:.4f} > {max_ratio:.4f}", value=ratio)
    return ok(name, f"did null-rate {ratio:.4f}", value=ratio)


# --------------------------------------------------------------------------- deferred
# TODO (v2): full DQ taxonomy — see reference/atmosphere-monitor-signal-taxonomy.md
#   validity:
#     - quality.core_post_sentiment.score_range ([-1, 1])
#     - quality.language_distribution.lang_code_valid (ISO 639)
#     - quality.stg_posts.event_time_sanity (2023-01-01 .. now+1h)
#     - quality.raw_events.kind_whitelist
#   uniqueness:
#     - quality.raw_events.dup_ratio (by did, time_us, kind)
#     - quality.stg_posts.dup_uri_ratio
#     - quality.core_post_sentiment.dup_ratio (by post_id)
#   integrity:
#     - quality.sentiment_vs_posts.join_coverage
#     - quality.mart_vs_core.row_delta
#   accuracy:
#     - quality.raw_events.schema_id drift
#     - quality.mart_language_distribution.schema_id drift
