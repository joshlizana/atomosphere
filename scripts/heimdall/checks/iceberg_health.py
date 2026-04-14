"""Iceberg storage-hygiene checks, templated over a list of tables.

v1 watches three representative tables (raw, core, mart). Expanding to all 22
Iceberg tables is a one-line edit to ``V1_ICEBERG_TABLES`` in ``config.py``.
Each registered signal is named ``iceberg.{metric}.{table}``.
"""
from __future__ import annotations

from ..config import THRESHOLDS, V1_ICEBERG_TABLES
from ..registry import check_over
from ..result import Result, breach, error, ok, warn


@check_over(
    keys=V1_ICEBERG_TABLES,
    name_template="iceberg.file_count.{key}",
    layer="l3",
    tier="slow",
)
def iceberg_file_count(ctx, *, key: str) -> Result:
    name = f"iceberg.file_count.{key}"
    max_count = THRESHOLDS["iceberg.file_count.max"]
    try:
        stats = ctx.engine.iceberg_file_stats(key)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if stats.file_count > max_count:
        return breach(
            name,
            f"{key}: {stats.file_count} files > {max_count}",
            value=stats.file_count,
            metric_key=key,
        )
    return ok(name, f"{key}: {stats.file_count} files", value=stats.file_count, metric_key=key)


@check_over(
    keys=V1_ICEBERG_TABLES,
    name_template="iceberg.avg_file_kb.{key}",
    layer="l3",
    tier="slow",
)
def iceberg_avg_file_kb(ctx, *, key: str) -> Result:
    name = f"iceberg.avg_file_kb.{key}"
    min_kb = THRESHOLDS["iceberg.avg_file_kb.min"]
    try:
        stats = ctx.engine.iceberg_file_stats(key)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if stats.file_count == 0:
        return ok(name, f"{key}: empty", value=0.0, metric_key=key)
    if stats.avg_file_kb < min_kb:
        return warn(
            name,
            f"{key}: avg {stats.avg_file_kb:.1f}KB < {min_kb}KB (compaction due)",
            value=stats.avg_file_kb,
            metric_key=key,
        )
    return ok(
        name,
        f"{key}: avg {stats.avg_file_kb:.1f}KB",
        value=stats.avg_file_kb,
        metric_key=key,
    )


@check_over(
    keys=V1_ICEBERG_TABLES,
    name_template="iceberg.small_file_ratio.{key}",
    layer="l3",
    tier="slow",
)
def iceberg_small_file_ratio(ctx, *, key: str) -> Result:
    name = f"iceberg.small_file_ratio.{key}"
    max_ratio = THRESHOLDS["iceberg.small_file_ratio.max"]
    try:
        stats = ctx.engine.iceberg_file_stats(key)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if stats.file_count == 0:
        return ok(name, f"{key}: empty", value=0.0, metric_key=key)
    if stats.small_file_ratio > max_ratio:
        return warn(
            name,
            f"{key}: {stats.small_file_ratio:.0%} small files > {max_ratio:.0%}",
            value=stats.small_file_ratio,
            metric_key=key,
        )
    return ok(
        name,
        f"{key}: {stats.small_file_ratio:.0%} small files",
        value=stats.small_file_ratio,
        metric_key=key,
    )


@check_over(
    keys=V1_ICEBERG_TABLES,
    name_template="iceberg.snapshot_count.{key}",
    layer="l3",
    tier="slow",
)
def iceberg_snapshot_count(ctx, *, key: str) -> Result:
    name = f"iceberg.snapshot_count.{key}"
    max_count = THRESHOLDS["iceberg.snapshot_count.max"]
    try:
        stats = ctx.engine.iceberg_file_stats(key)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if stats.snapshot_count > max_count:
        return warn(
            name,
            f"{key}: {stats.snapshot_count} snapshots > {max_count} (expire due)",
            value=stats.snapshot_count,
            metric_key=key,
        )
    return ok(
        name,
        f"{key}: {stats.snapshot_count} snapshots",
        value=stats.snapshot_count,
        metric_key=key,
    )


@check_over(
    keys=V1_ICEBERG_TABLES,
    name_template="iceberg.manifest_count.{key}",
    layer="l3",
    tier="slow",
)
def iceberg_manifest_count(ctx, *, key: str) -> Result:
    name = f"iceberg.manifest_count.{key}"
    max_count = THRESHOLDS["iceberg.manifest_count.max"]
    try:
        stats = ctx.engine.iceberg_file_stats(key)
    except Exception as exc:  # noqa: BLE001
        return error(name, f"engine failure: {exc}")
    if stats.manifest_count > max_count:
        return warn(
            name,
            f"{key}: {stats.manifest_count} manifests > {max_count}",
            value=stats.manifest_count,
            metric_key=key,
        )
    return ok(
        name,
        f"{key}: {stats.manifest_count} manifests",
        value=stats.manifest_count,
        metric_key=key,
    )
