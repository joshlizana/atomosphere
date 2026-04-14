"""Saturation checks — 'how full is each resource'.

All saturation checks are **L2**, not L1. L1 is strictly liveness ("is the
service up"); "how full is X" is operational health and belongs one layer
up. This includes host disk: if ``/`` fills the services break, but from a
layer-gating perspective that's still a resource-fullness metric, not a
liveness signal. Host liveness at L1 is implicit — if the monitor itself is
running, the host is up.

- ``mem_ratio.<service>`` — docker stats on every service in ``config.SERVICES``
- ``gpu_mem_used_ratio`` — nvidia-smi single-shot
- ``sentiment_gpu_util`` — 5-minute rolling GPU util (renamed to ``latency.*``)
- ``seaweedfs_disk_used`` — df inside the seaweedfs container
- ``host_disk_free_gb`` — df on the host / mount
"""
from __future__ import annotations

import subprocess
from typing import Iterable

from ..config import SERVICES, THRESHOLDS
from ..docker import DockerError, exec_command, stats
from ..registry import check
from ..result import Result, breach, error, ok, warn


# --------------------------------------------------------------------------- helpers
def _parse_mem_usage(raw: str) -> tuple[float, float] | None:
    """Parse '12.3GiB / 14GiB' -> (used_bytes, limit_bytes)."""
    try:
        used_s, limit_s = [p.strip() for p in raw.split("/")]
    except ValueError:
        return None
    return _parse_bytes(used_s), _parse_bytes(limit_s)


_UNIT = {
    "B": 1,
    "KB": 1_000,
    "KIB": 1024,
    "MB": 1_000_000,
    "MIB": 1024**2,
    "GB": 1_000_000_000,
    "GIB": 1024**3,
    "TB": 10**12,
    "TIB": 1024**4,
}


def _parse_bytes(value: str) -> float:
    value = value.strip()
    for unit in sorted(_UNIT.keys(), key=len, reverse=True):
        if value.upper().endswith(unit):
            num = float(value[: -len(unit)])
            return num * _UNIT[unit]
    return float(value)


def _nvidia_smi(query: Iterable[str]) -> list[list[str]] | None:
    try:
        out = subprocess.run(
            [
                "nvidia-smi",
                f"--query-gpu={','.join(query)}",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    rows: list[list[str]] = []
    for line in out.stdout.strip().splitlines():
        rows.append([p.strip() for p in line.split(",")])
    return rows


# --------------------------------------------------------------------------- per-service mem
# One check per service in ``config.SERVICES``. Each is an L2 saturation signal
# colored green below the shared 80% limit and red above. The TUI collapses
# these into a single dynamic "mem pressure" row that only materialises when
# at least one service is hot.
_MEM_MAX = 0.80


def _mem_check(service: str):
    cname = f"saturation.mem_ratio.{service}"

    @check(
        cname,
        layer="l2",
        tier="fast",
        service=service,
        zones=((0.0, _MEM_MAX, "green"), (_MEM_MAX, 1.0, "red")),
    )
    def _impl(ctx) -> Result:
        try:
            snap = stats(service)
        except DockerError as exc:
            return error(cname, f"docker stats: {exc}")
        if snap is None:
            return breach(cname, f"{service} not running", value=None)
        mem = _parse_mem_usage(snap.get("MemUsage", ""))
        if mem is None:
            return error(cname, f"unparseable MemUsage: {snap.get('MemUsage')}")
        used, limit = mem
        if limit <= 0:
            return error(cname, "zero MemUsage limit")
        ratio = used / limit
        if ratio > _MEM_MAX:
            return breach(cname, f"{service} mem {ratio:.2%} > {_MEM_MAX:.0%}", value=ratio)
        return ok(cname, f"{service} mem {ratio:.2%}", value=ratio)

    _impl.__name__ = f"mem_ratio_{service.replace('-', '_')}"
    return _impl


for _service in SERVICES:
    _mem_check(_service)


# --------------------------------------------------------------------------- gpu mem
@check(
    "saturation.gpu_mem_used_ratio",
    layer="l2",
    tier="fast",
    service="spark",
    zones=((0.0, 0.70, "green"), (0.70, 0.85, "orange"), (0.85, 1.0, "red")),
)
def gpu_mem_used_ratio(ctx) -> Result:
    name = "saturation.gpu_mem_used_ratio"
    max_ratio = THRESHOLDS[f"{name}.max"]
    rows = _nvidia_smi(["memory.used", "memory.total"])
    if rows is None:
        return ok(name, "no GPU present", value=0.0)
    used = sum(float(r[0]) for r in rows)
    total = sum(float(r[1]) for r in rows)
    if total <= 0:
        return error(name, "nvidia-smi reported zero total memory")
    ratio = used / total
    if ratio > max_ratio:
        return breach(name, f"gpu mem {ratio:.2%} > {max_ratio:.0%}", value=ratio)
    return ok(name, f"gpu mem {ratio:.2%}", value=ratio)


# --------------------------------------------------------------------------- gpu util 5min
# L2, not L1: GPU pinned near 100% is not a service-health problem (the GPU is
# doing its job) — it's an operational-latency signal that the sentiment
# mapInPandas stage is saturated and its micro-batch queue is backing up.
# Conversely, GPU util near 0% during active ingestion means the sentiment
# worker has starved, which is also an operational problem. Both extremes are
# L2 concerns, not L1 saturation.
@check(
    "latency.sentiment_gpu_util",
    layer="l2",
    tier="fast",
    service="spark",
    zones=((0.0, 10.0, "orange"), (10.0, 95.0, "green"), (95.0, 100.0, "red")),
)
def gpu_util_5min(ctx) -> Result:
    name = "latency.sentiment_gpu_util"
    min_util = THRESHOLDS[f"{name}.min"]
    max_util = THRESHOLDS[f"{name}.max"]

    rows = _nvidia_smi(["utilization.gpu"])
    if rows is None:
        return ok(name, "no GPU present", value=0.0)
    current = sum(float(r[0]) for r in rows) / len(rows)

    # Blend with the 5-minute rolling average from history so this check
    # does not flap on a single sampled read.
    history = ctx.history("latency.sentiment_gpu_util", window_seconds=300)
    samples = [row[3] for row in history if row[3] is not None]
    samples.append(current)
    rolling = sum(samples) / len(samples)

    if rolling > max_util:
        return breach(name, f"gpu util 5min avg {rolling:.1f}% > {max_util}%", value=rolling)
    if rolling < min_util and len(samples) >= 3:
        return warn(name, f"gpu util 5min avg {rolling:.1f}% < {min_util}% (starved?)", value=rolling)
    return ok(name, f"gpu util 5min avg {rolling:.1f}%", value=rolling)


# --------------------------------------------------------------------------- seaweedfs disk
@check(
    "saturation.seaweedfs_disk_used",
    layer="l2",
    tier="fast",
    service="seaweedfs",
    zones=((0.0, 0.70, "green"), (0.70, 0.85, "orange"), (0.85, 1.0, "red")),
)
def seaweedfs_disk_used(ctx) -> Result:
    name = "saturation.seaweedfs_disk_used"
    max_ratio = THRESHOLDS[f"{name}.max"]
    try:
        out = exec_command("seaweedfs", ["df", "-P", "/data"])
    except DockerError as exc:
        return error(name, f"docker exec: {exc}")
    lines = out.strip().splitlines()
    if len(lines) < 2:
        return error(name, f"unexpected df output: {out!r}")
    fields = lines[-1].split()
    try:
        used_pct = int(fields[4].rstrip("%")) / 100.0
    except (IndexError, ValueError):
        return error(name, f"unparseable df row: {fields}")
    if used_pct > max_ratio:
        return breach(name, f"seaweedfs /data {used_pct:.0%} > {max_ratio:.0%}", value=used_pct)
    return ok(name, f"seaweedfs /data {used_pct:.0%}", value=used_pct)


# --------------------------------------------------------------------------- host disk
@check(
    "saturation.host_disk_free_gb",
    layer="l2",
    tier="fast",
    service="host",
    zones=((0.0, 10.0, "red"), (10.0, 25.0, "orange"), (25.0, 10_000.0, "green")),
)
def host_disk_free_gb(ctx) -> Result:
    name = "saturation.host_disk_free_gb"
    min_gb = THRESHOLDS[f"{name}.min"]
    try:
        out = subprocess.run(
            ["df", "-P", "-BG", "/"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except FileNotFoundError:
        return error(name, "df not on PATH")
    lines = out.stdout.strip().splitlines()
    if len(lines) < 2:
        return error(name, f"unexpected df output: {out.stdout!r}")
    fields = lines[-1].split()
    try:
        avail_gb = float(fields[3].rstrip("G"))
    except (IndexError, ValueError):
        return error(name, f"unparseable df row: {fields}")
    if avail_gb < min_gb:
        return breach(name, f"host / free {avail_gb:.0f}G < {min_gb}G", value=avail_gb)
    return ok(name, f"host / free {avail_gb:.0f}G", value=avail_gb)


# --------------------------------------------------------------------------- host cpu
# 5s delta across two /proc/stat reads. Single-shot because cpu% is meaningful
# even without rolling history, and the runner cycle itself provides the
# cadence. If the first read is cached on the ctx, subsequent checks could
# share it; for v1 we just pay the 100ms nap per cycle.
_PROC_STAT = "/proc/stat"


def _read_cpu_ticks() -> tuple[int, int] | None:
    try:
        with open(_PROC_STAT) as fh:
            line = fh.readline()
    except OSError:
        return None
    if not line.startswith("cpu "):
        return None
    parts = line.split()[1:]
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    # Fields (Linux): user nice system idle iowait irq softirq steal guest guest_nice
    idle = nums[3] + (nums[4] if len(nums) > 4 else 0)
    total = sum(nums)
    return total, idle


@check(
    "saturation.host_cpu_used",
    layer="l2",
    tier="fast",
    service="host",
    zones=((0.0, 0.70, "green"), (0.70, 0.90, "orange"), (0.90, 1.0, "red")),
)
def host_cpu_used(ctx) -> Result:
    import time as _time

    name = "saturation.host_cpu_used"
    max_ratio = THRESHOLDS[f"{name}.max"]
    a = _read_cpu_ticks()
    if a is None:
        return error(name, f"cannot read {_PROC_STAT}")
    _time.sleep(0.1)
    b = _read_cpu_ticks()
    if b is None:
        return error(name, f"cannot read {_PROC_STAT}")
    d_total = b[0] - a[0]
    d_idle = b[1] - a[1]
    if d_total <= 0:
        return ok(name, "cpu 0%", value=0.0)
    used = 1.0 - (d_idle / d_total)
    if used > max_ratio:
        return breach(name, f"host cpu {used:.0%} > {max_ratio:.0%}", value=used)
    return ok(name, f"host cpu {used:.0%}", value=used)


# --------------------------------------------------------------------------- host memory
# MemAvailable is the Linux-kernel-blessed "how much can a new workload grab
# without swapping" — it already accounts for reclaimable cache/buffers, so
# we don't have to roll our own (MemTotal - MemFree - Buffers - Cached) math.
_PROC_MEMINFO = "/proc/meminfo"


def _read_meminfo() -> dict[str, int] | None:
    out: dict[str, int] = {}
    try:
        with open(_PROC_MEMINFO) as fh:
            for line in fh:
                key, _, rest = line.partition(":")
                parts = rest.strip().split()
                if not parts:
                    continue
                try:
                    kb = int(parts[0])
                except ValueError:
                    continue
                out[key] = kb * 1024
    except OSError:
        return None
    return out


@check(
    "saturation.host_mem_used",
    layer="l2",
    tier="fast",
    service="host",
    zones=((0.0, 0.80, "green"), (0.80, 0.95, "orange"), (0.95, 1.0, "red")),
)
def host_mem_used(ctx) -> Result:
    name = "saturation.host_mem_used"
    max_ratio = THRESHOLDS[f"{name}.max"]
    info = _read_meminfo()
    if info is None:
        return error(name, f"cannot read {_PROC_MEMINFO}")
    total = info.get("MemTotal")
    available = info.get("MemAvailable")
    if not total or available is None:
        return error(name, "missing MemTotal/MemAvailable")
    used = 1.0 - (available / total)
    if used > max_ratio:
        return breach(name, f"host mem {used:.0%} > {max_ratio:.0%}", value=used)
    return ok(name, f"host mem {used:.0%}", value=used)


# --------------------------------------------------------------------------- gpu temperature
@check(
    "saturation.gpu_temp_c",
    layer="l2",
    tier="fast",
    service="spark",
    zones=((0.0, 75.0, "green"), (75.0, 85.0, "orange"), (85.0, 120.0, "red")),
)
def gpu_temp_c(ctx) -> Result:
    name = "saturation.gpu_temp_c"
    max_c = THRESHOLDS[f"{name}.max"]
    rows = _nvidia_smi(["temperature.gpu"])
    if rows is None:
        return ok(name, "no GPU present", value=0.0)
    temps = [float(r[0]) for r in rows]
    hottest = max(temps)
    if hottest > max_c:
        return breach(name, f"gpu {hottest:.0f}C > {max_c:.0f}C", value=hottest)
    return ok(name, f"gpu {hottest:.0f}C", value=hottest)
