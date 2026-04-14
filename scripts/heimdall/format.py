"""Small formatting helpers shared by the TUI and ``heimdall status`` rendering.

Kept deliberately minimal — number compaction, duration humanization, and
severity-from-zones lookup. Nothing here touches DuckDB or Textual so the
helpers are easy to unit-test in isolation.
"""
from __future__ import annotations

from typing import Iterable

from .registry import Zone


_SI_SUFFIX = (
    (1_000_000_000_000.0, "T"),
    (1_000_000_000.0, "B"),
    (1_000_000.0, "M"),
    (1_000.0, "K"),
)


def compact_number(value: float | int | None) -> str:
    """Format ``value`` with at most 3 significant digits + SI suffix.

    Rounding is normal (never truncation) so a number growing past a boundary
    is visibly larger. Boundary jitter (999 → 1.00K on the next tick) is
    accepted as the correct cost of readability. ``None`` renders as ``-``.
    """
    if value is None:
        return "-"
    try:
        v = float(value)
    except (TypeError, ValueError):
        return "-"
    neg = v < 0
    v = abs(v)
    for divisor, suffix in _SI_SUFFIX:
        if v >= divisor:
            scaled = v / divisor
            if scaled >= 100:
                s = f"{scaled:.0f}{suffix}"
            elif scaled >= 10:
                s = f"{scaled:.1f}{suffix}"
            else:
                s = f"{scaled:.2f}{suffix}"
            return f"-{s}" if neg else s
    # Below 1000 — render as int when whole, else 1 decimal.
    if v >= 100:
        s = f"{v:.0f}"
    elif v >= 10:
        s = f"{v:.1f}"
    elif v == int(v):
        s = f"{int(v)}"
    else:
        s = f"{v:.2f}"
    return f"-{s}" if neg else s


def humanize_seconds(s: float | None) -> str:
    if s is None:
        return "-"
    if s < 1:
        return f"{s*1000:.0f} ms"
    if s < 60:
        return f"{s:.1f} s"
    if s < 3600:
        return f"{s/60:.1f} m"
    return f"{s/3600:.1f} h"


def severity_for_value(value: float | None, zones: Iterable[Zone]) -> str:
    """Map ``value`` to a severity by walking the zone list.

    Returns ``grey`` when ``value`` is None or no zone contains it. The first
    matching zone wins — ranges are ``[min, max)`` except the final one which
    is closed on both ends to absorb the upper boundary.
    """
    if value is None:
        return "grey"
    zones_list = list(zones)
    if not zones_list:
        return "grey"
    for i, (lo, hi, sev) in enumerate(zones_list):
        if i == len(zones_list) - 1:
            if lo <= value <= hi:
                return sev
        else:
            if lo <= value < hi:
                return sev
    return "grey"


def worst_of(severities: Iterable[str]) -> str:
    """Aggregate severities under the worst-wins rule: red > orange > yellow > green > grey."""
    order = {"red": 4, "orange": 3, "yellow": 2, "green": 1, "grey": 0}
    best = "grey"
    for s in severities:
        if order.get(s, 0) > order.get(best, 0):
            best = s
    return best
