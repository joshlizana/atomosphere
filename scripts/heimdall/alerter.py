"""Append-only JSON writer for logs/alerts.log.

State-transition detection: emit one line per OK -> not-OK (alert) and per
not-OK -> OK (recovery) transition, once tolerance has been satisfied. Anything
else is silent.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable

from .result import Result, Status

_TRIGGER: set[str] = {"breach", "error"}


class Alerter:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _write(self, payload: dict) -> None:
        line = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def emit_breach(self, result: Result, emit_class: str = "silent") -> None:
        self._write(
            {
                "ts": result.ts or int(time.time()),
                "kind": "BREACH",
                "emit_class": emit_class,
                "check": result.check,
                "metric_key": result.metric_key,
                "status": result.status,
                "message": result.message,
                "value": result.value,
            }
        )

    def emit_recovery(
        self,
        result: Result,
        previous_status: Status,
        emit_class: str = "silent",
    ) -> None:
        self._write(
            {
                "ts": result.ts or int(time.time()),
                "kind": "RECOVERY",
                "emit_class": emit_class,
                "check": result.check,
                "metric_key": result.metric_key,
                "from": previous_status,
                "message": result.message,
            }
        )


def should_alert(
    *,
    current: Status,
    prior_window: Iterable[Status],
    tolerance: int,
) -> bool:
    """True exactly on the cycle that crosses the tolerance threshold.

    ``prior_window`` is the ``tolerance`` most-recent statuses *before* the
    current one, ordered newest-first. We alert when:
    - current is a trigger status
    - the ``tolerance - 1`` most-recent prior statuses are all trigger
    - the ``tolerance``-th most-recent prior status exists and is not trigger
      (i.e. we just crossed the threshold), OR there is no such prior
      (fresh start, only if ``tolerance == 1``)

    This ensures a single alert per breach episode; re-alerts are silent until
    a recovery resets the streak.
    """
    if current not in _TRIGGER:
        return False
    prior = list(prior_window)
    needed_trigger = prior[: tolerance - 1]
    if len(needed_trigger) < tolerance - 1:
        return False
    if not all(s in _TRIGGER for s in needed_trigger):
        return False
    # Gate: the cycle BEFORE this streak must not be trigger, otherwise we've
    # already alerted on this episode.
    if len(prior) >= tolerance:
        gate = prior[tolerance - 1]
        if gate in _TRIGGER:
            return False
    return True


def is_recovery(*, current: Status, previous: Status | None) -> bool:
    return current == "ok" and previous is not None and previous in _TRIGGER
