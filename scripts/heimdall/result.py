"""Result dataclass — the only thing a check ever returns."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Status = Literal["ok", "warn", "breach", "error", "skip"]

_SEVERITY: dict[str, int] = {"breach": 0, "error": 1, "warn": 2, "skip": 3, "ok": 4}


@dataclass(frozen=True)
class Result:
    check: str
    status: Status
    message: str
    value: float | str | None = None
    metric_key: str | None = None      # stable grouping key for templated checks
    duration_ms: int = 0
    ts: int = 0                        # unix epoch seconds, set by runner

    @property
    def severity(self) -> int:
        return _SEVERITY.get(self.status, 99)


def ok(check: str, message: str, value: float | str | None = None, **kw) -> Result:
    return Result(check=check, status="ok", message=message, value=value, **kw)


def warn(check: str, message: str, value: float | str | None = None, **kw) -> Result:
    return Result(check=check, status="warn", message=message, value=value, **kw)


def breach(check: str, message: str, value: float | str | None = None, **kw) -> Result:
    return Result(check=check, status="breach", message=message, value=value, **kw)


def error(check: str, message: str, **kw) -> Result:
    return Result(check=check, status="error", message=message, **kw)


def skip(check: str, message: str, **kw) -> Result:
    return Result(check=check, status="skip", message=message, **kw)
