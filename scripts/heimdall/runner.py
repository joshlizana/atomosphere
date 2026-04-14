"""Runner — executes one cycle of every registered check.

Sequential in the first pass; upgraded to ThreadPoolExecutor in the next todo.
Handles: result persistence, tolerance-gated alerting, recovery detection,
dependency-based skipping, and on_breach heal invocation.
"""
from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from typing import Any

from . import config
from .alerter import Alerter, is_recovery, should_alert
from .registry import REGISTRY, CheckSpec
from .result import Result, error, skip
from .store import Store


@dataclass
class Context:
    """Per-cycle handles passed to every check. Lazy where possible."""

    store: Store
    alerter: Alerter
    started_at: float = field(default_factory=time.time)
    _docker = None
    _spark_progress = None
    _extras: dict[str, Any] = field(default_factory=dict)

    @property
    def docker(self):
        if self._docker is None:
            from . import docker as docker_mod

            self._docker = docker_mod
        return self._docker

    @property
    def spark_progress(self):
        if self._spark_progress is None:
            from . import spark_progress as sp

            self._spark_progress = sp.fetch()
        return self._spark_progress

    def history(self, check_name: str, window_seconds: int):
        return self.store.history(check_name, window_seconds)


@dataclass
class CycleSummary:
    counts: dict[str, int]
    duration_ms: int
    breach_names: list[str]
    error_names: list[str]

    def exit_code(self) -> int:
        if self.error_names:
            return 2
        if self.breach_names:
            return 1
        return 0

    def to_json(self) -> str:
        return json.dumps(
            {
                "duration_ms": self.duration_ms,
                "counts": self.counts,
                "breaches": self.breach_names,
                "errors": self.error_names,
                "ts": int(time.time()),
            },
            separators=(",", ":"),
            sort_keys=True,
        )


def _run_one(spec: CheckSpec, ctx: Context) -> Result:
    started = time.time()
    try:
        res: Result = spec.func(ctx)
    except Exception as exc:  # noqa: BLE001
        duration = int((time.time() - started) * 1000)
        return error(spec.name, f"exception: {exc.__class__.__name__}: {exc}", duration_ms=duration)
    duration = int((time.time() - started) * 1000)
    # Re-stamp fields the check may have forgotten.
    return Result(
        check=res.check or spec.name,
        status=res.status,
        message=res.message,
        value=res.value,
        metric_key=res.metric_key or spec.metric_key,
        duration_ms=duration,
        ts=int(time.time()),
    )


class Runner:
    def __init__(self, store: Store | None = None, alerter: Alerter | None = None) -> None:
        config.ensure_dirs()
        self.store = store or Store(config.WAL_PATH)
        self.alerter = alerter or Alerter(config.ALERTS_LOG)

    # --------------------------------------------------------------- run cycle
    def run_once(self) -> CycleSummary:
        start = time.time()
        counts = {"ok": 0, "warn": 0, "breach": 0, "error": 0, "skip": 0}
        breach_names: list[str] = []
        error_names: list[str] = []

        ctx = Context(store=self.store, alerter=self.alerter)
        breaching_by_layer = self._currently_breaching_by_layer()
        breaching_all: set[str] = set()
        for names in breaching_by_layer.values():
            breaching_all |= names

        # Order so L1 runs before L2 runs before L3; within a layer, REGISTRY
        # insertion order is preserved. Live breaches from *this* cycle also
        # count toward the layer gate, not just the store snapshot.
        specs = sorted(REGISTRY.values(), key=lambda s: (s.layer, s.name))
        live_breaches: dict[str, set[str]] = {"l1": set(), "l2": set(), "l3": set()}
        for layer, names in breaching_by_layer.items():
            live_breaches[layer] |= names

        for spec in specs:
            # Layer gate: L2 skipped if any L1 breach; L3 skipped if any L1 or L2 breach.
            gate_reason: str | None = None
            if spec.layer == "l2" and live_breaches["l1"]:
                gate_reason = "layer gate: L1 breached"
            elif spec.layer == "l3" and (live_breaches["l1"] or live_breaches["l2"]):
                gate_reason = "layer gate: L1/L2 breached"

            if gate_reason is not None:
                result = Result(
                    check=spec.name,
                    status="skip",
                    message=gate_reason,
                    duration_ms=0,
                    ts=int(time.time()),
                )
            elif any(dep in breaching_all for dep in spec.depends_on):
                result = skip(spec.name, f"dep breaching: {','.join(spec.depends_on)}")
                result = Result(
                    check=spec.name,
                    status="skip",
                    message=result.message,
                    duration_ms=0,
                    ts=int(time.time()),
                )
            else:
                result = _run_one(spec, ctx)

            if result.status in ("breach", "error"):
                live_breaches[spec.layer].add(spec.name)

            self._handle_result(spec, result, counts, breach_names, error_names)

        self.store.prune(config.RESULT_RETAIN_DAYS)

        summary = CycleSummary(
            counts=counts,
            duration_ms=int((time.time() - start) * 1000),
            breach_names=breach_names,
            error_names=error_names,
        )
        sys.stdout.write(summary.to_json() + "\n")
        sys.stdout.flush()
        return summary

    # --------------------------------------------------------------- helpers
    def _currently_breaching_by_layer(self) -> dict[str, set[str]]:
        """Breach/error names grouped by layer from the last-seen snapshot."""
        out: dict[str, set[str]] = {"l1": set(), "l2": set(), "l3": set()}
        for row in self.store.snapshot():
            name, _metric, status = row[0], row[1], row[2]
            if status not in ("breach", "error"):
                continue
            spec = REGISTRY.get(name)
            if spec is None:
                continue
            out.setdefault(spec.layer, set()).add(name)
        return out

    def _handle_result(
        self,
        spec: CheckSpec,
        result: Result,
        counts: dict[str, int],
        breach_names: list[str],
        error_names: list[str],
    ) -> None:
        prior = self.store.last_result(spec.name)
        prior_status = prior[1] if prior else None

        # Persist first so the alerter can read its own writes on recovery.
        self.store.record(result)

        counts[result.status] = counts.get(result.status, 0) + 1
        if result.status == "breach":
            breach_names.append(spec.name)
        elif result.status == "error":
            error_names.append(spec.name)

        # Recovery on transition back to OK.
        if is_recovery(current=result.status, previous=prior_status):
            self.alerter.emit_recovery(
                result,
                previous_status=prior_status or "breach",
                emit_class=spec.emit_class,
            )
            return

        # Breach alerting once tolerance is met.
        if result.status in ("breach", "error"):
            # Fetch tolerance+1 rows; first row is the result we just inserted,
            # so strip it and pass the prior window to should_alert.
            recent = self.store.last_n_statuses(spec.name, spec.tolerance + 1)
            prior_window = recent[1:]
            if should_alert(
                current=result.status,
                prior_window=prior_window,
                tolerance=spec.tolerance,
            ):
                self.alerter.emit_breach(result, emit_class=spec.emit_class)
                self._invoke_heal(spec, result)

    def _invoke_heal(self, spec: CheckSpec, result: Result) -> None:
        if spec.on_breach is None:
            return
        target = result.metric_key or spec.name
        recent = self.store.recent_heals(target, config.HEAL_RATE_WINDOW_S)
        if recent >= config.HEAL_RATE_LIMIT:
            self.store.record_heal(target, "unknown", spec.name, "skipped_rate_limit")
            return
        try:
            outcome = spec.on_breach()
            self.store.record_heal(target, "heal", spec.name, str(outcome) if outcome else "success")
        except Exception as exc:  # noqa: BLE001
            self.store.record_heal(target, "heal", spec.name, f"error: {exc}")
