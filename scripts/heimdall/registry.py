"""Decorator-based check registry.

Importing a check module runs ``@check`` which populates ``REGISTRY``. No
explicit ``runner.add_check(...)`` calls anywhere.
"""
from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterable

from .config import DEFAULT_TOLERANCE

if TYPE_CHECKING:
    from .result import Result
    from .runner import Context

CheckFunc = Callable[["Context"], "Result"]
HealAction = Callable[[], Any]
Zone = tuple[float, float, str]  # (min, max, severity) — severity ∈ {green,orange,red,yellow}

LAYERS = ("l1", "l2", "l3")
VALID_SEVERITIES = ("green", "orange", "red", "yellow")


@dataclass
class CheckSpec:
    name: str
    func: CheckFunc
    layer: str
    tier: str = "fast"
    tolerance: int = DEFAULT_TOLERANCE
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    metric_key: str | None = None
    on_breach: HealAction | None = None
    service: str | None = None
    zones: tuple[Zone, ...] = field(default_factory=tuple)
    emit_class: str = "silent"  # intervention | attention | silent


REGISTRY: dict[str, CheckSpec] = {}


def _validate_layer(name: str, layer: str) -> None:
    if layer not in LAYERS:
        raise ValueError(
            f"check {name!r}: invalid layer {layer!r}, expected one of {LAYERS}"
        )


def _validate_zones(name: str, zones: Iterable[Zone]) -> tuple[Zone, ...]:
    out: list[Zone] = []
    for z in zones:
        if len(z) != 3:
            raise ValueError(f"check {name!r}: zone {z!r} must be (min, max, severity)")
        lo, hi, sev = z
        if sev not in VALID_SEVERITIES:
            raise ValueError(
                f"check {name!r}: zone severity {sev!r} must be one of {VALID_SEVERITIES}"
            )
        if lo > hi:
            raise ValueError(f"check {name!r}: zone min {lo} > max {hi}")
        out.append((float(lo), float(hi), sev))
    return tuple(out)


EMIT_CLASSES = ("intervention", "attention", "silent")


def _validate_emit_class(name: str, ec: str) -> None:
    if ec not in EMIT_CLASSES:
        raise ValueError(
            f"check {name!r}: invalid emit_class {ec!r}, expected one of {EMIT_CLASSES}"
        )


def check(
    name: str,
    *,
    layer: str,
    tier: str = "fast",
    tolerance: int = DEFAULT_TOLERANCE,
    depends_on: Iterable[str] = (),
    on_breach: HealAction | None = None,
    service: str | None = None,
    zones: Iterable[Zone] = (),
    emit_class: str = "silent",
) -> Callable[[CheckFunc], CheckFunc]:
    """Register a check under ``name``. Importing the module is the wire-up."""
    _validate_layer(name, layer)
    _validate_emit_class(name, emit_class)
    zones_t = _validate_zones(name, zones)

    def decorator(func: CheckFunc) -> CheckFunc:
        if name in REGISTRY:
            raise ValueError(f"duplicate check name: {name!r}")
        REGISTRY[name] = CheckSpec(
            name=name,
            func=func,
            layer=layer,
            tier=tier,
            tolerance=tolerance,
            depends_on=tuple(depends_on),
            on_breach=on_breach,
            service=service,
            zones=zones_t,
            emit_class=emit_class,
        )
        return func

    return decorator


def check_over(
    *,
    keys: Iterable[str],
    layer: str,
    name_template: str = "{func}.{key}",
    tier: str = "fast",
    tolerance: int = DEFAULT_TOLERANCE,
    depends_on: Iterable[str] = (),
    on_breach_factory: Callable[[str], HealAction] | None = None,
    service: str | None = None,
    zones: Iterable[Zone] = (),
    emit_class: str = "silent",
) -> Callable[[Callable[..., "Result"]], Callable[..., "Result"]]:
    """Expand one function into N registered checks, one per ``key``.

    The wrapped function is called as ``func(ctx, key=<key>)``. ``key`` is the
    stable ``metric_key`` stored on every Result from that instance.
    """
    _validate_layer(name_template, layer)
    _validate_emit_class(name_template, emit_class)
    zones_t = _validate_zones(name_template, zones)

    def decorator(func: Callable[..., "Result"]) -> Callable[..., "Result"]:
        for key in keys:
            name = name_template.format(func=func.__name__, key=key)
            if name in REGISTRY:
                raise ValueError(f"duplicate check name: {name!r}")
            bound = functools.partial(func, key=key)
            functools.update_wrapper(bound, func)
            REGISTRY[name] = CheckSpec(
                name=name,
                func=bound,  # type: ignore[arg-type]
                layer=layer,
                tier=tier,
                tolerance=tolerance,
                depends_on=tuple(depends_on),
                metric_key=key,
                on_breach=on_breach_factory(key) if on_breach_factory else None,
                service=service,
                zones=zones_t,
                emit_class=emit_class,
            )
        return func

    return decorator


def reset_registry() -> None:
    """Test helper — wipe the registry."""
    REGISTRY.clear()
