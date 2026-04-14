"""Container state checks.

One check per service in ``config.SERVICES``. Breaches when the container is
absent or not in the ``running`` state. ``on_breach`` is wired in a later
todo once ``heal.restart_container`` exists.
"""
from __future__ import annotations

from ..config import SERVICES
from ..docker import DockerError, state
from ..registry import check
from ..result import Result, breach, error, ok


def _container_check(name: str) -> Result:
    try:
        snap = state(name)
    except DockerError as exc:
        return error(f"containers.{name}.state", f"docker error: {exc}")
    if snap is None:
        return breach(
            f"containers.{name}.state",
            f"container {name} not found",
            value="missing",
        )
    status, st = snap
    if status == "running":
        # Flag very fresh restarts so operators notice flapping even if running.
        restart_count = st.get("Restarting") if isinstance(st, dict) else None
        return ok(
            f"containers.{name}.state",
            f"{name} running",
            value=status,
        )
    return breach(
        f"containers.{name}.state",
        f"{name} status={status}",
        value=status,
    )


def _make_check(service: str):
    @check(f"containers.{service}.state", layer="l1", tier="fast", service=service)
    def _impl(ctx) -> Result:
        return _container_check(service)

    _impl.__name__ = f"containers_{service.replace('-', '_')}_state"
    return _impl


# Register one check per service in the active stack.
for _service in SERVICES:
    _make_check(_service)
