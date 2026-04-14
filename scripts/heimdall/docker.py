"""Thin wrapper over the ``docker`` CLI.

Heimdall intentionally avoids docker-py to keep the dependency set small and
to match how a human would debug: everything it does is reproducible with a
plain ``docker`` invocation.
"""
from __future__ import annotations

import json
import subprocess
from typing import Any


class DockerError(RuntimeError):
    pass


def _run(args: list[str], *, timeout: int = 15) -> str:
    try:
        proc = subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise DockerError("docker CLI not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise DockerError(f"docker {' '.join(args)} timed out after {timeout}s") from exc
    if proc.returncode != 0:
        raise DockerError(proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}")
    return proc.stdout


def inspect(name: str) -> dict[str, Any] | None:
    """Return the first element of ``docker inspect`` or None if not found."""
    try:
        out = _run(["inspect", name])
    except DockerError as exc:
        if "No such object" in str(exc):
            return None
        raise
    data = json.loads(out)
    return data[0] if data else None


def state(name: str) -> tuple[str, dict[str, Any]] | None:
    """Return (state_string, state_dict) or None if container absent.

    ``state_string`` is docker's canonical ``State.Status`` value: ``running``,
    ``exited``, ``restarting``, ``paused``, ``dead``, ``created``.
    """
    info = inspect(name)
    if info is None:
        return None
    st = info.get("State", {}) or {}
    return st.get("Status", "unknown"), st


def stats(name: str) -> dict[str, Any] | None:
    """Single-shot ``docker stats``. Returns None if container absent."""
    try:
        out = _run(
            ["stats", "--no-stream", "--format", "{{json .}}", name],
            timeout=10,
        )
    except DockerError as exc:
        if "No such container" in str(exc) or "not found" in str(exc).lower():
            return None
        raise
    line = out.strip().splitlines()[-1] if out.strip() else ""
    return json.loads(line) if line else None


def restart(name: str, *, timeout: int = 30) -> None:
    _run(["restart", name], timeout=timeout + 5)


def exec_command(name: str, cmd: list[str], *, timeout: int = 15) -> str:
    return _run(["exec", name, *cmd], timeout=timeout)
