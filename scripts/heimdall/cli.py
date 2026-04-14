"""Heimdall CLI (Typer).

First pass ships only ``run``; status/history/alerts/heal/list/tui come in
later todos.
"""
from __future__ import annotations

import sys

import typer

from . import config
from .runner import Runner

app = typer.Typer(
    help="Heimdall — Atmosphere monitor. See reference/heimdall-plan.md.",
    no_args_is_help=False,
    add_completion=False,
)


@app.callback(invoke_without_command=True)
def _default(ctx: typer.Context) -> None:
    """If invoked with no subcommand, behave as ``heimdall run``."""
    if ctx.invoked_subcommand is None:
        run()


@app.command()
def run() -> None:
    """Execute one cycle of every registered check."""
    # Import check modules so decorators register. Done lazily to keep import
    # of individual subcommands cheap.
    from . import checks  # noqa: F401

    runner = Runner()
    summary = runner.run_once()
    raise typer.Exit(code=summary.exit_code())


@app.command("list")
def list_checks() -> None:
    """List every registered check name."""
    from . import checks  # noqa: F401
    from .registry import REGISTRY

    for name in sorted(REGISTRY.keys()):
        typer.echo(name)


@app.command()
def tui() -> None:
    """Launch the Textual dashboard (interactive, foreground)."""
    try:
        from .tui import run_tui
    except ImportError as exc:
        typer.echo(
            f"heimdall tui not yet implemented ({exc}). "
            "Use `make heimdall-run` for a one-shot cycle.",
            err=True,
        )
        raise typer.Exit(code=3)
    run_tui()


@app.command()
def watch(interval: float = typer.Option(10.0, help="Seconds between cycles.")) -> None:
    """Headless loop — one cycle every ``interval`` seconds until Ctrl-C."""
    import time

    from . import checks  # noqa: F401

    runner = Runner()
    try:
        while True:
            runner.run_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        raise typer.Exit(code=0)


if __name__ == "__main__":
    app()
