"""Heimdall Textual TUI — three-band dashboard + modal queue.

Entry point is ``run_tui()`` (also reachable via ``heimdall tui`` /
``make heimdall``). The app polls the Store every second on the UI thread
and pushes the background runner onto a worker thread when the operator
presses ``r``. All modal screens live in this single file so the layout,
color rules, and check-grouping logic stay in one place.

See ``reference/heimdall-tui-design.md`` for the design contract this
module implements.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable

from rich import box
from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Header, Static

from . import checks as _checks  # noqa: F401 — import-time check registration
from . import config
from .format import compact_number, humanize_seconds, severity_for_value, worst_of
from .registry import REGISTRY, CheckSpec
from .runner import Runner
from .store import Store


# --------------------------------------------------------------------------- display aliases
DISPLAY_NAMES: dict[str, str] = {
    # Render-time aliases. The container + check tag stays as the source name;
    # the TUI swaps to the friendly name when drawing rows.
    "spark-unified": "spark",
}


def display(service: str | None) -> str:
    if service is None:
        return "(unassigned)"
    return DISPLAY_NAMES.get(service, service)


# --------------------------------------------------------------------------- color mapping
# Rich/Textual color tokens. These match the One Dark palette used elsewhere
# in the design doc. Colorblind-safe pairing relies on glyphs + labels too.
SEVERITY_STYLE = {
    "red": "bold red",
    "orange": "bold #d19a66",
    "yellow": "bold #e5c07b",
    "green": "bold #98c379",
    "grey": "dim white",
}


def style_for(sev: str) -> str:
    return SEVERITY_STYLE.get(sev, "white")


def severity_from_status(status: str) -> str:
    """Fallback severity when a check has no zones."""
    return {
        "ok": "green",
        "warn": "orange",
        "breach": "red",
        "error": "red",
        "skip": "grey",
    }.get(status, "grey")


def resolved_severity(spec: CheckSpec, status: str, value_num: float | None) -> str:
    """Prefer zone-based severity when zones + numeric value are present."""
    if spec.zones and value_num is not None:
        return severity_for_value(value_num, spec.zones)
    return severity_from_status(status)


# --------------------------------------------------------------------------- snapshot model
@dataclass
class CheckRow:
    spec: CheckSpec
    status: str
    message: str
    value_num: float | None
    value_str: str | None
    ts: int
    severity: str


def load_rows(store: Store) -> dict[str, CheckRow]:
    """Pull the latest result per check and attach its spec + computed severity."""
    out: dict[str, CheckRow] = {}
    for row in store.snapshot():
        name, metric_key, status, message, value_num, value_str, duration_ms, ts = row
        spec = REGISTRY.get(name)
        if spec is None:
            continue
        sev = resolved_severity(spec, status, value_num)
        out[name] = CheckRow(
            spec=spec,
            status=status,
            message=message or "",
            value_num=value_num,
            value_str=value_str,
            ts=int(ts),
            severity=sev,
        )
    return out


# --------------------------------------------------------------------------- L1 band
class L1Band(Static):
    """Single-line liveness row — each service name colored by worst-of severity.

    L1 is glance-only: one character per service would be enough, but showing
    the service name trades a little width for legibility. Drill-in surfaces
    the state/http split when an operator needs to know *why* a name is red.
    """

    rows_data: reactive[dict[str, CheckRow]] = reactive({}, always_update=True)
    header_severity: reactive[str] = reactive("green")

    def render(self) -> Text:
        rows = self.rows_data or {}
        by_service: dict[str, list[CheckRow]] = {}
        for cr in rows.values():
            if cr.spec.layer != "l1":
                continue
            svc = cr.spec.service or "(unassigned)"
            by_service.setdefault(svc, []).append(cr)

        ordering = list(config.SERVICES) + ["host"]
        sorted_keys = [s for s in ordering if s in by_service] + [
            s for s in sorted(by_service.keys()) if s not in ordering
        ]

        text = Text()
        header_severities: list[str] = []

        header_sev = self.header_severity
        glyph = "\u2717" if header_sev == "red" else ("!" if header_sev == "orange" else "")
        text.append(
            f"L1 \u2500\u2500 liveness{' ' + glyph if glyph else ''}   ",
            style=style_for(header_sev),
        )

        for i, svc in enumerate(sorted_keys):
            svc_checks = by_service[svc]
            svc_sev = worst_of(c.severity for c in svc_checks)
            header_severities.append(svc_sev)
            if i > 0:
                text.append("  ", style="default")
            text.append(display(svc), style=style_for(svc_sev))
        text.append("\n")

        self.header_severity = worst_of(header_severities) if header_severities else "green"
        return text


# --------------------------------------------------------------------------- L2 band
class L2Band(Static):
    """Colored-number row of streaming headline metrics."""

    rows_data: reactive[dict[str, CheckRow]] = reactive({}, always_update=True)
    gated: reactive[bool] = reactive(False)
    gated_reason: reactive[str] = reactive("")

    # Curated order + friendly labels for known L2 checks. Unknown L2 checks
    # fall through as "name value".
    # Grouped by pipeline stage so related signals sit together: when
    # ``sentiment lag`` breaches the operator sees ``gpu util`` and ``gpu
    # mem`` right underneath (the usual culprit). ``failed queries`` lives
    # in resources as a spark-wide residual.
    L2_GROUPS: tuple[tuple[str, tuple[tuple[str, str], ...]], ...] = (
        (
            "ingest",
            (
                ("traffic.jetstream_input_rps", "raw/s"),
                ("latency.jetstream_to_raw", "batch age"),
            ),
        ),
        (
            "pipeline lag",
            (
                ("latency.pipeline.post", "post"),
                ("latency.pipeline.like", "like"),
                ("latency.pipeline.repost", "repost"),
                ("latency.pipeline.follow", "follow"),
                ("latency.pipeline.block", "block"),
                ("latency.pipeline.profile", "profile"),
            ),
        ),
        (
            "sentiment",
            (
                ("latency.sentiment.lag", "lag"),
                ("traffic.sentiment_rps", "rows/s"),
                ("saturation.sentiment_backlog", "backlog"),
                ("latency.sentiment_gpu_util", "gpu util"),
                ("saturation.gpu_mem_used_ratio", "gpu mem"),
            ),
        ),
        (
            "resources",
            (
                ("saturation.host_cpu_used", "host cpu"),
                ("saturation.host_mem_used", "host mem"),
                ("saturation.host_disk_free_gb", "host free"),
                ("saturation.seaweedfs_disk_used", "sfs disk"),
                ("saturation.gpu_temp_c", "gpu temp"),
            ),
        ),
    )

    @property
    def L2_ORDER(self) -> tuple[tuple[str, str], ...]:
        """Flat (check_name, label) tuple derived from groups. Kept for any
        code that still wants ungrouped access."""
        return tuple(entry for _, items in self.L2_GROUPS for entry in items)

    def render(self) -> RenderableType:
        rows = self.rows_data or {}
        header_sev = "grey" if self.gated else "green"
        header_suffix = (
            f"         \u2191 gated \u00b7 {self.gated_reason}" if self.gated else ""
        )

        if not self.gated:
            l2_sevs = [r.severity for r in rows.values() if r.spec.layer == "l2"]
            if l2_sevs:
                header_sev = worst_of(l2_sevs)

        header = Text()
        glyph = "\u2717" if header_sev == "red" else ("!" if header_sev == "orange" else "")
        header.append(
            f"L2 \u2500\u2500 operational health{header_suffix}{' ' + glyph if glyph else ''}",
            style=style_for(header_sev),
        )

        # Horizontal layout: one metric|value pair per group, side by side.
        # Columns are [ingest_m, ingest_v, sentiment_m, sentiment_v,
        # resources_m, resources_v] so Rich draws a vertical divider between
        # every pair (box.SIMPLE_HEAD produces those thin group rules).
        table = Table(
            box=box.SIMPLE,
            show_header=False,
            expand=False,
            pad_edge=False,
            padding=(0, 1),
            collapse_padding=False,
        )
        for i, (_, entries) in enumerate(self.L2_GROUPS):
            widest_label = max(len(label) for _, label in entries)
            table.add_column(
                f"m{i}",
                justify="left",
                no_wrap=True,
                min_width=widest_label + 2,
            )
            table.add_column(
                f"v{i}",
                justify="right",
                no_wrap=True,
                min_width=8,
            )

        def cell(check_name: str, label: str) -> tuple[Text, Text]:
            cr = rows.get(check_name)
            if cr is None:
                val, sev = "-", "grey"
            elif self.gated or cr.status == "skip":
                val, sev = "-", "grey"
            else:
                val = _render_l2_value(check_name, cr)
                sev = cr.severity
            return (
                Text(f"  {label}", style="default"),
                Text(val, style=style_for(sev)),
            )

        # Row 0: bold group-name headers spanning their metric cells.
        header_cells: list[Text] = []
        for _, entries in self.L2_GROUPS:
            group_name_idx = self.L2_GROUPS.index(
                next(g for g in self.L2_GROUPS if g[1] is entries)
            )
            group_name = self.L2_GROUPS[group_name_idx][0]
            if self.gated:
                group_sev = "grey"
            else:
                group_checks = [rows.get(n) for n, _ in entries]
                group_sevs = [c.severity for c in group_checks if c is not None]
                group_sev = worst_of(group_sevs) if group_sevs else "grey"
            header_cells.append(
                Text(group_name, style=f"bold {style_for(group_sev)}")
            )
            header_cells.append(Text(""))
        table.add_row(*header_cells, end_section=True)

        # Data rows: pad the shorter groups with empty cells so each row has
        # one cell per (group, metric-slot). Row count = max group length.
        max_rows = max(len(entries) for _, entries in self.L2_GROUPS)
        for row_idx in range(max_rows):
            row_cells: list[Text] = []
            for _, entries in self.L2_GROUPS:
                if row_idx < len(entries):
                    check_name, label = entries[row_idx]
                    m, v = cell(check_name, label)
                    row_cells.append(m)
                    row_cells.append(v)
                else:
                    row_cells.append(Text(""))
                    row_cells.append(Text(""))
            table.add_row(*row_cells)

        # Dynamic mem-pressure section: only renders when at least one
        # ``saturation.mem_ratio.<service>`` check is in a non-green zone
        # (threshold is 80% inside the check, so severity == "red").
        parts: list[RenderableType] = [header, table]
        if not self.gated:
            hot: list[tuple[str, CheckRow]] = []
            for cname, cr in rows.items():
                if not cname.startswith("saturation.mem_ratio."):
                    continue
                if cr.severity == "red":
                    hot.append((cname, cr))
            if hot:
                hot.sort(key=lambda item: item[1].value_num or 0.0, reverse=True)
                mem_header = Text()
                mem_header.append(
                    "\u2500\u2500 memory pressure \u2500\u2500 \u2717",
                    style="bold " + style_for("red"),
                )
                parts.append(mem_header)
                mem_table = Table(
                    box=box.SIMPLE,
                    show_header=False,
                    expand=False,
                    pad_edge=False,
                    padding=(0, 1),
                )
                mem_table.add_column("service", justify="left", no_wrap=True)
                mem_table.add_column("usage", justify="right", no_wrap=True)
                for cname, cr in hot:
                    service = cr.spec.service or cname.rsplit(".", 1)[-1]
                    pct = (cr.value_num or 0.0) * 100
                    mem_table.add_row(
                        Text(f"  {service}", style="default"),
                        Text(f"{pct:.0f}%", style=style_for(cr.severity)),
                    )
                parts.append(mem_table)

        return Group(*parts)


def _render_l2_value(check_name: str, cr: CheckRow) -> str:
    if cr.value_num is None:
        return cr.value_str or "-"
    if check_name == "traffic.jetstream_input_rps":
        return compact_number(cr.value_num)
    if check_name == "latency.jetstream_to_raw":
        return humanize_seconds(cr.value_num)
    if check_name.startswith("latency.pipeline."):
        return humanize_seconds(cr.value_num)
    if check_name == "latency.sentiment.lag":
        return humanize_seconds(cr.value_num)
    if check_name == "traffic.sentiment_rps":
        return compact_number(cr.value_num)
    if check_name == "saturation.sentiment_backlog":
        return f"{int(cr.value_num)}"
    if check_name == "latency.sentiment_gpu_util":
        return f"{cr.value_num:.0f}%"
    if check_name == "errors.streaming_query_state":
        return f"{int(cr.value_num)}"
    if check_name == "saturation.gpu_mem_used_ratio":
        return f"{cr.value_num * 100:.0f}%"
    if check_name == "saturation.seaweedfs_disk_used":
        return f"{cr.value_num * 100:.0f}%"
    if check_name == "saturation.host_disk_free_gb":
        return f"{cr.value_num:.0f}G"
    if check_name == "saturation.host_cpu_used":
        return f"{cr.value_num * 100:.0f}%"
    if check_name == "saturation.host_mem_used":
        return f"{cr.value_num * 100:.0f}%"
    if check_name == "saturation.gpu_temp_c":
        return f"{cr.value_num:.0f}C"
    return compact_number(cr.value_num)


# --------------------------------------------------------------------------- L3 band
class L3Band(Static):
    """Disabled placeholder until the L3 DuckDB reader lands."""

    gated: reactive[bool] = reactive(False)
    gated_reason: reactive[str] = reactive("")

    def render(self) -> Text:
        text = Text()
        if self.gated:
            text.append(
                f"L3 \u2500\u2500 data fidelity         \u2191 gated \u00b7 {self.gated_reason}\n",
                style=style_for("grey"),
            )
        else:
            text.append(
                "L3 \u2500\u2500 data fidelity        (deferred \u2014 DuckDB reader pending)\n",
                style=style_for("grey"),
            )
        return text


# --------------------------------------------------------------------------- modal screens
class InterventionModal(ModalScreen[str]):
    """Red-bordered modal that requires an explicit decision."""

    BINDINGS = [
        Binding("a", "accept", "accept"),
        Binding("e", "escalate", "escalate"),
        Binding("n", "next", "next"),
        Binding("d", "details", "details"),
    ]

    DEFAULT_CSS = """
    InterventionModal {
        align: center middle;
    }
    InterventionModal > Container {
        width: 70;
        height: auto;
        border: thick red;
        background: $surface;
        padding: 1 2;
    }
    """

    def __init__(self, modal_id: int, title: str, body: dict) -> None:
        super().__init__()
        self.modal_id = modal_id
        self.title_text = title
        self.body = body

    def compose(self) -> ComposeResult:
        yield Container(
            Static(
                Text.assemble(
                    ("INTERVENTION \u2500 ", "bold red"),
                    (self.title_text, "bold"),
                )
            ),
            Static("\n" + self._format_body() + "\n"),
            Static("[a] accept     [e] escalate     [d] details     [n] next", classes="footer"),
        )

    def _format_body(self) -> str:
        lines = []
        for k, v in self.body.items():
            if isinstance(v, list):
                lines.append(f"{k}:")
                for item in v:
                    lines.append(f"  \u2022 {item}")
            else:
                lines.append(f"{k}: {v}")
        return "\n".join(lines)

    def action_accept(self) -> None:
        self.dismiss("accept")

    def action_escalate(self) -> None:
        self.dismiss("escalate")

    def action_next(self) -> None:
        self.dismiss("next")

    def action_details(self) -> None:
        # Drill-in placeholder: stay in the modal for now.
        pass


class AttentionModal(ModalScreen[str]):
    """Yellow-bordered modal that only needs acknowledgment."""

    BINDINGS = [
        Binding("enter", "dismiss_ack", "dismiss"),
        Binding("n", "next", "next"),
        Binding("d", "details", "details"),
    ]

    DEFAULT_CSS = """
    AttentionModal {
        align: center middle;
    }
    AttentionModal > Container {
        width: 70;
        height: auto;
        border: thick yellow;
        background: $surface;
        padding: 1 2;
    }
    """

    def __init__(self, modal_id: int, title: str, body: dict) -> None:
        super().__init__()
        self.modal_id = modal_id
        self.title_text = title
        self.body = body

    def compose(self) -> ComposeResult:
        yield Container(
            Static(
                Text.assemble(
                    ("ATTENTION \u2500 ", "bold #e5c07b"),
                    (self.title_text, "bold"),
                )
            ),
            Static("\n" + self._format_body() + "\n"),
            Static("[enter] dismiss     [d] details     [n] next", classes="footer"),
        )

    def _format_body(self) -> str:
        return "\n".join(f"{k}: {v}" for k, v in self.body.items())

    def action_dismiss_ack(self) -> None:
        self.dismiss("ack")

    def action_next(self) -> None:
        self.dismiss("next")

    def action_details(self) -> None:
        pass


class DrillInModal(ModalScreen[None]):
    """Grey-bordered historical view of a single check."""

    BINDINGS = [
        Binding("enter", "close", "close"),
        Binding("q", "close", "close"),
        Binding("escape", "close", "close"),
    ]

    DEFAULT_CSS = """
    DrillInModal {
        align: center middle;
    }
    DrillInModal > Container {
        width: 80;
        height: auto;
        max-height: 30;
        border: thick #5c6370;
        background: $surface;
        padding: 1 2;
    }
    """

    def __init__(self, check_name: str, history: list[tuple]) -> None:
        super().__init__()
        self.check_name = check_name
        self.history = history

    def compose(self) -> ComposeResult:
        yield Container(
            Static(Text.assemble(("DETAILS \u2500 ", "bold #5c6370"), (self.check_name, "bold"))),
            Static("\n" + self._format_history() + "\n"),
            Static("[enter/q/esc] close", classes="footer"),
        )

    def _format_history(self) -> str:
        if not self.history:
            return "(no history)"
        lines = []
        for ts, status, message, value_num, value_str, duration_ms in self.history[:20]:
            when = time.strftime("%H:%M:%S", time.localtime(ts))
            val = (
                f"{value_num:.2f}"
                if value_num is not None
                else (value_str or "-")
            )
            lines.append(f"  {when}  {status:<6}  {val:>10}  {message}")
        return "\n".join(lines)

    def action_close(self) -> None:
        self.dismiss(None)


class FailedQueriesModal(ModalScreen[None]):
    """Lists the names of currently-failed streaming queries.

    Reads the message field of the most recent ``errors.streaming_query_state``
    row — the check already encodes the failed query names there as
    ``"<n> failed queries: <name1>, <name2>, ..."``.
    """

    BINDINGS = [
        Binding("enter", "close", "close"),
        Binding("q", "close", "close"),
        Binding("escape", "close", "close"),
    ]

    DEFAULT_CSS = """
    FailedQueriesModal {
        align: center middle;
    }
    FailedQueriesModal > Container {
        width: 60;
        height: auto;
        max-height: 20;
        border: thick red;
        background: $surface;
        padding: 1 2;
    }
    """

    def __init__(self, count: int, names: list[str], last_ts: int) -> None:
        super().__init__()
        self.count = count
        self.names = names
        self.last_ts = last_ts

    def compose(self) -> ComposeResult:
        title = Text.assemble(
            ("FAILED QUERIES \u2500 ", "bold red"),
            (f"{self.count} streaming queries in failed state", "bold"),
        )
        when = time.strftime("%H:%M:%S", time.localtime(self.last_ts))
        if self.names:
            body = "\n".join(f"  \u2022 {n}" for n in self.names)
        else:
            body = "  (no detail in last result message)"
        yield Container(
            Static(title),
            Static(f"\nas of {when}\n"),
            Static(body + "\n"),
            Static("[enter/q/esc] close", classes="footer"),
        )

    def action_close(self) -> None:
        self.dismiss(None)


def _parse_failed_query_names(message: str) -> list[str]:
    """Extract query names from an ``errors.streaming_query_state`` breach.

    Message format from the check: ``"5 failed queries: a, b, c, d, e"``.
    Returns an empty list if the message does not match (e.g. OK message).
    """
    if ":" not in message:
        return []
    tail = message.split(":", 1)[1].strip()
    if not tail:
        return []
    return [part.strip() for part in tail.split(",") if part.strip()]


# --------------------------------------------------------------------------- main app
class HeimdallApp(App):
    """Heimdall dashboard + modal queue."""

    TITLE = "Heimdall"
    SUB_TITLE = ""

    BINDINGS = [
        Binding("q", "quit", "quit"),
        Binding("r", "force_run", "run"),
        Binding("enter", "drill_in", "details"),
        Binding("f", "failed_queries", "failed"),
    ]

    DEFAULT_CSS = """
    Screen {
        background: $surface;
    }
    #bands {
        height: 100%;
    }
    L1Band {
        height: auto;
        min-height: 2;
        padding: 1 2 0 2;
    }
    L2Band {
        height: auto;
        min-height: 10;
        padding: 1 2 0 2;
    }
    L3Band {
        height: auto;
        min-height: 3;
        padding: 1 2 0 2;
    }
    .footer {
        color: $text-muted;
    }
    """

    pending_count: reactive[int] = reactive(0)
    run_in_progress: reactive[bool] = reactive(False)

    def __init__(self) -> None:
        super().__init__()
        self.store = Store(config.WAL_PATH)
        self.runner = Runner(store=self.store)
        self._l1_band = L1Band()
        self._l2_band = L2Band()
        self._l3_band = L3Band()
        self._modal_shown: set[int] = set()

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical(id="bands"):
            yield self._l1_band
            yield self._l2_band
            yield self._l3_band
        yield Footer()

    def on_mount(self) -> None:
        # Two cadences: UI refresh every second (cheap — reads DuckDB hot tier)
        # and a full runner cycle every 5 seconds (~several hundred ms to a
        # couple seconds of work depending on how many DuckDB/Iceberg checks
        # are in the fast tier). The runner worker is serialized by the
        # ``run_in_progress`` guard so overlapping ticks are dropped.
        self.set_interval(1.0, self.refresh_state)
        self.set_interval(5.0, self._auto_run)
        self.refresh_state()
        # Kick off an initial run so the first view has real data instead of
        # whatever is replayed from the WAL.
        self._auto_run()

    # ------------------------------------------------------------------- state
    def refresh_state(self) -> None:
        rows = load_rows(self.store)

        # Compute layer gating from current snapshot.
        l1_breaching = any(
            r.spec.layer == "l1" and r.status in ("breach", "error") for r in rows.values()
        )
        l2_breaching = any(
            r.spec.layer == "l2" and r.status in ("breach", "error") for r in rows.values()
        )

        self._l1_band.rows_data = rows
        self._l2_band.rows_data = rows
        self._l2_band.gated = l1_breaching
        self._l2_band.gated_reason = "L1 breached" if l1_breaching else ""
        self._l3_band.gated = l1_breaching or l2_breaching
        self._l3_band.gated_reason = (
            "L1 breached" if l1_breaching else "L2 breached" if l2_breaching else ""
        )

        # Header subtitle reports Heimdall's own health: total registered
        # checks plus the Spark "failed streaming queries" count. That check
        # is a meta-health signal about the Spark pipeline's internal query
        # state, not a pipeline operational metric — so it doesn't belong in
        # an L2 group. We still register it at L2 so the runner evaluates
        # it each cycle; the value just surfaces in the banner here.
        total = len(REGISTRY)
        fq_row = rows.get("errors.streaming_query_state")
        if fq_row is None or fq_row.value_num is None:
            fq_frag = ""
        else:
            fq_count = int(fq_row.value_num)
            fq_frag = f" \u00b7 {fq_count} failed queries"
        self.sub_title = f"{total} checks{fq_frag}"

        # Modal queue: surface any un-dismissed modal we haven't opened yet.
        self._poll_modals()

    def _poll_modals(self) -> None:
        pending = self.store.pending_modals()
        self.pending_count = len(pending)
        for modal_id, ts, emit_class, title, body_json in pending:
            if modal_id in self._modal_shown:
                continue
            try:
                body = json.loads(body_json)
            except json.JSONDecodeError:
                body = {"raw": body_json}
            self._modal_shown.add(modal_id)
            if emit_class == "intervention":
                self.push_screen(InterventionModal(modal_id, title, body), self._on_modal_result)
            elif emit_class == "attention":
                self.push_screen(AttentionModal(modal_id, title, body), self._on_modal_result)
            # silent class does not open a modal — status row is the signal

    def _on_modal_result(self, result: str | None) -> None:
        # Find the most recent modal we opened and dismiss it in the store.
        # Textual drops the id inside the screen itself, but we don't get a
        # handle on it from the callback — so we scan pending and dismiss the
        # oldest that's in _modal_shown.
        pending = self.store.pending_modals()
        for row in pending:
            modal_id = int(row[0])
            if modal_id in self._modal_shown:
                if result != "next":
                    self.store.dismiss_modal(modal_id)
                return

    # ------------------------------------------------------------------- actions
    def _auto_run(self) -> None:
        """Fire a runner cycle on the 5s interval. Dropped if already running."""
        if self.run_in_progress:
            return
        self._spawn_runner()

    def action_force_run(self) -> None:
        if self.run_in_progress:
            return
        self._spawn_runner()

    def _spawn_runner(self) -> None:
        self.run_in_progress = True

        def worker() -> None:
            try:
                self.runner.run_once()
            finally:
                self.call_from_thread(self._finish_run)

        threading.Thread(target=worker, daemon=True).start()

    def _finish_run(self) -> None:
        self.run_in_progress = False
        self.refresh_state()

    def action_drill_in(self) -> None:
        # v1: drill into the first L2 check with numeric history. Upgrade to
        # cursor-driven selection once DataTable rows replace the Static bands.
        for check_name in (
            "traffic.jetstream_input_rps",
            "latency.pipeline.post",
            "latency.jetstream_to_raw",
        ):
            if check_name in REGISTRY:
                history = self.store.history(check_name, 600)
                self.push_screen(DrillInModal(check_name, history))
                return

    def action_failed_queries(self) -> None:
        last = self.store.last_result("errors.streaming_query_state")
        if last is None:
            return
        ts, status, message = last
        count_row = self.store.snapshot()
        # Pull the numeric count straight from the snapshot row for this check.
        count = 0
        for name, _mk, _st, _msg, value_num, _vs, _dms, _ts in count_row:
            if name == "errors.streaming_query_state" and value_num is not None:
                count = int(value_num)
                break
        names = _parse_failed_query_names(message or "")
        if count == 0 and not names:
            # Nothing to show — surface a trivial modal so the operator knows
            # the key fired but there's nothing wrong.
            self.push_screen(FailedQueriesModal(0, [], int(ts)))
            return
        self.push_screen(FailedQueriesModal(count, names, int(ts)))


def run_tui() -> None:
    app = HeimdallApp()
    app.run()
