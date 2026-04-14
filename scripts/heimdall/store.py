"""DuckDB in-memory store backed by a JSONL WAL on disk.

Hot tier: DuckDB ``:memory:`` — all reads go here.
Durable tier: ``logs/heimdall-wal.jsonl`` — one line per record, appended
synchronously on every write and fsync'd. On startup the WAL is replayed into
DuckDB so crash recovery is automatic.

The public surface mirrors the old ``SqliteStore`` in ``state.py`` so the
runner + alerter need no behavioral changes beyond the import swap.

Deferred (see ``reference/heimdall-architecture.md``):
    * periodic flush of pruned-WAL rows to an Iceberg cold-remote table
    * DuckDB ``ATTACH`` to Polaris for L3 data-fidelity checks
    * Iceberg-availability probe gating the flush
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import IO

from . import engine
from .result import Result, Status

_SCHEMA = """
CREATE TABLE IF NOT EXISTS results (
    ts          BIGINT  NOT NULL,
    check_name  VARCHAR NOT NULL,
    metric_key  VARCHAR,
    status      VARCHAR NOT NULL,
    message     VARCHAR,
    value_num   DOUBLE,
    value_str   VARCHAR,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_results_check_ts ON results(check_name, ts);

CREATE TABLE IF NOT EXISTS heals (
    ts      BIGINT  NOT NULL,
    target  VARCHAR NOT NULL,
    action  VARCHAR NOT NULL,
    reason  VARCHAR,
    outcome VARCHAR
);
CREATE INDEX IF NOT EXISTS idx_heals_target_ts ON heals(target, ts);

CREATE SEQUENCE IF NOT EXISTS modals_id_seq START 1;
CREATE TABLE IF NOT EXISTS modals (
    id           BIGINT PRIMARY KEY DEFAULT nextval('modals_id_seq'),
    ts           BIGINT  NOT NULL,
    emit_class   VARCHAR NOT NULL,
    title        VARCHAR NOT NULL,
    body_json    VARCHAR NOT NULL,
    dismissed_at BIGINT
);
CREATE INDEX IF NOT EXISTS idx_modals_dismissed ON modals(dismissed_at);
"""


def _coerce_value(value: float | str | None) -> tuple[float | None, str | None]:
    if isinstance(value, bool):  # bool is an int in python; store as num
        return float(value), None
    if isinstance(value, (int, float)):
        return float(value), None
    if isinstance(value, str):
        return None, value
    return None, None


class Store:
    """DuckDB hot tier + JSONL WAL durable tier."""

    def __init__(self, wal_path: Path | str) -> None:
        self.wal_path = Path(wal_path)
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        # Share the process-wide DuckDB instance so extensions and the
        # attached Polaris catalog are loaded exactly once — see engine.py.
        # We never cache a cursor: each method grabs a fresh one via the
        # ``_conn`` property, which returns a ``_LockedCursor`` wrapping a
        # brand-new underlying DuckDB cursor. Two reasons this matters:
        #   1. The global ``_exec_lock`` in engine.py protects a single
        #      .execute() call, but not the gap between execute() and
        #      fetch*(). A cached cursor shared across the TUI main thread
        #      and the runner worker thread lets thread B's execute() stomp
        #      thread A's result set before A can fetch — racing the
        #      iceberg extension inside libc and producing the
        #      'free(): chunks in smallbin corrupted' abort.
        #   2. Per-call cursors scope fetch state to the caller, so
        #      execute().fetchone() chains in one thread are safe even if
        #      another thread is also executing through the same Store.
        self._conn.execute(_SCHEMA)
        self._wal: IO[str] | None = None
        self._replay_wal()
        # Re-open in append mode for subsequent writes.
        self._wal = open(self.wal_path, "a", buffering=1, encoding="utf-8")

    @property
    def _conn(self) -> "engine._LockedCursor":
        """Fresh per-call cursor so execute()/fetch() pairs don't race."""
        return engine.cursor()

    # ----------------------------------------------------------------- WAL
    def _replay_wal(self) -> None:
        if not self.wal_path.exists():
            return
        with self.wal_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = row.get("kind")
                if kind == "result":
                    self._insert_result_row(row)
                elif kind == "heal":
                    self._insert_heal_row(row)
                elif kind == "modal":
                    self._insert_modal_row(row)
                elif kind == "modal_dismiss":
                    self._apply_modal_dismiss(row)
        # Advance modals_id_seq past replayed ids. DuckDB has no setval and
        # no ALTER SEQUENCE RESTART, and the sequence is referenced by the
        # modals.id DEFAULT so it can't be dropped. Consuming nextval() N
        # times is cheap at realistic volumes (modal ids grow slowly).
        max_row = self._conn.execute("SELECT max(id) FROM modals").fetchone()
        max_id = int(max_row[0]) if max_row and max_row[0] is not None else 0
        if max_id > 0:
            cur = int(
                self._conn.execute("SELECT nextval('modals_id_seq')").fetchone()[0]
            )
            while cur <= max_id:
                cur = int(
                    self._conn.execute("SELECT nextval('modals_id_seq')").fetchone()[0]
                )

    def _append_wal(self, row: dict) -> None:
        assert self._wal is not None
        self._wal.write(json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n")
        self._wal.flush()
        try:
            os.fsync(self._wal.fileno())
        except OSError:
            # tmpfs etc. — durability degrades gracefully
            pass

    # ----------------------------------------------------------------- inserts
    def _insert_result_row(self, row: dict) -> None:
        self._conn.execute(
            "INSERT INTO results (ts, check_name, metric_key, status, message,"
            " value_num, value_str, duration_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                int(row["ts"]),
                row["check_name"],
                row.get("metric_key"),
                row["status"],
                row.get("message"),
                row.get("value_num"),
                row.get("value_str"),
                row.get("duration_ms") or 0,
            ),
        )

    def _insert_heal_row(self, row: dict) -> None:
        self._conn.execute(
            "INSERT INTO heals (ts, target, action, reason, outcome)"
            " VALUES (?, ?, ?, ?, ?)",
            (
                int(row["ts"]),
                row["target"],
                row["action"],
                row.get("reason"),
                row.get("outcome"),
            ),
        )

    def _insert_modal_row(self, row: dict) -> None:
        # If the row carries an id (from a replay or rewrite), honor it so
        # dismissals applied later still find their target. Otherwise DuckDB
        # assigns the next sequence value.
        if "id" in row and row["id"] is not None:
            self._conn.execute(
                "INSERT INTO modals (id, ts, emit_class, title, body_json,"
                " dismissed_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    int(row["id"]),
                    int(row["ts"]),
                    row["emit_class"],
                    row["title"],
                    row["body_json"],
                    row.get("dismissed_at"),
                ),
            )
        else:
            self._conn.execute(
                "INSERT INTO modals (ts, emit_class, title, body_json,"
                " dismissed_at) VALUES (?, ?, ?, ?, ?)",
                (
                    int(row["ts"]),
                    row["emit_class"],
                    row["title"],
                    row["body_json"],
                    row.get("dismissed_at"),
                ),
            )

    def _apply_modal_dismiss(self, row: dict) -> None:
        self._conn.execute(
            "UPDATE modals SET dismissed_at = ? WHERE id = ?",
            (int(row["dismissed_at"]), int(row["id"])),
        )

    # ----------------------------------------------------------------- public writes
    def record(self, result: Result) -> None:
        value_num, value_str = _coerce_value(result.value)
        row = {
            "kind": "result",
            "ts": result.ts or int(time.time()),
            "check_name": result.check,
            "metric_key": result.metric_key,
            "status": result.status,
            "message": result.message,
            "value_num": value_num,
            "value_str": value_str,
            "duration_ms": result.duration_ms,
        }
        self._append_wal(row)
        self._insert_result_row(row)

    def record_modal(
        self,
        *,
        emit_class: str,
        title: str,
        body: dict,
    ) -> int:
        """Queue a modal for the TUI. Returns the assigned modal id."""
        body_json = json.dumps(body, separators=(",", ":"), sort_keys=True)
        ts = int(time.time())
        row = self._conn.execute(
            "INSERT INTO modals (ts, emit_class, title, body_json)"
            " VALUES (?, ?, ?, ?) RETURNING id",
            (ts, emit_class, title, body_json),
        ).fetchone()
        modal_id = int(row[0])
        self._append_wal(
            {
                "kind": "modal",
                "id": modal_id,
                "ts": ts,
                "emit_class": emit_class,
                "title": title,
                "body_json": body_json,
                "dismissed_at": None,
            }
        )
        return modal_id

    def dismiss_modal(self, modal_id: int) -> None:
        ts = int(time.time())
        self._conn.execute(
            "UPDATE modals SET dismissed_at = ? WHERE id = ?",
            (ts, modal_id),
        )
        self._append_wal(
            {"kind": "modal_dismiss", "id": modal_id, "dismissed_at": ts}
        )

    def pending_modals(self) -> list[tuple]:
        """All un-dismissed modals in FIFO order. The TUI reads this every tick."""
        return self._conn.execute(
            "SELECT id, ts, emit_class, title, body_json FROM modals"
            " WHERE dismissed_at IS NULL ORDER BY ts ASC, id ASC"
        ).fetchall()

    def record_heal(self, target: str, action: str, reason: str, outcome: str) -> None:
        row = {
            "kind": "heal",
            "ts": int(time.time()),
            "target": target,
            "action": action,
            "reason": reason,
            "outcome": outcome,
        }
        self._append_wal(row)
        self._insert_heal_row(row)

    # ----------------------------------------------------------------- reads
    def last_n_statuses(self, check_name: str, n: int) -> list[Status]:
        rows = self._conn.execute(
            "SELECT status FROM results WHERE check_name = ? ORDER BY ts DESC LIMIT ?",
            (check_name, n),
        ).fetchall()
        return [row[0] for row in rows]

    def last_result(self, check_name: str) -> tuple[int, Status, str] | None:
        row = self._conn.execute(
            "SELECT ts, status, message FROM results WHERE check_name = ?"
            " ORDER BY ts DESC LIMIT 1",
            (check_name,),
        ).fetchone()
        return row if row else None

    def recent_heals(self, target: str, within_seconds: int) -> int:
        since = int(time.time()) - within_seconds
        row = self._conn.execute(
            "SELECT count(*) FROM heals WHERE target = ? AND ts >= ?",
            (target, since),
        ).fetchone()
        return int(row[0]) if row else 0

    def snapshot(self) -> list[tuple]:
        """Latest row per check_name, for layer gating and the TUI."""
        return self._conn.execute(
            """
            SELECT r.check_name, r.metric_key, r.status, r.message,
                   r.value_num, r.value_str, r.duration_ms, r.ts
            FROM results r
            JOIN (
                SELECT check_name, max(ts) AS max_ts
                FROM results
                GROUP BY check_name
            ) latest
              ON r.check_name = latest.check_name AND r.ts = latest.max_ts
            ORDER BY r.check_name
            """
        ).fetchall()

    def history(self, check_name: str, since_seconds: int) -> list[tuple]:
        since = int(time.time()) - since_seconds
        return self._conn.execute(
            "SELECT ts, status, message, value_num, value_str, duration_ms"
            " FROM results WHERE check_name = ? AND ts >= ? ORDER BY ts DESC",
            (check_name, since),
        ).fetchall()

    # ----------------------------------------------------------------- maintenance
    def prune(self, retain_days: int) -> int:
        """Drop old rows from DuckDB and rewrite the WAL minus pruned rows.

        At L1+L2 write volume (~25 rows/cycle × 12 cycles/minute) a full WAL
        rewrite every cycle is still cheap. Revisit if the WAL grows past a
        few MB between prunes.
        """
        cutoff = int(time.time()) - retain_days * 86400
        before = self._conn.execute(
            "SELECT (SELECT count(*) FROM results WHERE ts < ?)"
            " + (SELECT count(*) FROM heals WHERE ts < ?)",
            (cutoff, cutoff),
        ).fetchone()
        deleted = int(before[0]) if before else 0
        if deleted == 0:
            return 0
        self._conn.execute("DELETE FROM results WHERE ts < ?", (cutoff,))
        self._conn.execute("DELETE FROM heals WHERE ts < ?", (cutoff,))
        self._rewrite_wal_from_hot()
        return deleted

    def _rewrite_wal_from_hot(self) -> None:
        """Dump the hot tier back out to a fresh WAL file atomically."""
        assert self._wal is not None
        self._wal.close()
        tmp = self.wal_path.with_suffix(".jsonl.tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            for row in self._conn.execute(
                "SELECT ts, check_name, metric_key, status, message,"
                " value_num, value_str, duration_ms FROM results ORDER BY ts"
            ).fetchall():
                fh.write(
                    json.dumps(
                        {
                            "kind": "result",
                            "ts": row[0],
                            "check_name": row[1],
                            "metric_key": row[2],
                            "status": row[3],
                            "message": row[4],
                            "value_num": row[5],
                            "value_str": row[6],
                            "duration_ms": row[7],
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                )
            for row in self._conn.execute(
                "SELECT ts, target, action, reason, outcome FROM heals ORDER BY ts"
            ).fetchall():
                fh.write(
                    json.dumps(
                        {
                            "kind": "heal",
                            "ts": row[0],
                            "target": row[1],
                            "action": row[2],
                            "reason": row[3],
                            "outcome": row[4],
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                )
            for row in self._conn.execute(
                "SELECT id, ts, emit_class, title, body_json, dismissed_at"
                " FROM modals ORDER BY id"
            ).fetchall():
                fh.write(
                    json.dumps(
                        {
                            "kind": "modal",
                            "id": row[0],
                            "ts": row[1],
                            "emit_class": row[2],
                            "title": row[3],
                            "body_json": row[4],
                            "dismissed_at": row[5],
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                    + "\n"
                )
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp, self.wal_path)
        self._wal = open(self.wal_path, "a", buffering=1, encoding="utf-8")

    def close(self) -> None:
        if self._wal is not None:
            try:
                self._wal.flush()
                try:
                    os.fsync(self._wal.fileno())
                except OSError:
                    pass
            finally:
                self._wal.close()
                self._wal = None
        # Nothing to release for _conn — it's a per-call property returning
        # a fresh cursor on the shared engine instance, which is torn down
        # via engine.close() at process shutdown.

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
