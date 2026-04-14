"""Heimdall DuckDB stress harness — empirically find the root cause of the
``free(): chunks in smallbin corrupted`` heap corruption.

Each test mode exercises one hypothesis about where the bug lives:

  A. sequential_cursors    — single-threaded many-cursor churn on the shared
                             instance. Baseline: must pass or engine.py is
                             fundamentally broken.
  B. concurrent_simple     — N threads, trivial counts. Isolates thread-safety
                             of the singleton without exercising iceberg.
  C. sequential_iceberg    — single-threaded iceberg scans, many repeats.
                             Isolates whether iceberg alone corrupts heap.
  D. concurrent_iceberg    — N threads, iceberg scans. Isolates whether the
                             combination of threads + iceberg is the trigger.
  E. pipeline_join         — the exact 1000-row terminal-first join query
                             used by latency.pipeline.* checks, sequential.
  F. pipeline_join_threads — same join, concurrent threads. Most like what
                             runner.run_once() would do with a ThreadPool.
  G. cursor_churn          — rapid open/close of cursors without queries.
  H. tui_overlap_sim       — simulate the TUI pattern: one long-running
                             background thread doing iceberg scans while a
                             second thread fires off checks on the same
                             instance.

Each mode runs in an isolated subprocess so a glibc abort in one doesn't take
down the harness. The child enables ``faulthandler`` and dumps a traceback to
stderr before aborting.

Usage::

    .venv/bin/python -m scripts.heimdall.stress_test           # run all
    .venv/bin/python -m scripts.heimdall.stress_test D F       # run subset
    .venv/bin/python -m scripts.heimdall.stress_test --runs 5  # repeat each
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from typing import Callable


# --------------------------------------------------------------------------- child-side workloads
# Each workload is a snippet run inside a fresh Python subprocess. The child
# imports heimdall.engine, runs the pattern, and exits 0 on success. Any crash
# surfaces via the subprocess return code + stderr.

_WORKLOADS: dict[str, str] = {
    "A_sequential_cursors": """
        import faulthandler; faulthandler.enable()
        from scripts.heimdall import engine
        for i in range(200):
            con = engine.cursor()
            con.execute("SELECT 1").fetchone()
            del con
        print("ok")
    """,
    "B_concurrent_simple": """
        import faulthandler; faulthandler.enable()
        import threading
        from scripts.heimdall import engine
        engine.instance()  # pre-bootstrap
        errs = []
        def w():
            try:
                for _ in range(200):
                    con = engine.cursor()
                    con.execute("SELECT 1").fetchone()
            except Exception as e:
                errs.append(repr(e))
        ts = [threading.Thread(target=w) for _ in range(8)]
        for t in ts: t.start()
        for t in ts: t.join()
        if errs: raise SystemExit("thread errs: " + "; ".join(errs))
        print("ok")
    """,
    "C_sequential_iceberg": """
        import faulthandler; faulthandler.enable()
        from scripts.heimdall import engine
        con = engine.cursor()
        for i in range(30):
            con.execute(
                "SELECT count(*) FROM polaris.raw.raw_events "
                "WHERE collection = 'app.bsky.feed.post'"
            ).fetchone()
        print("ok")
    """,
    "D_concurrent_iceberg": """
        import faulthandler; faulthandler.enable()
        import threading
        from scripts.heimdall import engine
        engine.instance()
        errs = []
        def w():
            try:
                con = engine.cursor()
                for _ in range(10):
                    con.execute(
                        "SELECT count(*) FROM polaris.raw.raw_events "
                        "WHERE collection = 'app.bsky.feed.like'"
                    ).fetchone()
            except Exception as e:
                errs.append(repr(e))
        ts = [threading.Thread(target=w) for _ in range(6)]
        for t in ts: t.start()
        for t in ts: t.join()
        if errs: raise SystemExit("thread errs: " + "; ".join(errs))
        print("ok")
    """,
    "E_pipeline_join": """
        import faulthandler; faulthandler.enable()
        from scripts.heimdall import engine
        con = engine.cursor()
        sql = '''
            WITH recent AS (
                SELECT did, time_us, ingested_at
                FROM polaris.staging.stg_follows
                ORDER BY ingested_at DESC LIMIT 1000
            )
            SELECT count(*), avg(EXTRACT(EPOCH FROM (t.ingested_at - raw.ingested_at)))
            FROM recent t
            INNER JOIN polaris.raw.raw_events raw
              ON raw.did = t.did AND raw.time_us = t.time_us
            WHERE raw.collection = 'app.bsky.graph.follow'
        '''
        for i in range(10):
            row = con.execute(sql).fetchone()
            print(f"iter {i}: {row}")
        print("ok")
    """,
    "F_pipeline_join_threads": """
        import faulthandler; faulthandler.enable()
        import threading
        from scripts.heimdall import engine
        engine.instance()
        sqls = {
            'post':    ('polaris.core.core_post_sentiment', 'did',       None,     'app.bsky.feed.post'),
            'like':    ('polaris.core.core_engagement',     'actor_did', 'like',   'app.bsky.feed.like'),
            'repost':  ('polaris.core.core_engagement',     'actor_did', 'repost', 'app.bsky.feed.repost'),
            'follow':  ('polaris.staging.stg_follows',      'did',       None,     'app.bsky.graph.follow'),
            'block':   ('polaris.staging.stg_blocks',       'did',       None,     'app.bsky.graph.block'),
            'profile': ('polaris.staging.stg_profiles',     'did',       None,     'app.bsky.actor.profile'),
        }
        errs = []
        def w(et, term, did_col, et_filter, coll):
            try:
                con = engine.cursor()
                filt = f"WHERE event_type = '{et_filter}'" if et_filter else ''
                sql = f'''
                    WITH recent AS (
                        SELECT {did_col} AS did, time_us, ingested_at
                        FROM {term}
                        {filt}
                        ORDER BY ingested_at DESC LIMIT 1000
                    )
                    SELECT count(*),
                           avg(EXTRACT(EPOCH FROM (t.ingested_at - raw.ingested_at)))
                    FROM recent t
                    INNER JOIN polaris.raw.raw_events raw
                      ON raw.did = t.did AND raw.time_us = t.time_us
                    WHERE raw.collection = '{coll}'
                '''
                for _ in range(5):
                    con.execute(sql).fetchone()
            except Exception as e:
                errs.append(f"{et}: {e!r}")
        ts = [threading.Thread(target=w, args=(et, *rest)) for et, rest in sqls.items()]
        for t in ts: t.start()
        for t in ts: t.join()
        if errs: raise SystemExit("thread errs: " + "; ".join(errs))
        print("ok")
    """,
    "G_cursor_churn": """
        import faulthandler; faulthandler.enable()
        from scripts.heimdall import engine
        engine.instance()
        for i in range(5000):
            con = engine.cursor()
            del con
        print("ok")
    """,
    "I_store_vs_iceberg": """
        import faulthandler; faulthandler.enable()
        import threading, time
        from pathlib import Path
        from scripts.heimdall import engine
        from scripts.heimdall.store import Store
        from scripts.heimdall.result import Result
        # Fresh WAL per run so replay doesn't pollute timings.
        wal = Path('/tmp/heimdall-stress-store.jsonl')
        if wal.exists(): wal.unlink()
        store = Store(wal)
        errs = []
        def iceberg_bg():
            try:
                con = engine.cursor()
                for _ in range(10):
                    con.execute(
                        "SELECT count(*) FROM polaris.raw.raw_events "
                        "WHERE collection='app.bsky.feed.like'"
                    ).fetchone()
                    time.sleep(0.01)
            except Exception as e:
                errs.append(f"ice: {e!r}")
        def store_fg():
            try:
                for i in range(500):
                    r = Result(
                        check='stress.store', status='ok',
                        message=f'iter {i}', value=float(i), ts=int(time.time()),
                        duration_ms=1,
                    )
                    store.record(r)
                    _ = store.snapshot()
                    _ = store.last_result('stress.store')
            except Exception as e:
                errs.append(f"store: {e!r}")
        t_ice = threading.Thread(target=iceberg_bg); t_ice.start()
        t_store = threading.Thread(target=store_fg); t_store.start()
        t_ice.join()
        t_store.join()
        store.close()
        if errs: raise SystemExit("errs: " + "; ".join(errs))
        print("ok")
    """,
    "H_tui_overlap_sim": """
        import faulthandler; faulthandler.enable()
        import threading, time
        from scripts.heimdall import engine
        engine.instance()
        stop = threading.Event()
        errs = []
        def bg():
            try:
                con = engine.cursor()
                while not stop.is_set():
                    con.execute(
                        "SELECT count(*) FROM polaris.raw.raw_events "
                        "WHERE collection='app.bsky.feed.like'"
                    ).fetchone()
            except Exception as e:
                errs.append(f"bg: {e!r}")
        def fg():
            try:
                for _ in range(15):
                    con = engine.cursor()
                    con.execute(
                        "WITH r AS (SELECT did, time_us, ingested_at "
                        "FROM polaris.staging.stg_follows "
                        "ORDER BY ingested_at DESC LIMIT 500) "
                        "SELECT count(*), avg(EXTRACT(EPOCH FROM "
                        "(t.ingested_at - raw.ingested_at))) "
                        "FROM r t INNER JOIN polaris.raw.raw_events raw "
                        "ON raw.did=t.did AND raw.time_us=t.time_us "
                        "WHERE raw.collection='app.bsky.graph.follow'"
                    ).fetchone()
            except Exception as e:
                errs.append(f"fg: {e!r}")
        b = threading.Thread(target=bg, daemon=True); b.start()
        f = threading.Thread(target=fg); f.start()
        f.join()
        stop.set()
        b.join(timeout=5)
        if errs: raise SystemExit("thread errs: " + "; ".join(errs))
        print("ok")
    """,
}


# --------------------------------------------------------------------------- runner
@dataclass
class RunResult:
    mode: str
    run: int
    rc: int
    crashed: bool
    stderr_tail: str
    duration_s: float


def _run_child(mode: str, body: str) -> RunResult:
    import time

    # Dedent so the body is valid python at the top level of `python -c`.
    src = textwrap.dedent(body)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env = os.environ.copy()
    env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")

    t0 = time.time()
    proc = subprocess.run(
        [sys.executable, "-c", src],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    dur = time.time() - t0
    # Any nonzero exit is considered a failure. Negative rc = signal (SIGABRT=-6).
    crashed = proc.returncode != 0
    stderr = (proc.stderr or "").strip()
    tail = "\n".join(stderr.splitlines()[-15:]) if stderr else ""
    return RunResult(
        mode=mode, run=0, rc=proc.returncode, crashed=crashed, stderr_tail=tail, duration_s=dur
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("modes", nargs="*", help="letters (A..H). default: all")
    parser.add_argument("--runs", type=int, default=3, help="repeat each mode N times")
    args = parser.parse_args(argv)

    selected = _WORKLOADS
    if args.modes:
        wanted = {m.upper() for m in args.modes}
        selected = {k: v for k, v in _WORKLOADS.items() if k.split("_")[0] in wanted}
        if not selected:
            print(f"No modes matched {args.modes}", file=sys.stderr)
            return 2

    print(f"Running {len(selected)} modes × {args.runs} repeats each\n")
    totals: dict[str, list[RunResult]] = {}
    for mode, body in selected.items():
        results: list[RunResult] = []
        for i in range(args.runs):
            print(f"▶ {mode}  run {i + 1}/{args.runs} ...", end=" ", flush=True)
            r = _run_child(mode, body)
            r.run = i + 1
            results.append(r)
            status = "CRASH" if r.crashed else "ok"
            print(f"{status}  (rc={r.rc}, {r.duration_s:.1f}s)")
            if r.crashed and r.stderr_tail:
                print(textwrap.indent(r.stderr_tail, "    "))
        totals[mode] = results

    # Summary
    print("\n─── summary ─────────────────────────────────")
    any_crash = False
    for mode, rs in totals.items():
        crashes = sum(1 for r in rs if r.crashed)
        any_crash = any_crash or crashes > 0
        marker = "✗" if crashes else "✓"
        print(f"  {marker} {mode:<32} {crashes}/{len(rs)} crashed")
    return 1 if any_crash else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
