"""Process-wide DuckDB instance with Iceberg + Polaris pre-attached.

Heimdall uses a single ``duckdb.connect(":memory:")`` database for everything:
  * the :class:`Store` hot tier (``results``, ``heals``, ``modals`` tables)
  * every L2 iceberg-reading check (``polaris.raw.*`` via the attached catalog)
  * the forthcoming L3 data-fidelity checks

Why one instance, many cursors:

DuckDB's extension manager is process-global. Loading the ``iceberg`` /
``httpfs`` extensions from two independent ``duckdb.connect(":memory:")``
instances in the same process — one on the main thread (the Store) and one on
a worker thread (a check) — races on global extension state and segfaults
inside libc's AVX2 memcpy during concurrent ``INSTALL``/``LOAD``. The
DuckDB-recommended pattern is one database, many connections obtained via
``connection.cursor()``; each cursor is its own session on the shared database
and does not re-initialise extensions.

This module exposes that singleton. Callers do::

    from . import engine
    con = engine.cursor()
    row = con.execute("SELECT ... FROM polaris.raw.raw_events").fetchone()

There is no ``close()`` on the cursor — letting it fall out of scope is the
right thing to do. :func:`close` tears down the whole instance at process
shutdown.
"""
from __future__ import annotations

import atexit
import threading

import duckdb

from . import config


_lock = threading.Lock()
_instance: duckdb.DuckDBPyConnection | None = None

# Global serialization lock for all query execution. The iceberg extension
# holds thread-unsafe shared state during scans — stress_test.py mode D
# (6 threads × concurrent iceberg counts) SIGSEGVs 3/3 in 0.4s, while
# single-threaded iceberg (mode C) is stable. DuckDB's cursor-per-thread
# contract does not extend to the iceberg extension, so every .execute()
# that may touch polaris.* must be serialized.
_exec_lock = threading.Lock()


class _LockedCursor:
    """Proxy around a DuckDB cursor that serializes execute() on _exec_lock.

    The iceberg scan happens inside execute(), which materialises the result
    set; subsequent fetchone()/fetchall() calls work on the buffered result
    and do not re-enter the extension, so only execute() needs the lock.
    """

    __slots__ = ("_cur",)

    def __init__(self, cur: duckdb.DuckDBPyConnection) -> None:
        self._cur = cur

    def execute(self, *args, **kwargs):
        with _exec_lock:
            self._cur.execute(*args, **kwargs)
        return self._cur

    def __getattr__(self, name):
        return getattr(self._cur, name)


def instance() -> duckdb.DuckDBPyConnection:
    """Return the process-wide DuckDB database, bootstrapping on first call.

    First call installs + loads iceberg, creates the ``s3_seaweedfs`` and
    ``polaris_secret`` DuckDB secrets, and ATTACHes the Polaris warehouse as
    ``polaris``. Subsequent calls return the cached handle.
    """
    global _instance
    with _lock:
        if _instance is not None:
            return _instance
        con = duckdb.connect(":memory:")
        _bootstrap(con)
        _instance = con
        atexit.register(close)
        return con


def cursor() -> "_LockedCursor":
    """Return a fresh per-caller cursor on the shared instance.

    The returned object is a thin proxy that takes _exec_lock around every
    execute() so concurrent callers don't race inside the iceberg extension.
    """
    return _LockedCursor(instance().cursor())


def close() -> None:
    """Tear down the shared instance. Safe to call multiple times."""
    global _instance
    with _lock:
        if _instance is None:
            return
        try:
            _instance.close()
        finally:
            _instance = None


# --------------------------------------------------------------------------- internals
def _bootstrap(con: duckdb.DuckDBPyConnection) -> None:
    # Force UTC so now() and any naive/aware coercion is consistent on any
    # host locale. Every Iceberg event_time is stored as TIMESTAMP WITH TIME
    # ZONE in UTC, and DuckDB will otherwise re-attach the host TZ during
    # subtraction and leak the offset into results.
    con.execute("SET TimeZone='UTC';")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute(
        f"""
        CREATE SECRET s3_seaweedfs (
            TYPE s3,
            KEY_ID '{config.S3_ACCESS_KEY}',
            SECRET '{config.S3_SECRET_KEY}',
            ENDPOINT '{config.S3_ENDPOINT.replace("http://", "").replace("https://", "")}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{config.S3_REGION}'
        );
        """
    )
    con.execute(
        f"""
        CREATE SECRET polaris_secret (
            TYPE iceberg,
            CLIENT_ID '{config.POLARIS_ROOT_USER}',
            CLIENT_SECRET '{config.POLARIS_ROOT_PASSWORD}',
            OAUTH2_SERVER_URI '{config.POLARIS_URI}/v1/oauth/tokens',
            OAUTH2_SCOPE '{config.POLARIS_SCOPE}'
        );
        """
    )
    con.execute(
        f"""
        ATTACH '{config.POLARIS_WAREHOUSE}' AS polaris (
            TYPE iceberg,
            ENDPOINT '{config.POLARIS_URI}',
            ACCESS_DELEGATION_MODE 'none'
        );
        """
    )
