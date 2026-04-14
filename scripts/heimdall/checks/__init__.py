"""Heimdall check modules. Importing any submodule registers its checks.

Each check module uses ``@check`` from ``registry`` to self-register at import
time, so this file's only job is to import every module we want active.
"""

# L1 + L2 active modules.
from . import containers, probes, saturation, sentiment, spark  # noqa: F401

# L3 checks (data_quality, iceberg_health) are disabled until the DuckDB
# Iceberg reader lands — they reference the removed SqlEngine abstraction.
# Re-enable by adding `data_quality, iceberg_health` to the import above.
