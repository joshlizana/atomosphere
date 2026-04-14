"""Heimdall runtime config: paths, thresholds, endpoints, secrets.

Everything tunable is concentrated here so checks stay declarative. Secrets
come from the repo-root ``.env`` via python-dotenv; there are no hardcoded
credentials in source.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# --------------------------------------------------------------------------- paths
REPO_ROOT = Path(__file__).resolve().parents[2]
LOGS_DIR = REPO_ROOT / "logs"
WAL_PATH = LOGS_DIR / "heimdall-wal.jsonl"
ALERTS_LOG = LOGS_DIR / "alerts.log"

# Load secrets from repo-root .env. Values already in the real environment win.
load_dotenv(REPO_ROOT / ".env", override=False)


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise RuntimeError(
            f"{name} is not set. Heimdall reads secrets from {REPO_ROOT / '.env'}."
        )
    return val


# --------------------------------------------------------------------------- monitored services
SERVICES = ["seaweedfs", "postgres", "polaris", "spark", "grafana", "clickhouse"]

# --------------------------------------------------------------------------- iceberg tables watched in v1 (L3)
V1_ICEBERG_TABLES = [
    "atmosphere.raw.raw_events",
    "atmosphere.core.core_posts",
    "atmosphere.mart.mart_trending_hashtags",
]

# --------------------------------------------------------------------------- alerting + tolerance
DEFAULT_TOLERANCE = 2              # consecutive breaches before alerting
RESULT_RETAIN_DAYS = 30            # Store.prune window
HEAL_RATE_LIMIT = 3                # max heals per target per HEAL_RATE_WINDOW_S
HEAL_RATE_WINDOW_S = 300

# --------------------------------------------------------------------------- check thresholds
# Keys match check names so overrides stay greppable.
THRESHOLDS: dict[str, float] = {
    "traffic.jetstream_input_rps.min": 50.0,
    "latency.jetstream_to_raw.max_s": 10.0,
    "latency.pipeline.post.max_s": 60.0,
    "latency.pipeline.like.max_s": 10.0,
    "latency.pipeline.repost.max_s": 10.0,
    "latency.pipeline.follow.max_s": 10.0,
    "latency.pipeline.block.max_s": 10.0,
    "latency.pipeline.profile.max_s": 10.0,
    "saturation.gpu_mem_used_ratio.max": 0.85,
    "latency.sentiment_gpu_util.min": 10.0,
    "latency.sentiment_gpu_util.max": 95.0,
    "latency.sentiment.lag.max_s": 60.0,
    "traffic.sentiment_rps.min": 5.0,
    "saturation.sentiment_backlog.max": 500.0,
    "saturation.seaweedfs_disk_used.max": 0.85,
    "saturation.host_disk_free_gb.min": 10.0,
    "saturation.host_cpu_used.max": 0.90,
    "saturation.host_mem_used.max": 0.95,
    "saturation.gpu_temp_c.max": 85.0,
    "quality.raw_events.freshness_s.max": 15.0,
    "quality.raw_events.null_did_rate.max": 0.001,
    "quality.mart_trending.freshness_s.max": 120.0,
    "iceberg.file_count.max": 5000,
    "iceberg.avg_file_kb.min": 32.0,
    "iceberg.small_file_ratio.max": 0.50,
    "iceberg.snapshot_count.max": 1000,
    "iceberg.manifest_count.max": 200,
}

# --------------------------------------------------------------------------- endpoints (non-secret)
GRAFANA_URL = os.environ.get("HEIMDALL_GRAFANA_URL", "http://localhost:3000")
SPARK_UI_URL = os.environ.get("HEIMDALL_SPARK_UI_URL", "http://localhost:4040")
SPARK_APP_NAME = os.environ.get("HEIMDALL_SPARK_APP_NAME", "spark")
POLARIS_URI = os.environ.get("HEIMDALL_POLARIS_URI", "http://localhost:8181/api/catalog")
POLARIS_WAREHOUSE = os.environ.get("HEIMDALL_POLARIS_WAREHOUSE", "atmosphere")
POLARIS_SCOPE = os.environ.get("HEIMDALL_POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL")
S3_ENDPOINT = os.environ.get("HEIMDALL_S3_ENDPOINT", "http://localhost:9000")
S3_REGION = os.environ.get("HEIMDALL_S3_REGION", "us-east-1")
CLICKHOUSE_URL = os.environ.get("HEIMDALL_CLICKHOUSE_URL", "http://localhost:8123")

# --------------------------------------------------------------------------- secrets (from .env)
POLARIS_ROOT_USER = _require("POLARIS_ROOT_USER")
POLARIS_ROOT_PASSWORD = _require("POLARIS_ROOT_PASSWORD")
POLARIS_CREDENTIAL = f"{POLARIS_ROOT_USER}:{POLARIS_ROOT_PASSWORD}"

S3_ACCESS_KEY = _require("SEAWEEDFS_ADMIN_USER")
S3_SECRET_KEY = _require("SEAWEEDFS_ADMIN_PASSWORD")

CLICKHOUSE_READER_USER = _require("CLICKHOUSE_READER_USER")
CLICKHOUSE_READER_PASSWORD = _require("CLICKHOUSE_READER_PASSWORD")


def ensure_dirs() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
