"""L1 HTTP liveness probes — one round-trip per service per cycle.

These run before any operational or data-fidelity check so that a probe
failure short-circuits the rest of Heimdall via the layer gate in
``runner.run_once``. A probe is "ok" as long as the service speaks HTTP and
returns an expected response — the check deliberately does not look at
bodies beyond what's needed to confirm identity.
"""
from __future__ import annotations

import requests

from .. import config
from ..registry import check
from ..result import Result, breach, error, ok

_TIMEOUT = 2.0


def _get(url: str) -> tuple[int | None, str]:
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
    except requests.RequestException as exc:
        return None, exc.__class__.__name__
    return resp.status_code, resp.text


@check("probes.polaris.http", layer="l1", tier="fast", service="polaris")
def polaris_http(ctx) -> Result:
    name = "probes.polaris.http"
    url = f"{config.POLARIS_URI.rstrip('/')}/v1/config"
    status, body = _get(url)
    if status is None:
        return breach(name, f"polaris unreachable: {body}", value=body)
    # 200 = ok; 401 = up but unauthenticated (fine — we're just pinging)
    if status in (200, 401):
        return ok(name, f"polaris http {status}", value=status)
    return breach(name, f"polaris http {status}", value=status)


@check("probes.clickhouse.http", layer="l1", tier="fast", service="clickhouse")
def clickhouse_http(ctx) -> Result:
    name = "probes.clickhouse.http"
    url = f"{config.CLICKHOUSE_URL.rstrip('/')}/ping"
    status, body = _get(url)
    if status is None:
        return breach(name, f"clickhouse unreachable: {body}", value=body)
    if status == 200 and "Ok" in body:
        return ok(name, "clickhouse ping Ok", value=status)
    return breach(name, f"clickhouse http {status}", value=status)


@check("probes.grafana.http", layer="l1", tier="fast", service="grafana")
def grafana_http(ctx) -> Result:
    name = "probes.grafana.http"
    url = f"{config.GRAFANA_URL.rstrip('/')}/api/health"
    status, body = _get(url)
    if status is None:
        return breach(name, f"grafana unreachable: {body}", value=body)
    if status == 200:
        return ok(name, "grafana /api/health 200", value=status)
    return breach(name, f"grafana http {status}", value=status)


@check("probes.spark_ui.http", layer="l1", tier="fast", service="spark")
def spark_ui_http(ctx) -> Result:
    name = "probes.spark_ui.http"
    url = f"{config.SPARK_UI_URL.rstrip('/')}/api/v1/applications"
    status, body = _get(url)
    if status is None:
        return breach(name, f"spark UI unreachable: {body}", value=body)
    if status == 200:
        return ok(name, "spark UI 200", value=status)
    return breach(name, f"spark UI http {status}", value=status)
