# Plan: Replace query-api with ClickHouse as the Grafana serving layer

## Progress

| Ticket | State | Notes |
|---|---|---|
| CLH-194 (CH-01) | **In Review** — 2026-04-12 | compose service + config.d/users.d XML landed |
| CLH-196 (CH-02) | **In Review** — 2026-04-12 | Polaris reader principal + RBAC chain in `setup.py` |
| CLH-195 (CH-03) | **In Review** — 2026-04-12 | 7-step init, `CREATE DATABASE polaris_catalog` green |
| CLH-197 (CH-04) | Backlog | Starts with `vended_credentials=false` fix (see Deviations) |
| CLH-198..203 (CH-05..10) | Backlog | — |

### Deviations from original plan during CH-01..03

1. **4 env vars, not 6 (supersedes decision #14).** The plan assumed `CLICKHOUSE_CLIENT_ID` / `CLICKHOUSE_CLIENT_SECRET` could be pre-seeded on the `clickhouse` principal. Polaris's `POST /api/management/v1/principals` rejects caller-supplied secrets — it generates `clientId`/`clientSecret` server-side and returns them once. `setup.py` now persists the generated pair to `/var/polaris-creds/clickhouse.json` (mode 0600) via a new `polaris-creds` named volume, and rotates on 409 if the file is missing but the principal exists (fresh init volume on an existing Polaris). Only `CLICKHOUSE_READER_USER/PASSWORD` + `CLICKHOUSE_ADMIN_USER/PASSWORD` are in `.env`.
2. **Grant list corrected (supersedes decision #15).** Polaris's `CatalogPrivilege` enum has no `NAMESPACE_LIST_TABLES`; it's two separate privileges: `NAMESPACE_LIST` (list namespaces) and `TABLE_LIST` (list tables within namespaces). Final set in `setup.py`: `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `TABLE_LIST`, `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` (6 privs, catalog-level).
3. **Polaris 500 + "duplicate key" tolerated on re-grants and re-assignments.** Polaris returns HTTP 500 (`duplicate key value violates unique constraint`) when a grant or role assignment already exists. `setup.py` swallows 500+"duplicate key" on the grant loop and on both PUT `/principals/{p}/principal-roles` and PUT `/principal-roles/{pr}/catalog-roles/{cat}` calls so re-running `make up` stays idempotent.
4. **`docker/clickhouse/Dockerfile` added (escape hatch on decision #20).** The plan preferred the raw `:26.1-alpine` image with bind-mounted `config.d/` + `users.d/`. ClickHouse's entrypoint writes `default-user.xml` into `users.d/` on first boot; a RO bind mount rejects this with `/entrypoint.sh: line 147: ... Read-only file system` and kills the container. Fix is a thin Dockerfile that `COPY`s both directories into the image — no Alpine package additions needed.
5. **Healthcheck uses `http://127.0.0.1:8123/ping`, not `localhost`.** WSL2 does not expose IPv6; Alpine's `localhost` resolves to `::1` first and `wget` fails before falling back. Literal IPv4 sidesteps this. ClickHouse's own `listen_try=1` binds only `0.0.0.0` in WSL2, which the healthcheck reaches on `127.0.0.1`.
6. **ClickHouse 26.1 renamed the experimental flag.** `allow_experimental_database_unity_catalog` was superseded by `allow_database_iceberg` in 26.1 — the former alone yields `Code 344: SUPPORT_IS_DISABLED`. Both are now set in `docker/clickhouse/config.d/datalake.xml`, mirrored into the `atmosphere_reader` profile in `users.d/readonly-profile.xml`, and passed as query params on the init CREATE DATABASE call.
7. **Data-read path issue deferred to CH-04.** First `SELECT count() FROM polaris_catalog.\`mart.mart_events_per_second\`` under `vended_credentials=true` failed with an S3 404 where the key is doubled: `mart/mart_events_per_second/warehouse/mart/mart_events_per_second/metadata/01397-*.metadata.json`. Root cause is almost certainly vended-cred path composition — Polaris returns a location that ClickHouse re-joins against its catalog base. Since RustFS accepts static root creds (no STS vending needed), the CH-04 fix is to flip `vended_credentials=false` in the DataLakeCatalog DDL. CH-01..03 acceptance only required bootstrap + connectivity, which is fully green.

## Context

Atmosphere's M6 Dashboard milestone originally shipped a custom FastAPI + PySpark `query-api` container that served mart query results to Grafana via the Infinity datasource plugin. The `query-api` container and its Spark base image have already been removed from the stack; the dashboard is currently non-functional because [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml) still points at `http://query-api:8000`.

The intended replacement is **ClickHouse** running as a thin reader over the existing Polaris-managed Iceberg lakehouse. ClickHouse's `DataLakeCatalog` engine (`catalog_type = 'rest'`) connects directly to Polaris, authenticates via OAuth2 client-credentials, and reads Iceberg tables from RustFS using credentials vended by Polaris. Grafana switches from Infinity to the official `grafana-clickhouse-datasource` plugin and queries mart tables with native SQL.

Outcome: fewer custom components (no hand-rolled REST layer), purpose-built OLAP latency for dashboard queries, a cleaner demonstration of "a lot with a little" on the portfolio — Spark for ingest/stream/transform/ML, ClickHouse for serving, Iceberg as the integration point. This entirely reimplements M6's serving layer; M7 (Cloudflare Tunnel) and M8 (Hardening) remain unaffected.

Research notes: [reference/clickhouse-iceberg-research.md](reference/clickhouse-iceberg-research.md).

## Decisions captured

| # | Decision | Value |
|---|---|---|
| 1 | ClickHouse image pin | `clickhouse/clickhouse-server:26.1` (Polaris docs-blessed floor) |
| 2 | Read-only enforcement | Polaris RBAC + ClickHouse `readonly=1` profile (defence in depth) |
| 3 | Polaris realm | `POLARIS` (default name; sent via `auth_header = 'Polaris-Realm:POLARIS'`) |
| 4 | Dashboard migration | Rewrite panel SQL in-place against `polaris_catalog.<namespace>.<table>` |
| 5 | `{window}` template var | Grafana custom dropdown variable (preserves current UX) |
| 6 | Iceberg time travel | Skip — dashboards stay live-only |
| 7 | `pipeline_health` (Iceberg view) | Try DataLakeCatalog view passthrough first → fall back to inline ClickHouse SQL → materialize as last resort |
| 8 | Network topology | `clickhouse` joins **both** `atmosphere-data` and `atmosphere-frontend` (mirrors old query-api) |
| 9 | Docs scope | Same PR: ROADMAP §6.1–6.2, TRD, TDD, CLAUDE.md, README updated together |
| 10 | Smoke test | Rewritten in same PR to probe ClickHouse |
| 11 | Grafana plugin install | Custom `grafana/Dockerfile` bakes `grafana-clickhouse-datasource` at build time (no runtime install, no ~30s boot penalty) |
| 12 | ClickHouse bootstrap owner | Reuse the existing `init` container — extend `infra/init/setup.py` to also create the `polaris_catalog` database after ClickHouse is reachable. No new init container, no `/docker-entrypoint-initdb.d/` volume. |
| 13 | Deployment modes | Plan for **two targets**: (a) local dev — ClickHouse reachable only on the internal Docker network, basic auth; (b) Grafana Cloud — ClickHouse exposed publicly via Cloudflare Tunnel (or equivalent), TLS required, strong credentials. Local mode ships in this PR; cloud mode is a documented follow-up under M7. |
| 14 | Credential separation | **Six distinct env vars**, three independent credential pairs: (a) `CLICKHOUSE_CLIENT_ID` / `CLICKHOUSE_CLIENT_SECRET` — Polaris OAuth client, used only by the DataLakeCatalog engine; (b) `CLICKHOUSE_READER_USER` / `CLICKHOUSE_READER_PASSWORD` — ClickHouse user Grafana logs in as, `readonly=1` profile; (c) `CLICKHOUSE_ADMIN_USER` / `CLICKHOUSE_ADMIN_PASSWORD` — ClickHouse user init uses once to run `CREATE DATABASE` DDL at bootstrap, never touched after. Each rotates independently. |
| 15 | Polaris grant scope | Grant `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `NAMESPACE_LIST_TABLES`, `NAMESPACE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` **at the catalog level** on `atmosphere`. Cascades to every namespace (raw, staging, core, mart) — needed because panel 6 reads `raw_events` directly. |
| 16 | Panel SQL location | Standalone `.sql` files under `grafana/sql/` mirroring [spark/transforms/sql/mart/](spark/transforms/sql/mart/). Dashboard JSON is assembled by a pre-build step (`scripts/build-dashboard.py`) that reads a template + the .sql files and emits `grafana/dashboards/atmosphere.json`. Run via `make dashboard-build`, invoked automatically from `make up` if the output is stale. |
| 17 | Smoke test depth | Three-layer probe: (1) CH row counts per mart table via `clickhouse-client`; (2) Grafana datasource health via `curl /api/datasources/uid/clickhouse/health`; (3) Dashboard visibility via `curl /api/search?query=Atmosphere`. All three must pass for smoke-test to exit 0. |
| 18 | CH data volume | **Persistent** — mount `clickhouse-data:/var/lib/clickhouse`. Warms the DataLakeCatalog metadata cache across restarts. Added to `volumes:` in docker-compose. |
| 19 | Generated dashboard JSON | **Commit** `grafana/dashboards/atmosphere.json` to git. Build script is deterministic (sorted keys), so diffs stay minimal and fresh clones provision without requiring `make dashboard-build` first. |
| 20 | CH image variant | **Try `clickhouse/clickhouse-server:26.1-alpine` first**. Verify during CH-01 that the alpine image ships `wget`, `clickhouse-client`, and anything else the healthcheck + smoke test need. Only fall back to the full Ubuntu `:26.1` tag if something is missing and adding it via a custom Dockerfile outweighs the ~700 MB savings. |
| 21 | CH log level | `information` (ClickHouse default). Balanced signal for local dev — surfaces DataLakeCatalog refresh events and query errors without per-query spam. Configured in `docker/clickhouse/config.d/datalake.xml` alongside the experimental flag. |

## Files to modify / create

### New files
- `docker/clickhouse/config.d/datalake.xml` — enables `allow_experimental_database_unity_catalog` profile-wide so Grafana sessions don't need `SET`; also pins `<logger><level>information</level></logger>` (CH default, stated explicitly for clarity)
- `docker/clickhouse/users.d/readonly-profile.xml` — `atmosphere_reader` profile with `readonly = 1` and `changeable_in_readonly` on `max_execution_time` (required by the Go client the Grafana plugin uses); also pins `use_iceberg_partition_pruning = 1`
- `docker/clickhouse/users.d/users.xml` — defines **two** users:
  - `atmosphere_reader` — bound to the readonly profile, password from `CLICKHOUSE_READER_PASSWORD` env. This is the user Grafana logs in as.
  - `atmosphere_admin` — bound to the `default` profile (full DDL), password from `CLICKHOUSE_ADMIN_PASSWORD` env. Used only by the init container to run `CREATE DATABASE polaris_catalog`. Network-restricted to the Docker network via `<networks><ip>` rules.
- `grafana/sql/` directory with 23 `.sql` files — one per panel. Filename maps to panel id, e.g. `panel_01_rolling_sentiment.sql`, `panel_12_trending_hashtags.sql`, etc. Uses `$__timeFilter(bucket_min)` and Grafana variables (`$window`) as runtime placeholders.
- `grafana/dashboards/atmosphere.template.json` — the dashboard skeleton. Panels reference their SQL by filename key (e.g. `"queryFile": "panel_12_trending_hashtags.sql"`). The build script replaces these with inline `rawSql` on output.
- `scripts/build-dashboard.py` — reads `atmosphere.template.json` + all `grafana/sql/*.sql` files, substitutes the SQL into panel `targets[0].rawSql`, writes to `grafana/dashboards/atmosphere.json`. Deterministic (sorted keys) so diffs are minimal.
- *(no new grafana/Dockerfile needed — [grafana/Dockerfile](grafana/Dockerfile) already exists and installs the Infinity plugin; we edit the existing file to swap plugin names, see "Modified files" below)*
- `grafana/provisioning/datasources/clickhouse.yml` — new datasource config (native protocol, port 9000, `defaultDatabase = polaris_catalog`, user `$CLICKHOUSE_READER_USER`, password `$CLICKHOUSE_READER_PASSWORD`)

### Modified files
- [docker-compose.yml](docker-compose.yml) — add `clickhouse` service: `clickhouse/clickhouse-server:26.1-alpine`, 2 GB memory, joins **both** `atmosphere-data` and `atmosphere-frontend` networks, mounts `./docker/clickhouse/config.d` and `./docker/clickhouse/users.d` as read-only volumes plus a persistent `clickhouse-data:/var/lib/clickhouse` named volume (added to the `volumes:` block), `depends_on: polaris` (condition: `service_healthy`) so it starts in parallel with init's wait steps. Health check via `wget -qO- http://localhost:8123/ping`. **Init dependency update:** `init.depends_on` gains `clickhouse: service_started` (container-up only — init polls `/ping` itself for readiness). This means init blocks until *both* Polaris and ClickHouse exist before running, so it can bootstrap both systems in one pass. Grafana + spark-unified continue to `depends_on: init (service_completed_successfully)` — no change to their dependency lines. Bump stack memory note from ~22 GB to ~24 GB.
- [grafana/Dockerfile](grafana/Dockerfile) — one-line edit: replace `grafana cli --pluginsDir /opt/grafana/plugins plugins install yesoreyeram-infinity-datasource` with `grafana cli --pluginsDir /opt/grafana/plugins plugins install grafana-clickhouse-datasource`. Existing `GF_PATHS_PLUGINS` env var and chown setup remain unchanged.
- [infra/init/setup.py](infra/init/setup.py) — expanded bootstrap flow:
  1. `create_rustfs_bucket()` — unchanged
  2. `create_catalog()` — unchanged
  3. `create_namespaces()` — unchanged
  4. **NEW** `create_clickhouse_reader(token)` — creates `clickhouse` Polaris principal (secret from `CLICKHOUSE_CLIENT_SECRET` env), `clickhouse_reader` principal role, `atmosphere_reader` catalog role with `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `NAMESPACE_LIST_TABLES`, `NAMESPACE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES`, wires `atmosphere_reader → clickhouse_reader → clickhouse` grants. Idempotent.
  5. **NEW** `wait_for_clickhouse()` — poll `http://clickhouse:8123/ping` until 200 (reuses the same `wait_for_service` helper)
  6. **NEW** `create_clickhouse_database()` — HTTP POST the `CREATE DATABASE IF NOT EXISTS polaris_catalog ENGINE = DataLakeCatalog(...)` statement to `http://clickhouse:8123/?user=default` (uses the built-in `default` user for DDL since `atmosphere_reader` is readonly). Idempotent via `IF NOT EXISTS`.
  All ClickHouse bootstrap uses Python `requests` (already in the init image's base deps) — no `clickhouse-client` binary needed.
- [.env.example](.env.example) — add **six** variables (three credential pairs):
  - `CLICKHOUSE_CLIENT_ID=clickhouse` (Polaris OAuth client id — used by DataLakeCatalog engine)
  - `CLICKHOUSE_CLIENT_SECRET=clickhouse-secret-key` (Polaris OAuth client secret)
  - `CLICKHOUSE_READER_USER=atmosphere_reader` (ClickHouse user Grafana logs in as)
  - `CLICKHOUSE_READER_PASSWORD=atmosphere-reader-secret` (ClickHouse reader password)
  - `CLICKHOUSE_ADMIN_USER=atmosphere_admin` (ClickHouse user init uses for DDL, never touched after)
  - `CLICKHOUSE_ADMIN_PASSWORD=atmosphere-admin-secret` (ClickHouse admin password)
- [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml) — **delete** this file; Infinity datasource is retired
- [grafana/dashboards/atmosphere.json](grafana/dashboards/atmosphere.json) — rewrite all 23 panels to use `grafana-clickhouse-datasource` with SQL against `polaris_catalog.mart.<table>`. Per-panel treatment:
  - **20 simple panels** (sentiment_timeseries, top_posts, firehose_stats, language_distribution, content_breakdown, embed_usage, engagement_velocity, most_mentioned, events_per_second): straight `SELECT ... FROM polaris_catalog.mart.<table> WHERE $__timeFilter(bucket_min) ORDER BY bucket_min`
  - **Panel 6 (Operations Breakdown)**: raw query against `raw_events` — rewrite as ClickHouse SQL against `polaris_catalog.raw.raw_events`
  - **Panel 12 (Trending Hashtags)**: port the windowed spike-ratio logic from [spark/transforms/sql/mart/read_trending_hashtags.sql](spark/transforms/sql/mart/read_trending_hashtags.sql) into ClickHouse SQL using window functions. Add Grafana custom variable `$window` with options `1h/6h/24h`
  - **Panels 18–19 (pipeline_health)**: investigate Iceberg view passthrough via DataLakeCatalog → if unsupported, inline the `COUNT/MAX(event_time)` logic from the original view as ClickHouse SQL against upstream core/mart tables (cheapest fallback)
- [scripts/smoke-test.sh](scripts/smoke-test.sh) — replace `curl http://query-api:8000/...` probes with `docker exec clickhouse clickhouse-client --query "SELECT count() FROM polaris_catalog.mart.<table>"` for each mart; assert non-zero row counts after data has flowed
- [Makefile](Makefile) — add new target `dashboard-build` that runs `python scripts/build-dashboard.py`; `make up` prereqs on `dashboard-build` so the dashboard JSON is always regenerated from `grafana/sql/*.sql` before Grafana reads it. `make up` / `make down` / `make smoke-test` flows otherwise unchanged.
- [docs/ROADMAP.md](docs/ROADMAP.md) — rewrite §8 Components 6.1 (Query API → ClickHouse Service) and 6.2 (Infinity → ClickHouse datasource); leave panel-level §6.3–6.8 tasks as-is (panel semantics unchanged)
- [docs/TRD.md](docs/TRD.md) — replace all query-api + Infinity references (lines 97, 195, 268, 305, 314–317, 327, 341, 410–411, 452, 571) with ClickHouse + official datasource. Update container count in FR-23 from 8 to 8 (swap out, not remove)
- [docs/TDD.md](docs/TDD.md) — replace all query-api architecture sections (lines 92, 119, 136, 150, 165, 541–542, 555–569, 748, 771–778, 881, 922, 954, 972, 984, 1024, 1090, 1108, 1120, 1128, 1172) with ClickHouse + DataLakeCatalog description
- [CLAUDE.md](CLAUDE.md) — update the Key Files table (remove `spark/serving/query_api.py`, `spark/conf/spark-defaults-query.conf`, `spark/Dockerfile`; add `docker/clickhouse/`), update the Conventions section (replace the "Query serving" bullet; delete the "Maintenance" bullet since that responsibility moves to Heimdall)
- [README.md](README.md) — swap the Mermaid diagram ([README.md:11-29](README.md#L11-L29)) to show `clickhouse` instead of `query-api`, update the Tech Stack table (remove "FastAPI + PySpark", add "ClickHouse" and "grafana-clickhouse-datasource"), update the Project Structure tree (remove `spark/serving/`, add `docker/clickhouse/`)

### Future deployment mode — Grafana Cloud via Cloudflare Tunnel (M7, documented only)

At the end of local development, the local Grafana container is **removed** and the `cloudflared` container stands up in its place on the same `atmosphere-frontend` network. Grafana Cloud consumes ClickHouse remotely through the tunnel. This is an atomic swap, not an addition — the frontend network shape stays identical, only the consumer changes:

```
 before (this PR):                          after (M7 swap):
 ┌─ atmosphere-frontend ─┐                  ┌─ atmosphere-frontend ─┐
 │  grafana              │                  │  cloudflared          │
 │  clickhouse  ◄────────┤                  │  clickhouse  ◄────────┤
 └───────────────────────┘                  └───────────────────────┘
                                                    │
                                                    ▼
                                            Cloudflare edge → Grafana Cloud
```

Why this matters for the current PR: ClickHouse's **dual-network membership** (`atmosphere-data` for Polaris/RustFS, `atmosphere-frontend` for its consumer) is the enabler — the M7 swap is just "replace grafana with cloudflared on the same network, point cloudflared ingress at `http://clickhouse:8123` or `clickhouse:9440`". No ClickHouse-side changes required.

M7 follow-up work (out of scope here):
- **Transport**: Cloudflare Tunnel ingress rule routes `atmosphere.<domain>` to `clickhouse:8123` (HTTP) or `clickhouse:9440` (native TLS). Cloudflare terminates TLS at the edge → plain inside the Docker network, so ClickHouse doesn't need a server cert.
- **Auth**: rotate `CLICKHOUSE_READER_PASSWORD` to a strong value in the deployed `.env`; the default seed is for local dev only.
- **Grafana Cloud datasource**: configured via the Cloud UI (no provisioning YAML on our side), pointing at `https://atmosphere.<domain>`.
- Track as new issue **CH-11** (under M7): "Expose ClickHouse via Cloudflare Tunnel and decommission local Grafana container".

### Maintenance job — owned by Heimdall (out of scope here)
`query-api` previously hosted the `/api/maintenance/run` endpoint that ran Iceberg compaction inside its own SparkSession. That responsibility moves to **Heimdall**, the new Python monitor service (`scripts/heimdall/`, `make heimdall`), which will handle table-stats checks + compaction scheduling as part of its monitoring remit. This ClickHouse PR does **not** re-home the maintenance job.

**Open question for the Heimdall build (not this PR):** how Heimdall triggers compaction — either (a) spawn a short-lived SparkSession via `docker compose run --rm spark-unified python -m spark.analysis.maintenance`, or (b) drive it through ClickHouse if Iceberg write procedures like `rewrite_data_files` become available via DataLakeCatalog. Resolve when building Heimdall; not a blocker here.

## Reused functions / utilities
- [infra/init/setup.py:49 `get_polaris_token()`](infra/init/setup.py#L49) and [infra/init/setup.py:61 `polaris_headers()`](infra/init/setup.py#L61) — reuse for the new `create_clickhouse_reader()` function
- [spark/transforms/sql/mart/read_*.sql](spark/transforms/sql/mart/) — port these queries verbatim into panel JSON (ClickHouse SQL dialect is compatible for the simple aggregations; `read_trending_hashtags.sql` is the only one needing window-function adaptation)
- RustFS endpoint env pattern from [infra/init/setup.py:15](infra/init/setup.py#L15) — reuse for `storage_endpoint` construction in the ClickHouse init script

## Implementation order

1. **Docker compose + ClickHouse image** — get the service up with no catalog wiring; verify `clickhouse-client --query "SELECT 1"` works inside the container
2. **Polaris reader bootstrap** — extend `infra/init/setup.py`; verify with `curl` that the new principal can obtain a token and hit `/v1/config`
3. **ClickHouse DataLakeCatalog bootstrap** — init-db.d script that creates the database; verify `SELECT count() FROM polaris_catalog.mart.mart_events_per_second` returns rows
4. **Grafana datasource provisioning** — new `clickhouse.yml`, delete `infinity.yml`; verify datasource "Test" button is green
5. **Dashboard panel rewrite** — panel-by-panel. Start with one simple panel end-to-end, then batch the other 19, then tackle the 3 complex panels (6, 12, 18–19)
6. **Pipeline health decision** — at panel-18/19 time, branch: test Iceberg view passthrough → if it fails, rewrite inline → if that's unacceptably slow, materialize as a new mart in a follow-up
7. **Smoke test rewrite** — once all panels render, port `scripts/smoke-test.sh` to the ClickHouse probe pattern
8. **Docs update** — ROADMAP, TRD, TDD, CLAUDE.md, README all updated in one pass

## Verification

End-to-end validation (all must pass before merge):

1. **Cold start**: `make clean && make up` → all containers healthy inside 90 seconds (including ClickHouse); no manual steps required
2. **ClickHouse connectivity**: `docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM polaris_catalog"` lists all namespaces.tables (raw, staging, core, mart)
3. **Read-only enforcement**:
   - ClickHouse-level: `docker exec clickhouse clickhouse-client --user atmosphere_reader --query "CREATE DATABASE x"` fails with readonly error
   - Polaris-level: attempting `INSERT INTO polaris_catalog.mart.mart_events_per_second` through the reader token fails at the Polaris RBAC layer
4. **Grafana datasource**: `curl -u admin:$GRAFANA_ADMIN_PASSWORD http://localhost:3000/api/datasources/uid/clickhouse/health` returns `{"status":"OK"}`
5. **Dashboard render**: open `http://localhost:3000` → Atmosphere dashboard → all 23 panels show live data with auto-refresh at 5s
6. **Trending hashtags variable**: switching the `$window` dropdown (1h/6h/24h) re-renders the panel with expected row counts
7. **Data freshness**: write a new row via spark-unified, observe it in the Grafana panel within one refresh cycle (Iceberg snapshot visibility is per-query, so this should be near-immediate)
8. **Smoke test**: `make smoke-test` exits 0 after all assertions pass
9. **Memory budget**: `docker stats` shows total stack usage ≤ 24 GB (up from ~22 GB — ClickHouse adds ~2 GB)
10. **Doc consistency**: `grep -r "query-api\|query_api\|Infinity\|yesoreyeram" docs/ CLAUDE.md README.md` returns zero results (excluding historical changelogs if any)

---

## Linear issue list (for a follow-up session to create)

This work slots into **M6 Dashboard** (currently In Progress). Each issue below is sized to be one reviewable PR or one self-contained implementation chunk. Dependencies form a strict chain except where noted — issues marked *(parallelizable)* can start as soon as their explicit dependency closes.

### CH-01: Add `clickhouse` service to docker-compose
**Depends on:** none — start here.
**Summary:** Introduce `clickhouse/clickhouse-server:26.1-alpine` as a new container joining both `atmosphere-data` and `atmosphere-frontend` networks. 2 GB memory limit. Persistent `clickhouse-data:/var/lib/clickhouse` volume (warms DataLakeCatalog cache across restarts). **Verify during implementation** that the alpine image ships `wget` and `clickhouse-client`; if not, either add them via a thin custom Dockerfile or fall back to the full `:26.1` tag. Mounts `./docker/clickhouse/config.d` and `./docker/clickhouse/users.d` as read-only volumes. Health check via `wget -qO- http://localhost:8123/ping`. `depends_on: polaris` with condition `service_healthy`. Authoring the ClickHouse XML configs lives in this ticket: `datalake.xml` enables `allow_experimental_database_unity_catalog` profile-wide; `readonly-profile.xml` defines the `atmosphere_reader` profile (`readonly=1`, `use_iceberg_partition_pruning=1`); `users.xml` defines **two** users — `atmosphere_reader` (bound to readonly profile, password from `CLICKHOUSE_READER_PASSWORD`, used by Grafana) and `atmosphere_admin` (bound to `default` profile for DDL, password from `CLICKHOUSE_ADMIN_PASSWORD`, network-restricted to the Docker subnet via `<networks><ip>` rules, used only by init for `CREATE DATABASE`).
**Files:** `docker-compose.yml`, `docker/clickhouse/config.d/datalake.xml`, `docker/clickhouse/users.d/readonly-profile.xml`, `docker/clickhouse/users.d/users.xml`, `.env.example` (add **six** vars — three credential pairs: `CLICKHOUSE_CLIENT_ID`, `CLICKHOUSE_CLIENT_SECRET`, `CLICKHOUSE_READER_USER`, `CLICKHOUSE_READER_PASSWORD`, `CLICKHOUSE_ADMIN_USER`, `CLICKHOUSE_ADMIN_PASSWORD`)
**Acceptance:**
- `make up` brings up `clickhouse` alongside existing services; `docker compose ps` shows healthy
- `docker exec clickhouse wget -qO- http://localhost:8123/ping` returns `Ok.`
- `docker exec clickhouse clickhouse-client --user $CLICKHOUSE_READER_USER --password $CLICKHOUSE_READER_PASSWORD --query "SELECT 1"` returns `1`
- `docker exec clickhouse clickhouse-client --user $CLICKHOUSE_READER_USER --password $CLICKHOUSE_READER_PASSWORD --query "CREATE DATABASE x"` fails with readonly error
- Stack fits within 24 GB via `docker stats`

### CH-02: Provision Polaris read-only principal + RBAC for ClickHouse (catalog-level grants)
**Depends on:** none *(parallelizable with CH-01)*.
**Summary:** Extend `infra/init/setup.py` with a `create_clickhouse_reader(token)` function that creates the `clickhouse` principal (secret from `CLICKHOUSE_CLIENT_SECRET` env), a `clickhouse_reader` principal role, and an `atmosphere_reader` catalog role. Grant the following privileges **at the catalog level** (not per-namespace): `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `NAMESPACE_LIST_TABLES`, `NAMESPACE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES`. Catalog-level cascades to all namespaces (raw, staging, core, mart) — required because panel 6 reads `polaris_catalog.raw.raw_events` directly. Wires `atmosphere_reader → clickhouse_reader → clickhouse` grants. Fully idempotent like the existing setup steps. Reuses [`get_polaris_token()`](infra/init/setup.py#L49) and [`polaris_headers()`](infra/init/setup.py#L61).
**Files:** `infra/init/setup.py`
**Acceptance:**
- Re-running `make up` leaves Polaris state idempotent (no duplicate grants, no errors)
- `curl -u clickhouse:<secret> http://localhost:8181/api/catalog/v1/oauth/tokens -d "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL"` returns a valid token
- Attempting an INSERT via that token against any Iceberg table is rejected by Polaris RBAC

### CH-03: Extend init container to bootstrap the `polaris_catalog` DataLakeCatalog database
**Depends on:** CH-01, CH-02.
**Summary:** Reuse the existing `init` container — no new init paths, no sidecar containers, no `/docker-entrypoint-initdb.d/`. Dependency direction: `clickhouse` starts in parallel with init's other waits (both only need Polaris healthy), then `init.depends_on` adds `clickhouse: service_started` so init blocks until Polaris AND ClickHouse both exist. Init then does the whole bootstrap in one pass.

Add two new functions to [infra/init/setup.py](infra/init/setup.py):
- `wait_for_clickhouse()` — polls `http://clickhouse:8123/ping` until 200 (reuses the existing [`wait_for_service`](infra/init/setup.py#L139) helper)
- `create_clickhouse_database()` — HTTP POST to `http://clickhouse:8123/?user=${CLICKHOUSE_ADMIN_USER}&password=${CLICKHOUSE_ADMIN_PASSWORD}` with body:
```sql
CREATE DATABASE IF NOT EXISTS polaris_catalog
ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog/v1')
SETTINGS
    catalog_type = 'rest',
    catalog_credential = '${CLICKHOUSE_CLIENT_ID}:${CLICKHOUSE_CLIENT_SECRET}',
    warehouse = 'atmosphere',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    auth_header = 'Polaris-Realm:POLARIS',
    oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://rustfs:9000',
    vended_credentials = true;
```
`main()` becomes a 7-step flow: `[1/7] Wait RustFS → [2/7] Create bucket → [3/7] Wait Polaris → [4/7] Create catalog → [5/7] Create namespaces → [6/7] Wait ClickHouse → [7/7] CREATE DATABASE polaris_catalog`. `IF NOT EXISTS` keeps step 7 idempotent. `init.depends_on` in docker-compose adds `clickhouse: service_started` (container-up only; init polls `/ping` itself for actual readiness). No new binaries in the init image — `requests` is already present.
**Files:** `infra/init/setup.py`, `docker-compose.yml` (`init.depends_on.clickhouse`)
**Acceptance:**
- `docker exec clickhouse clickhouse-client --query "SHOW DATABASES"` (via default user) lists `polaris_catalog`
- `docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM polaris_catalog"` lists `raw.*`, `staging.*`, `core.*`, `mart.*` tables (~25)
- `docker exec clickhouse clickhouse-client --query "SELECT count() FROM polaris_catalog.\\\`mart.mart_events_per_second\\\`"` returns a non-zero count after streaming has warmed up
- Bounce test: `docker compose restart clickhouse` → the in-memory `polaris_catalog` reappears on next `make up` (init re-runs and re-creates it idempotently)
- Re-running `make up` twice back-to-back leaves init logs showing "already exists" for every step

### CH-04: Swap Grafana Infinity datasource for official ClickHouse datasource (plugin baked at build time)
**Depends on:** CH-03.
**Summary:** [grafana/Dockerfile](grafana/Dockerfile) already exists and bakes `yesoreyeram-infinity-datasource` via `grafana cli`. One-line edit: swap that plugin name for `grafana-clickhouse-datasource`. Delete `grafana/provisioning/datasources/infinity.yml`. Create `grafana/provisioning/datasources/clickhouse.yml` with native TCP protocol, `host: clickhouse`, `port: 9000`, `protocol: native`, `defaultDatabase: polaris_catalog`, `username: $CLICKHOUSE_READER_USER`, `secureJsonData.password: $CLICKHOUSE_READER_PASSWORD`, `uid: clickhouse`. Grafana is already on `build: ./grafana` in docker-compose — no docker-compose change needed for this ticket.
**Files:** [grafana/Dockerfile](grafana/Dockerfile) (one-line plugin swap), `grafana/provisioning/datasources/clickhouse.yml` (new), `grafana/provisioning/datasources/infinity.yml` (deleted)
**Acceptance:**
- `docker compose build grafana` succeeds and produces an image with `grafana-clickhouse-datasource` preinstalled (verify via `docker run --rm <img> grafana-cli plugins ls`)
- Grafana boot time returns to baseline (no runtime plugin download — `make up` Grafana startup <10s after image is built)
- `curl -u admin:$GRAFANA_ADMIN_PASSWORD http://localhost:3000/api/datasources/uid/clickhouse/health` returns `{"status":"OK"}`
- Grafana UI → Datasources → ClickHouse → Test: green
- Infinity datasource no longer appears in Grafana

### CH-05: Introduce dashboard build pipeline + rewrite simple panels (20 panels)
**Depends on:** CH-04.
**Summary:** Establish the SQL-file pipeline and port the 20 simple panels. Create `grafana/sql/` directory with one `.sql` file per panel. Create `grafana/dashboards/atmosphere.template.json` as the dashboard skeleton (copied from current `atmosphere.json`), with each panel's `targets[0].rawSql` replaced by a `queryFile` reference e.g. `"queryFile": "panel_01_rolling_sentiment.sql"`. Create `scripts/build-dashboard.py` that walks the template, reads referenced .sql files from `grafana/sql/`, substitutes them into a new `targets[0].rawSql` field, and writes `grafana/dashboards/atmosphere.json`. Add `dashboard-build` Makefile target wired into `make up` as a prereq. For the 20 simple panels, write SQL against `polaris_catalog.mart.<table>` using the `$__timeFilter(bucket_min)` macro. Flip the panel `datasource` block in the template from Infinity uid to `clickhouse` uid.
**Files:** `grafana/sql/panel_01_*.sql` through `panel_23_*.sql` (the 20 simple ones only this ticket — 6, 12, 18, 19 land in CH-06/07), `grafana/dashboards/atmosphere.template.json`, `scripts/build-dashboard.py`, `Makefile`, `grafana/dashboards/atmosphere.json` (generated output, still committed for diff-ability)
**Acceptance:**
- `make dashboard-build` emits a valid `atmosphere.json` that Grafana provisions without error
- All 20 simple panels render live data at 5-second refresh
- Time-range selector reflects in panel queries (verified via panel inspector)
- Re-running `make dashboard-build` with no source changes produces a byte-identical `atmosphere.json` (deterministic output)
- No Infinity references remain in those panel definitions

### CH-06: Rewrite complex dashboard panels (Operations Breakdown + Trending Hashtags)
**Depends on:** CH-05.
**Summary:** Panel 6 (Operations Breakdown) currently runs a raw SQL against `raw_events` via query-api's `/api/sql` endpoint — rewrite as ClickHouse SQL against `polaris_catalog.raw.raw_events`. Panel 12 (Trending Hashtags) needs the windowed spike-ratio logic from [spark/transforms/sql/mart/read_trending_hashtags.sql](spark/transforms/sql/mart/read_trending_hashtags.sql) ported to ClickHouse window functions. Add a Grafana custom template variable `$window` with options `1h / 6h / 24h` and interpolate into the SQL.
**Files:** `grafana/sql/panel_06_operations_breakdown.sql`, `grafana/sql/panel_12_trending_hashtags.sql`, `grafana/dashboards/atmosphere.template.json` (add `$window` variable definition + queryFile refs), regenerated `grafana/dashboards/atmosphere.json`
**Acceptance:**
- Panel 6 renders the create/update/delete breakdown over time
- Panel 12 renders with spike-ratio column populated
- Switching the `$window` dropdown to `1h`, `6h`, `24h` re-renders panel 12 with different row counts and values
- Panel 12 query returns in <1 second (match original performance baseline)

### CH-07: Resolve pipeline_health panels (18, 19) — view passthrough → inline → materialize
**Depends on:** CH-05.
**Summary:** Execute the decision ladder:
1. **Try Iceberg view passthrough** — query `polaris_catalog.core.pipeline_health` (the existing Iceberg VIEW) directly from ClickHouse. If DataLakeCatalog surfaces it as a queryable table, use it and close.
2. **Inline fallback** — if views don't surface, port the view's `COUNT / MAX(event_time)` logic into ClickHouse SQL directly in the panel definitions, reading from upstream core tables. Measure latency; if <2s per panel, ship it.
3. **Materialize last resort** — if inline latency is unacceptable, add `mart_pipeline_health` to [spark/transforms/marts.py](spark/transforms/marts.py) as an 11th streaming mart (1-minute tumbling window like the others). Panels then read from the materialized mart. Opens a new milestone-internal dependency.
**Files:** `grafana/sql/panel_18_processing_lag.sql`, `grafana/sql/panel_19_last_batch_timestamp.sql`, regenerated `grafana/dashboards/atmosphere.json` (all cases); `spark/transforms/marts.py`, `spark/transforms/sql/mart/pipeline_health.sql` (only if materialize path is taken)
**Acceptance:**
- Panels 18 (Processing Lag) and 19 (Last Batch Timestamp) render live data
- Panel latency ≤2 seconds per refresh
- Document which of the three paths was taken in the PR description

### CH-08: Rewrite `scripts/smoke-test.sh` as three-layer ClickHouse/Grafana probe
**Depends on:** CH-05 (simple panels must be proven working first).
**Summary:** Replace the `curl http://query-api:8000/...` pattern with a three-layer probe that must all pass for smoke-test to exit 0:
1. **ClickHouse row counts** — `docker exec clickhouse clickhouse-client --user $CLICKHOUSE_READER_USER --password $CLICKHOUSE_READER_PASSWORD --query "SELECT count() FROM polaris_catalog.\\\`mart.<table>\\\`"` for each mart table; assert non-zero after the `--wait` period.
2. **Grafana datasource health** — `curl -u admin:$GRAFANA_ADMIN_PASSWORD http://localhost:3000/api/datasources/uid/clickhouse/health` must return `{"status":"OK"}`. Exercises end-to-end Grafana → ClickHouse → Polaris → RustFS auth path.
3. **Dashboard visibility** — `curl -u admin:$GRAFANA_ADMIN_PASSWORD "http://localhost:3000/api/search?query=Atmosphere"` must return a non-empty result set confirming the provisioned dashboard loaded.
**Files:** `scripts/smoke-test.sh`
**Acceptance:**
- `make smoke-test` exits 0 on a fresh `make up`
- All three probe layers reported individually in the test output
- Test script runs under the same ~3-minute budget as the pre-swap version

### CH-09: Update docs (ROADMAP, TRD, TDD, CLAUDE.md, README)
**Depends on:** CH-01 through CH-08 (documents the final, working state).
**Summary:** Rewrite all query-api + Infinity references to ClickHouse + official datasource across:
- [docs/ROADMAP.md](docs/ROADMAP.md) §8 Components 6.1 (Query API → ClickHouse Service) and 6.2 (Infinity → ClickHouse datasource); leave panel-level §6.3–6.8 tasks as-is
- [docs/TRD.md](docs/TRD.md) — every line referencing query-api, Infinity, or FastAPI (lines ~97, 195, 268, 305, 314–317, 327, 341, 410–411, 452, 571 at time of planning)
- [docs/TDD.md](docs/TDD.md) — architecture sections describing query-api (~lines 92, 119, 136, 150, 165, 541–569, 748, 771–778, 881, 922, 954, 972, 984, 1024, 1090, 1108, 1120, 1128, 1172)
- [CLAUDE.md](CLAUDE.md) — Key Files table (remove `spark/serving/query_api.py`, `spark/conf/spark-defaults-query.conf`, `spark/Dockerfile`; add `docker/clickhouse/`), Conventions section (replace Query serving and Maintenance bullets)
- [README.md](README.md) — Mermaid diagram, Tech Stack table (remove FastAPI+PySpark, add ClickHouse + grafana-clickhouse-datasource), Project Structure tree
**Acceptance:**
- `grep -r "query-api\|query_api\|Infinity\|yesoreyeram\|FastAPI" docs/ CLAUDE.md README.md` returns zero results
- README Mermaid diagram reflects the new topology
- All three requirement docs remain cross-consistent

### CH-11 *(M7, not in this PR)*: Decommission local Grafana container and expose ClickHouse via Cloudflare Tunnel
**Depends on:** CH-01 through CH-09 merged, plus an M7 Cloudflare Tunnel domain + token.
**Summary:** Once local development is complete, remove the `grafana` service from docker-compose.yml and add a `cloudflared` service joined to `atmosphere-frontend` (the same network ClickHouse already belongs to). Configure tunnel ingress to route the public hostname to `http://clickhouse:8123` (or `clickhouse:9440` for native TLS). Rotate `CLICKHOUSE_READER_PASSWORD` from the local-dev seed to a strong secret. Grafana Cloud configures its datasource via the Cloud UI pointing at `https://atmosphere.<domain>` — no provisioning YAML on our side. Local dashboards in `grafana/dashboards/atmosphere.json` become an exportable JSON template for Grafana Cloud import rather than a provisioned file.
**Files:** `docker-compose.yml` (remove grafana, add cloudflared), `infra/cloudflare/config.yml` (new ingress rules), `.env` on the deployment host (rotated secrets + `TUNNEL_TOKEN`)
**Acceptance:**
- `docker compose ps` shows `cloudflared` healthy and `grafana` absent
- `curl -I https://atmosphere.<domain>/ping` returns 200 via the tunnel
- A Grafana Cloud dashboard configured against the public ClickHouse endpoint renders live data
- Local dev path (earlier milestones) still works by uncommenting/restoring the grafana service from the PR history

### Maintenance job — **not in this PR**
Iceberg compaction (`rewrite_data_files` + `expire_snapshots`) is Heimdall's responsibility, not a ClickHouse concern. Tracked separately under the Heimdall work. This PR does not add a `make maintain` target or re-home `spark/analysis/maintenance.py`.

---

## Dependency graph

```
CH-01 ─┐
       ├─► CH-03 ─► CH-04 ─► CH-05 ─┬─► CH-06 ─┐
CH-02 ─┘                            ├─► CH-07 ─┼─► CH-09
                                    └─► CH-08 ─┘
```

Concrete runtime orchestration produced by CH-01 + CH-03 (what `make up` actually does):
```
rustfs (healthy) ─┐                   ┌─► init (runs full 7-step bootstrap) ─► spark-unified
postgres ─► polaris-bootstrap ─► polaris (healthy) ─┤                                            └─► grafana
                                  └─► clickhouse (started) ─┘
```
init blocks on `rustfs:healthy + polaris:healthy + clickhouse:started`. ClickHouse starts in parallel with init's Polaris wait, so the swap adds no wall-clock time to `make up` beyond the ClickHouse boot itself (~3s).

CH-01 and CH-02 can run in parallel. CH-06, CH-07, and CH-08 can run in parallel once CH-05 lands. CH-09 is the terminal docs pass, and CH-10 is an intentional follow-up outside this PR train.

