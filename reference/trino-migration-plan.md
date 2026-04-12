# Plan — Replace query-api with Trino

> **Source of truth.** This file at `reference/trino-migration-plan.md` is the canonical plan. Update it in place as phases complete or decisions change.

## Context

A previous session scrapped the query-api and monitor layers entirely. Current state:

- **Gone:** `query-api` service (removed from [docker-compose.yml](docker-compose.yml)), all of [spark/serving/](spark/serving/), [spark/Dockerfile](spark/Dockerfile), [spark/conf/spark-defaults-query.conf](spark/conf/spark-defaults-query.conf), `scripts/monitor.sh`, all of `scripts/monitor/`, all the monitor state logs under [logs/](logs/).
- **Makefile** has been stripped to: `up`, `down`, `logs`, `status`, `clean`, `reset-checkpoint`, `replay`, `smoke-test`. No more `maintain`, `monitor`, `health`, `sizing-*`.
- **Stack reduces to:** `rustfs`, `postgres`, `polaris-bootstrap`, `polaris`, `init`, `spark-unified`, `grafana`. Grafana boots but has no working datasource — it still has uncommitted edits from an earlier session pointing the Infinity provisioning at `http://query-api:8000`.
- **`grafana.depends_on`** was rewired to `init` as a placeholder and needs to become `trino` once the new service exists.
- **Orphans, not yet deleted:** [spark/analysis/maintenance.py](spark/analysis/maintenance.py), [spark/analysis/sizing.py](spark/analysis/sizing.py), [spark/transforms/sql/mart/read_*.sql](spark/transforms/sql/mart/) (only query-api consumed them), [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml), [grafana/dashboards/atmosphere.json](grafana/dashboards/atmosphere.json) with ~30 `http://query-api:8000/...` panel URLs.
- **Uncommitted in-flight work to preserve:** uncommitted edits to [spark/transforms/core.py](spark/transforms/core.py), [spark/transforms/marts.py](spark/transforms/marts.py), [spark/transforms/sentiment.py](spark/transforms/sentiment.py), [spark/conf/spark-defaults.conf](spark/conf/spark-defaults.conf), plus older uncommitted edits in [CLAUDE.md](CLAUDE.md), [README.md](README.md), [docs/TRD.md](docs/TRD.md), [docs/TDD.md](docs/TDD.md), [grafana/Dockerfile](grafana/Dockerfile), [grafana/dashboards/atmosphere.json](grafana/dashboards/atmosphere.json), [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml). **Do not clobber these** — they get folded into this migration's diffs.

The replacement is Trino with the Iceberg connector pointed at Polaris. Trino gives a standard SQL serving layer, a native Grafana plugin (with proper `$__timeFilter` macros and a query inspector), and first-class Iceberg maintenance procedures (`ALTER TABLE … EXECUTE optimize`, `expire_snapshots`) that replace the SparkSession-hosted compaction path.

**In scope:** Trino service, catalog + SQL translation, Grafana datasource swap + panel rewrite, new `scripts/maintain.sh` + sizing tool, deletion of remaining query-api orphans, full doc sweep, final cloudflared + auth phase for going live.

**Out of scope:** Monitor rebuild. The monitor was scrapped and its redesign is a separate pending decision. This plan does not touch monitoring.

## Decisions locked in

| # | Decision | Choice |
|---|---|---|
| D1 | Trino topology | **Single-node** (coordinator + worker in one JVM) |
| D2 | Grafana datasource | **Native `grafana-trino-datasource` plugin** |
| D3 | SQL file layout | **New top-level `trino/sql/` dir** (not under `spark/`) |
| D4 | Maintenance relocation | **Trino `EXECUTE` procedures** (`optimize`, `expire_snapshots`) driven by shell loop |
| D5 | Sizing analysis | **Port to Trino** — times queries through Trino, reads Iceberg metadata tables |
| D6 | Catalog naming | **`atmosphere`** Trino catalog → identical FQNs to Spark (`atmosphere.mart.<t>`) |
| D7 | Network exposure | **Local dev first, cloudflared + HTTP basic auth last.** The entire migration is developed against a fully local stack where Trino is reachable only from Grafana and the host. A final phase wires up cloudflared, flips Trino to `http-server.authentication.type=PASSWORD` with a password file, and ports Grafana to be the public surface. Auth config does **not** ship in phase 1. |
| D8 | Memory budget | **4 GB** → total stack ~24 GB |
| D9 | Trino version | **Pinned** — version chosen during phase-0 spike |
| D10 | Maintenance concurrency | **Serial loop** over tables |
| D11 | Sizing doc | **Rerun against Trino**, replace numbers in [docs/mart-sizing-analysis.md](docs/mart-sizing-analysis.md) |
| D12 | ROADMAP treatment | **Add M6.5 "Query layer migration"**, preserve original M6 as shipped |
| — | Documentation tone | **Strictly technical** — no "portfolio framing" or "custom REST API" talking points. Describe what the system does, not why it's impressive |

## Phase 0 — Research findings (pulled from web during planning)

The three unknowns were resolved by fetching the Trino 480 docs and a Polaris+Trino integration write-up. Findings below are authoritative for phase 1; the remaining TODOs are things that can only be verified at runtime against the actual Polaris/RustFS instances, not open questions about Trino itself.

### 0.1 — Trino version

**Pinned: `trinodb/trino:480`** (current stable as of April 2026). Sources: [Trino 480 docs](https://trino.io/docs/current/installation/containers.html), [trinodb/trino Docker Hub](https://hub.docker.com/r/trinodb/trino).

### 0.2 — Trino Iceberg connector with Polaris REST catalog

Exact property set for `trino/etc/catalog/atmosphere.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris:8181/api/catalog
iceberg.rest-catalog.warehouse=atmosphere
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=root:s3cr3t
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
iceberg.rest-catalog.vended-credentials-enabled=false
iceberg.expire-snapshots.min-retention=1d
iceberg.remove-orphan-files.min-retention=1d

fs.native-s3.enabled=true
s3.endpoint=http://rustfs:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=<rustfs user>
s3.aws-secret-key=<rustfs password>
```

**Key decisions baked into this config:**

- **`iceberg.rest-catalog.uri=http://polaris:8181/api/catalog`** — matches Polaris's standard REST endpoint path (no trailing slash).
- **OAuth2 credential format = `clientId:clientSecret`** — Polaris's default bootstrap credentials are `root/s3cr3t` (matches the `POLARIS_BOOTSTRAP_CREDENTIALS` env var in [docker-compose.yml](docker-compose.yml)). **Do not hardcode this in `atmosphere.properties`.** Ship `atmosphere.properties.template` with `${POLARIS_OAUTH2_CREDENTIAL}` alongside the S3 credential placeholders, and have the entrypoint envsubst the whole file at container start. The `POLARIS_OAUTH2_CREDENTIAL` env is wired from the compose file's existing Polaris bootstrap secret.
- **Scope = `PRINCIPAL_ROLE:ALL`** — Polaris's convention for granting all roles assigned to the principal.
- **`vended-credentials-enabled=false`** — Polaris *can* vend temporary S3 credentials per query, but this requires Polaris to hold RustFS's root credentials and mint STS tokens. Our RustFS doesn't speak STS, so we disable vending and Trino uses direct S3 creds.
- **`iceberg.expire-snapshots.min-retention=1d` + `remove-orphan-files.min-retention=1d`** — **critical gotcha.** Trino defaults both floors to 7 days. Atmosphere's convention is 1-day snapshot retention; without these overrides, `EXECUTE expire_snapshots(retention_threshold => '1d')` will throw.

**TODO at phase 1 (runtime verification only):**

- Commit to the envsubst-template path for catalog properties regardless of whether Trino 480 supports `${ENV:VAR}` inline. The template covers three secrets (`RUSTFS_ROOT_USER`, `RUSTFS_ROOT_PASSWORD`, `POLARIS_OAUTH2_CREDENTIAL`) and keeps all of them out of the repo. Volume-mount shape: ship `trino/etc/catalog/atmosphere.properties.template` + a thin entrypoint that runs `envsubst < template > /etc/trino/catalog/atmosphere.properties` before exec'ing the upstream launcher.
- Does Polaris's current deployment expose `/api/catalog` at exactly that path, or has the base path drifted?

### 0.3 — Trino `ALTER TABLE ... EXECUTE` procedures

Verified syntax (Trino 480):

```sql
-- compaction
ALTER TABLE atmosphere.mart.mart_events_per_second EXECUTE optimize;
ALTER TABLE atmosphere.mart.mart_events_per_second EXECUTE optimize(file_size_threshold => '128MB');

-- snapshot expiry (retention_threshold must be >= iceberg.expire-snapshots.min-retention)
ALTER TABLE atmosphere.mart.mart_events_per_second EXECUTE expire_snapshots(retention_threshold => '1d');
ALTER TABLE atmosphere.mart.mart_events_per_second EXECUTE expire_snapshots(retention_threshold => '1d', retain_last => 1);

-- orphan file cleanup (retention_threshold >= iceberg.remove-orphan-files.min-retention)
ALTER TABLE atmosphere.mart.mart_events_per_second EXECUTE remove_orphan_files(retention_threshold => '1d');
```

**Notes applied to `scripts/maintain.sh` design:**

- `file_size_threshold` default is **100 MB**. Mart tables are small (~KBs each file) — the default will coalesce everything into one file per table, which is what we want. No tuning needed.
- `optimize` can take an optional `WHERE` predicate to compact a specific partition. We'll compact whole tables in phase 1; per-partition optimization is a future refinement if runs get slow.
- `expire_snapshots` takes `retain_last` (min ancestors to preserve, default 1) — the 1-day retention + retain_last=1 matches the previous `spark/analysis/maintenance.py` semantics.
- `remove_orphan_files` is new capability (the previous Spark maintenance didn't do this). Add it to the `maintain.sh` loop.

Source: [Trino Iceberg connector docs — ALTER TABLE EXECUTE](https://trino.io/docs/current/connector/iceberg.html#alter-table-execute).

### 0.4 — Grafana Trino datasource plugin

- **Plugin id: `grafana-trino-datasource`** — install via `grafana-cli plugins install grafana-trino-datasource` in [grafana/Dockerfile](grafana/Dockerfile).
- **Supported macros (prefix is `$__`, standard Grafana convention):**
  - `$__timeFilter($column)` — range condition as timestamps (e.g. `WHERE $__timeFilter(bucket_min)`)
  - `$__timeGroup($column, $interval)` — rounds to interval (replaces Spark's `window(ts, '5 seconds')`)
  - `$__timeFrom($column)` / `$__timeTo($column)` — lower/upper bounds as timestamps
  - `$__dateFilter($column)` — date-only variant
  - `$__unixEpochFilter($column)` — Unix epoch variant
  - `$__parseTime` — parse string timestamp
- **Auth:** HTTP Basic, TLS client, JWT access token, or OAuth. Phase 1 uses no auth; phase 9 switches to HTTP Basic.
- **The inline `window(ingested_at, '5 seconds')` query at [grafana/dashboards/atmosphere.json:445](grafana/dashboards/atmosphere.json#L445)** becomes:
  ```sql
  SELECT $__timeGroup(ingested_at, '5s') AS window_start,
         operation,
         count(*) AS op_count
  FROM atmosphere.raw.raw_events
  WHERE $__timeFilter(ingested_at)
  GROUP BY 1, operation
  ORDER BY 1
  ```
  `$__timeGroup` handles the 5-second tumbling window directly — no `date_trunc` arithmetic needed.

Source: [grafana.com/grafana/plugins/trino-datasource](https://grafana.com/grafana/plugins/trino-datasource/).

**Stop for review before phase 1.**

## Phase 1 — Bring Trino up alongside the broken stack

Create the Trino service and its config, verify it can read Iceberg via Polaris. Grafana stays broken in this phase; that is expected.

### Files to create (no auth — local dev only)

- **`trino/etc/config.properties`**:
  ```properties
  coordinator=true
  node-scheduler.include-coordinator=true
  http-server.http.port=8080
  query.max-memory=2GB
  query.max-memory-per-node=1GB
  discovery.uri=http://localhost:8080
  ```
  No authenticator — added in phase 9.
- **`trino/etc/jvm.config`** — start from Trino 480's upstream reference (`-server -Xmx3G -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+ExplicitGCInvokesConcurrent -XX:+ExitOnOutOfMemoryError -XX:+HeapDumpOnOutOfMemoryError -XX:-OmitStackTraceInFastThrow -XX:ReservedCodeCacheSize=512M -XX:PerMethodRecompilationCutoff=10000 -XX:PerBytecodeRecompilationCutoff=10000 -Djdk.attach.allowAttachSelf=true -Djdk.nio.maxCachedBufferSize=2000000`). **Memory budget caveat:** `-Xmx3G` inside a 4 GB `mem_limit` leaves only 1 GB for off-heap (G1 regions, direct buffers, native S3 client, 512 MB code cache). That is tight and can trigger OOM-kills under load. During phase 1 verification, watch `docker stats trino` during a dashboard refresh + `make maintain` run. If RSS trends above 3.5 GB, either bump `mem_limit` to 5 GB (stack total becomes ~25 GB) or drop `-Xmx` to 2.5 GB. Decide before finalizing the docs in phase 7.
- **`trino/etc/node.properties`** — `node.environment=atmosphere`, `node.id=trino-coordinator`, `node.data-dir=/var/trino/data`.
- **`trino/etc/log.properties`** — `io.trino=INFO`.
- **`trino/etc/catalog/atmosphere.properties`** — content from §0.2 above, verbatim. If `${ENV:VAR}` interpolation doesn't work (see §0.2 TODO), ship a `.template` file + entrypoint that runs `envsubst` at container start.

### Files to modify

- **[docker-compose.yml](docker-compose.yml)**
  - Add `trino` service block: image `trinodb/trino:480`, container name `trino`, port `8080:8080`, volume-mount `./trino/etc:/etc/trino:ro`, env vars (`RUSTFS_ROOT_USER`, `RUSTFS_ROOT_PASSWORD`), `depends_on: { init: service_completed_successfully, polaris: service_healthy }`, both networks (`atmosphere-data` + `atmosphere-frontend`), `mem_limit: 4g` (revisit per memory caveat above), healthcheck (10s interval, start_period 30s).
  - **Healthcheck command:** the plan previously called for `curl -fsS http://localhost:8080/v1/info | grep -q '"starting":false'`. Verify `curl` is present in `trinodb/trino:480` before committing. If not, fall back to `wget -qO- http://localhost:8080/v1/info | grep -q '"starting":false'`, or drop to a JVM-side check (`trino --execute "SELECT 1"` runs inside the image and is the most reliable).
  - Change `grafana.depends_on` from `init` → `trino` (the placeholder `init` was added during the scrap and needs to move now that a real dependency exists).

### Verification

- `make up` starts cleanly. `docker compose ps trino` healthy.
- `docker exec trino trino --server http://localhost:8080 --user admin --password` runs `SELECT count(*) FROM atmosphere.mart.mart_events_per_second` (or whichever mart table currently has data) and returns a number.
- `SHOW SCHEMAS IN atmosphere` returns `raw, staging, core, mart`.

**Stop for review before phase 2.**

## Phase 2 — Translate read SQL to Trino dialect

Read-side SQL moves to a new top-level `trino/sql/` dir. Spark writer SQL stays untouched under [spark/transforms/sql/mart/](spark/transforms/sql/mart/).

### Dialect diffs to apply

- `current_timestamp()` → `current_timestamp` (or `now()`)
- `INTERVAL 5 MINUTES` → `INTERVAL '5' MINUTE`
- `get(array, idx)` → `element_at(array, idx + 1)` (Trino arrays are 1-indexed; `element_at` returns NULL on OOB, while `array[i]` throws — use `element_at` for all translated reads)
- Spark tumbling `window(ts, '5 seconds')` → `date_trunc('second', ts)` grouping, or the plugin's `$__timeGroup` macro (spike-dependent)
- Map/struct access stays compatible

### Files to create under `trino/sql/`

One file per dashboard read path, mirroring the current `read_*.sql` set. Exact filenames to create under `trino/sql/` (one-for-one with the current Spark reads):

- `read_events_per_second.sql`
- `read_engagement_velocity.sql`
- `read_sentiment_timeseries.sql`
- `read_most_mentioned.sql`
- `read_language_distribution.sql`
- `read_content_breakdown.sql`
- `read_embed_usage.sql`
- `read_top_posts.sql`
- `read_firehose_stats.sql`
- `read_trending_hashtags.sql`

Plus a translation of the inline ad-hoc query on [grafana/dashboards/atmosphere.json:445](grafana/dashboards/atmosphere.json#L445). Total: 11 Trino SQL files.

### Verification

Each `trino/sql/*.sql` runs against the live Trino and returns a non-empty result set, exercised via `docker exec trino trino -f /etc/trino/sql/<file>` (mount the dir read-only for testing, or pipe via stdin).

## Phase 3 — Swap Grafana over

### Files to modify

- **[grafana/Dockerfile](grafana/Dockerfile)** — add `grafana-cli plugins install grafana-trino-datasource` (exact plugin id from phase-0 spike).
- **[grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml)** — **delete**.
- **`grafana/provisioning/datasources/trino.yml`** — new file. Datasource points at `http://trino:8080`, catalog `atmosphere`, default schema `mart`, basic-auth credentials from env.
- **[grafana/dashboards/atmosphere.json](grafana/dashboards/atmosphere.json)** — every panel (~30 targets) updated:
  - `datasource.type` → `grafana-trino-datasource`
  - `datasource.uid` → `trino-atmosphere`
  - Remove `source: "url"`, `url: http://query-api:8000/...` fields
  - Add `rawSql: "SELECT … FROM atmosphere.mart.<t> WHERE $__timeFilter(bucket_min) …"` from the translated files under `trino/sql/`

### Verification (per the browser-test feedback rule)

- `make up`, hit `http://localhost:3000`, log in, open the Atmosphere dashboard.
- All panels render data. No red triangles.
- Time range selector (last 5 min / 15 min / 1 h) actually filters. This is the first real `$__timeFilter` test.
- Pick one panel, use Grafana's Query Inspector to confirm the SQL reaching Trino is what we expect.
- Check [http://localhost:8080](http://localhost:8080) Trino Web UI shows recent queries from Grafana.

## Phase 4 — Maintenance and sizing tools

### Files to create

- **`scripts/maintain.sh`** — serial loop. Enumerates tables via `trino --execute "SHOW TABLES IN atmosphere.<ns>"` for each of `raw, staging, core, mart`. For each table:
  1. `SELECT count(*) FROM "<t>$files"` → record before-count
  2. `ALTER TABLE atmosphere.<ns>.<t> EXECUTE optimize`
  3. `ALTER TABLE atmosphere.<ns>.<t> EXECUTE expire_snapshots(retention_threshold => '1d')`
  4. `SELECT count(*) FROM "<t>$files"` → record after-count
  5. Print progress line
- **`scripts/sizing-report.sh`** (or `tools/sizing.py` using `trino-python-client` — decision during phase 4 based on whether metadata-table queries are cleaner in SQL or Python) — reads `$files` / `$snapshots` / `$partitions`, times every `trino/sql/*.sql` read query, writes `reports/sizing-<timestamp>.md`.

### Files to delete

- **[spark/analysis/maintenance.py](spark/analysis/maintenance.py)** — orphaned since the scrap; Trino replaces it.
- **[spark/analysis/sizing.py](spark/analysis/sizing.py)** — same.
- **[spark/analysis/__init__.py](spark/analysis/__init__.py)** — delete the empty dir.

### Files to modify

- **[Makefile](Makefile)** — **add new** `maintain` and `sizing-report` targets (previously stripped during the scrap). `.PHONY` line updated. `maintain` invokes `scripts/maintain.sh`; `sizing-report` invokes the new sizing tool.

### Verification

- `make maintain` runs end-to-end, prints per-table before/after file counts, exits 0.
- Iceberg metadata confirms compaction happened: fewer files, new snapshots, old snapshots expired.
- `make sizing-report` produces a fresh report under [reports/](reports/).
- Re-run `make maintain` within 10 min. The old HTTP API rate-limit is gone, but compaction against live ingest still competes for I/O. `scripts/maintain.sh` keeps a lightweight `/tmp/atmosphere-maintain.last` timestamp (or `reports/maintain-last.txt` if persistence across container restarts matters) and refuses to run within 10 minutes of a prior invocation unless invoked with `--force`. This preserves the original semantics — "manual off-hours operation, not a loop" — without needing a serving process to enforce it.

## Phase 5 — Smoke test (monitor rebuild is out of scope, see Context)

### Files to modify

- **[scripts/smoke-test.sh](scripts/smoke-test.sh)** — `EXPECTED_SERVICES` gains `trino` (the previous scrap already dropped `query-api` from the list; verify before editing). Any layer-validation query that previously used `curl query-api` is replaced with `docker exec trino trino --execute`.

### Verification

- `make smoke-test` passes against a fresh `make up` of the new stack.

## Phase 6 — Delete query-api orphans

Most of the query-api remnants were already deleted during the scrap (`spark/serving/`, `spark/Dockerfile`, `spark/conf/spark-defaults-query.conf`, and the monitor tree — see Context for the full list). What's left to clean up:

- **[spark/analysis/maintenance.py](spark/analysis/maintenance.py)** — orphaned after the scrap; Trino replaces it in phase 4. Verify nothing in the repo imports it before deletion.
- **[spark/analysis/sizing.py](spark/analysis/sizing.py)** — same.
- **[spark/analysis/__init__.py](spark/analysis/__init__.py)** — remove the empty dir after its two modules are gone.
- **[spark/transforms/sql/mart/read_*.sql](spark/transforms/sql/mart/)** — these were consumed only by query-api. Writer SQL (`mart_*.sql`) stays. Grep to confirm no importer before deleting.
- **[grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml)** — the Infinity datasource file still exists from the old stack; replaced by the new Trino datasource in phase 3, deleted here.
- Stop-for-review checkpoint per the risky-action rule before running deletes.

## Phase 7 — Documentation sweep

Order matters — each doc must be internally consistent before moving to the next. **Strictly technical, no portfolio framing.**

1. **[CLAUDE.md](CLAUDE.md)** (reconcile against its uncommitted in-flight edits first) — lines 30, 41, 42, 43, 62, 80, 81, 82. Key files table: drop `spark/serving/query_api.py`, `spark/Dockerfile`, `spark-defaults-query.conf`, `spark/analysis/maintenance.py`, `spark/analysis/sizing.py` rows; add `trino/etc/` and `trino/sql/` rows. Query serving convention → Trino single-node, Iceberg connector via Polaris REST, native Grafana plugin, port 8080. Maintenance convention → Trino `EXECUTE optimize` / `expire_snapshots`, `scripts/maintain.sh`. Memory budget → ~24 GB (14 + 4 + 3 + 1 + 1 + 0.5 + …).
2. **[README.md](README.md)** — lines 21, 31, 39, 49, 52, 66. Architecture diagram node `query-api` → `trino`. Stack table `FastAPI + PySpark` row → `Trino` row. **Remove the "Custom query API" highlight bullet entirely** (no replacement — docs are strictly technical). Dir tree: `serving/query_api.py` → `trino/`.
3. **[docs/TRD.md](docs/TRD.md)** — lines 195, 268, 305, 314, 315, 317, 327, 401–414, 452, 571, 614. FR-23 container count (still 8: `init, rustfs, polaris, postgres, spark-unified, trino, grafana, cloudflared`). Network membership (Trino on both). Mermaid diagrams. Rewrite the "Query Serving" section (401–414) from FastAPI+PySpark to Trino Iceberg connector. Memory table → 4 GB. Health check row. Glossary.
4. **[docs/TDD.md](docs/TDD.md)** — heaviest rewrite. Lines 92, 136, 150, 165, 182, 541–542, 555, 566–569, 596, 748–778, 881, 922, 954, 972, 984, 1024, 1090, 1108, 1120, 1172. Three Mermaid diagrams. Container + network tables. **Rewrite the entire "Query API" section (748–778)** as "Trino": catalog config, REST auth, dashboard wiring, example queries. Dir-tree entries. Architectural rationale paragraphs (1108, 1120) → explain why Trino sits on both networks (data for Polaris/RustFS reads, frontend for Grafana). Glossary.
5. **[docs/BRD.md](docs/BRD.md)** — lines 111, 183, 221, 277, 306, 356. O6 container list; IR-02 (spark-unified ↔ trino via Polaris); C-03 memory budget ~24 GB; Mermaid; Phase 6 description. Glossary. **Remove** the "Query API" line under portfolio talking points (line 288 is about DataSource V2 which stays; line 356 is the glossary entry to delete/replace).
6. **[docs/ROADMAP.md](docs/ROADMAP.md)** — M6 stays as shipped. Add new **M6.5 — Query layer migration**: objective (replace query-api with Trino), deliverables (trino service, catalog config, SQL translation, dashboard rewrite, maintenance tool rewrite, doc sweep), tasks 6.5.1…6.5.N mirroring the phase structure above.
7. **[docs/mart-sizing-analysis.md](docs/mart-sizing-analysis.md)** — numbers replaced per D11. Happens after phase 4 but filed in the doc-sweep phase for cleanliness.
8. **[reference/iceberg-maintenance-procedures.md](reference/iceberg-maintenance-procedures.md)** — lines 6, 32. Replace query-api endpoint references with Trino `EXECUTE` procedures. Cross-link `reference/trino-iceberg-maintenance.md` from the phase-0 spike.
9. **[.claude/agents/dashboard.md](.claude/agents/dashboard.md)** — entire persona is query-api-centric. Rewrite: Grafana + Trino SQL for Iceberg + Trino Iceberg connector + dashboard provisioning. Lines 9, 18, 24–25, 47.
10. **[.claude/agents/infra.md](.claude/agents/infra.md)** — lines 29, 30, 31, 45, 58. Network membership, memory budget, dependency chain: swap query-api → trino.
11. **[.claude/agents/data-pipeline.md](.claude/agents/data-pipeline.md)** — lines 9, 24. Drop `spark/serving/` from owned dirs. PySpark expertise line stays (still describes the streaming layers accurately).

### References kept as-is

- [reference/spark-python-datasource-v2.md](reference/spark-python-datasource-v2.md) and [reference/sentiment-model-research.md](reference/sentiment-model-research.md) — pipeline-side PySpark, not query-api. No changes.

## Phase 8 — Rerun sizing analysis

Per D11. After phase 4 has the tool and phase 3 has the dashboard running against Trino:

- Let the stack run long enough to warm Trino's stats cache and accumulate mart data.
- Run `make sizing-report`.
- Replace numbers in [docs/mart-sizing-analysis.md](docs/mart-sizing-analysis.md) with the Trino measurements. Update the "<5 s on warm query-api" target language to reference Trino.

## Phase 9 — Cloudflared + auth (final, goes-live phase)

Everything up to this point runs locally with Trino reachable only on the Docker network. This phase flips the switch: Trino gains password auth, cloudflared is added to the stack, Grafana becomes the public entry point.

### Files to create

- **`trino/etc/password-authenticator.properties`** — file-based authenticator pointing at `/etc/trino/password.db`.
  - **TODO (fill in at phase 9):** authenticator config keys for the pinned Trino version.
- **`trino/etc/password.db`** — bcrypt-hashed admin password. Generated via `htpasswd -B -C 10 password.db admin`. Password itself sourced from env during build, committed hash only.
- **cloudflared service** in [docker-compose.yml](docker-compose.yml) — per the currently aspirational references in [docs/TRD.md](docs/TRD.md) and [docs/TDD.md](docs/TDD.md). Only Grafana is routed through the tunnel; Trino stays on the internal network.
  - **TODO (fill in at phase 9):** tunnel id, token env var, `config.yml` ingress rules.

### Files to modify

- **`trino/etc/config.properties`** — add `http-server.authentication.type=PASSWORD` and `http-server.process-forwarded=true` (behind cloudflared). Note: Trino's password authenticator requires HTTPS in front of it — cloudflared's TLS termination satisfies this.
- **`grafana/provisioning/datasources/trino.yml`** — add `basicAuth: true`, `basicAuthUser: admin`, `secureJsonData.basicAuthPassword: $TRINO_ADMIN_PASSWORD` from env.
- **[docker-compose.yml](docker-compose.yml)** — add `TRINO_ADMIN_PASSWORD` env passthrough to both `trino` and `grafana`. Add `cloudflared` service, networks, dependency on `grafana`.
- **[scripts/maintain.sh](scripts/maintain.sh)** and sizing tool — pass `--user admin --password` (reading from env) to the Trino CLI invocations.
- **`.env.example`** — document `TRINO_ADMIN_PASSWORD` and any cloudflared tunnel vars.

### Verification

- Local: `make up` still works; Grafana still renders panels; `make maintain` still runs. Auth transparent to the host-local workflow because the password comes from `.env`.
- Public: `curl https://<grafana-cloudflared-hostname>/api/health` returns 200. Trino is NOT directly reachable from the public hostname (only Grafana is).
- **Internal auth model (decided upfront, not at phase 9):** set `internal-communication.shared-secret=<generated>` in `trino/etc/config.properties` before flipping on the password authenticator. On a single-node coordinator+worker this is trivial (one secret, one JVM) and it means the `PASSWORD` authenticator applies uniformly to every HTTP listener without breaking anything. Internal callers from inside the Docker network (maintain.sh, sizing tool, smoke-test) all go through the same `--user admin --password` path as Grafana — no bypass path. The shared-secret env is added to the compose file's trino service.
- Security sanity check: `curl http://trino:8080/v1/info` from inside the Docker network should fail with 401 without creds, succeed with them. Confirm this is the observed behavior before declaring phase 9 done.

## Critical files — at-a-glance reference

| Path | Role |
|---|---|
| [docker-compose.yml](docker-compose.yml) | Add `trino` service; fix `grafana.depends_on` |
| `trino/etc/*` | Coordinator, JVM, node, log, password-authenticator, `atmosphere` catalog |
| `trino/sql/*` | Read-side Trino-dialect SQL |
| [grafana/Dockerfile](grafana/Dockerfile) | Install `grafana-trino-datasource` plugin |
| [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml) | Delete |
| `grafana/provisioning/datasources/trino.yml` | New datasource provisioning |
| [grafana/dashboards/atmosphere.json](grafana/dashboards/atmosphere.json) | ~30 panels: URL → rawSql |
| `scripts/maintain.sh` | New — Trino EXECUTE loop |
| `scripts/sizing-report.sh` | New — metadata reads + query timing via Trino |
| [Makefile](Makefile) | `maintain` and `sizing-report` targets |
| [scripts/smoke-test.sh](scripts/smoke-test.sh) | Layer validation via trino CLI |
| [spark/analysis/](spark/analysis/), [spark/transforms/sql/mart/read_*.sql](spark/transforms/sql/mart/), [grafana/provisioning/datasources/infinity.yml](grafana/provisioning/datasources/infinity.yml) | Delete in phase 6 (the rest of the query-api tree was already removed in the scrap) |
| [CLAUDE.md](CLAUDE.md), [README.md](README.md), [docs/TRD.md](docs/TRD.md), [docs/TDD.md](docs/TDD.md), [docs/BRD.md](docs/BRD.md), [docs/ROADMAP.md](docs/ROADMAP.md), [docs/mart-sizing-analysis.md](docs/mart-sizing-analysis.md), [reference/iceberg-maintenance-procedures.md](reference/iceberg-maintenance-procedures.md), [.claude/agents/dashboard.md](.claude/agents/dashboard.md), [.claude/agents/infra.md](.claude/agents/infra.md), [.claude/agents/data-pipeline.md](.claude/agents/data-pipeline.md) | Documentation sweep |

## End-to-end verification

After all phases:

1. **Cold start:** `make down && make up`. Containers healthy: rustfs, postgres, polaris (+ polaris-bootstrap exited 0), init exited 0, spark-unified, **trino**, grafana. That's 7 long-running + 2 one-shots.
2. **Smoke test:** `make smoke-test` passes against the new stack.
3. **Dashboard:** open [http://localhost:3000](http://localhost:3000), load the Atmosphere dashboard, every panel renders data, time range selector filters correctly, no red triangles.
4. **Maintenance:** `make maintain` runs end-to-end, Iceberg metadata (`$files`, `$snapshots`) shows fewer files and expired snapshots after.
5. **Sizing:** `make sizing-report` produces a report; mart latency numbers are sane (<5 s warm per the doc target).
6. **Trino Web UI:** [http://localhost:8080](http://localhost:8080) shows recent dashboard queries.
7. **Grep audit:** no remaining `query-api`, `query_api`, `fastapi`, `FastAPI`, `Infinity`, or `:8000` references outside of git history.
8. **Doc consistency:** CLAUDE.md memory budget, README architecture diagram, TRD container table, TDD container + network tables, BRD memory table all show the same container list and ~24 GB stack. Note: cloudflared remains in the docs as the final-phase addition; document consistently whether it's "in the stack" (phase 9) or "planned" (pre-phase 9).
9. **Linear:** M6.5 issues created, moved through Backlog → In Progress → In Review → Done (after push) per the Linear workflow rule.
