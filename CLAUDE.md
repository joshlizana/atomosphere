# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Atmosphere is a real-time Bluesky social network analytics platform. It ingests the full firehose (~240 events/sec) via Jetstream WebSocket, transforms it through a three-layer Iceberg medallion (raw → staging → core), runs GPU-accelerated multilingual sentiment analysis, and surfaces live dashboards via Grafana through a ClickHouse view layer that reads Iceberg via the Polaris catalog.

Project 3 in a portfolio trilogy. Work is tracked in Linear (project: Atmosphere) across 8 milestones.

## Architecture

Single unified Spark process (`spark`) running 5 streaming queries in one JVM, reading upstream Iceberg tables with 5-second micro-batch triggers. ClickHouse then reads the resulting Iceberg tables through the Polaris REST catalog and exposes them to Grafana as cached SQL views:

```
Jetstream → ingest → raw_events                                       (1 query)
  → staging → 6 stg_* tables                                          (1 query, foreachBatch → 6)
    → core → 5 core_* tables                                          (2 queries: posts + engagement)
      → sentiment → core_post_sentiment (GPU inference)               (1 query)
                                ↓
              ClickHouse (DataLakeCatalog over Polaris)
                  11 base mart views + 17 panel views
                                ↓
                              Grafana
```

All Spark layers run in one SparkSession (14 GB container, ~22 GB total stack). Infrastructure: SeaweedFS (S3-compatible storage), Polaris (Iceberg REST catalog), Postgres (catalog backing store), ClickHouse 26.1 (query serving), Docker Compose orchestration. There is no longer a materialized mart Iceberg layer — the 10 `mart_*` and 17 `panel_*` views are computed live in ClickHouse with a named filesystem cache (`s3_cache`) attached at the view definition.

## Current State

The ClickHouse migration (CH-01..04) has landed; the materialized Iceberg mart layer is gone and Grafana now serves panels through ClickHouse views. In the current working tree:

- `spark/serving/query_api.py`, `spark/Dockerfile`, and `cloudflared` have been removed.
- **CH-01..03**: `clickhouse` service runs on `clickhouse/clickhouse-server:26.1-alpine` via a thin `docker/clickhouse/Dockerfile` that bakes `config.d/` + `users.d/` (the entrypoint needs to write `default-user.xml` into `users.d/`, which a RO bind mount blocks). Joined to both `atmosphere-data` and `atmosphere-frontend`, 2 GB limit, persistent `clickhouse-data` volume. Two users: `atmosphere_reader` (readonly profile, Grafana login) and `atmosphere_admin` (DDL, init bootstrap only, network-restricted). The named filesystem cache `s3_cache` is configured at the storage-policy level; **every view in `atmosphere.*` must end with `SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1`** because the DataLakeCatalog read path does not propagate the profile-level setting.
- **CH-04**: Grafana plugin `yesoreyeram-infinity-datasource` was replaced with `grafana-clickhouse-datasource` v4.14.0 in `grafana/Dockerfile`. Datasource provisioning lives at `grafana/provisioning/datasources/clickhouse.yml` (UID `clickhouse-atmosphere`, env-var templated reader credentials, `deleteDatasources` block to evict stale `Atmosphere`/`infinity-atmosphere` entries from persistent `grafana-data`). Dashboard `grafana/dashboards/atmosphere.json` was rewritten so every panel target is `SELECT * FROM atmosphere.panel_<name>`.
- **ClickHouse view layer** (`spark/serving/views/`, loaded by `spark/serving/clickhouse_views.py` from `spark/unified.py` after the streaming queries are up): **11 base views** named `mart_*` (`mart_events_per_second`, `mart_engagement_velocity`, `mart_sentiment_timeseries`, `mart_trending_hashtags`, `mart_most_mentioned`, `mart_language_distribution`, `mart_content_breakdown`, `mart_embed_usage`, `mart_firehose_stats`, `mart_top_posts`, `mart_pipeline_health`) reading directly from the Iceberg `polaris_catalog.{raw,staging,core}.*` tables, plus **17 panel views** named `panel_*` that compose on the base views (or read upstream Iceberg directly when no base view fits, e.g. `panel_follows_per_second`, `panel_operations_breakdown`). ClickHouse inlines view bodies — there is no double-execution penalty from layering.
- `infra/init/setup.py` is a **7-step bootstrap**: SeaweedFS wait → bucket → Polaris wait → catalog → namespaces (`raw`, `staging`, `core` — the `mart` namespace is gone) → **ClickHouse reader principal + RBAC chain** (`clickhouse` principal → `clickhouse_reader` principal role → `atmosphere_reader` catalog role with read grants at catalog scope) → **`CREATE DATABASE polaris_catalog`** via the DataLakeCatalog engine on ClickHouse. Polaris generates the principal's client secret server-side on first create; it's persisted to the `polaris-creds` named volume (`/var/polaris-creds/clickhouse.json`) and reused across restarts. The atmosphere catalog has `polaris.config.drop-with-purge.enabled=true` set so leftover Iceberg views can be dropped without filesystem orphans.
- **Object store:** `rustfs` was replaced by `seaweedfs` (`chrislusf/seaweedfs:4.19`) after a deterministic RustFS range-read bug (range reads past the 1 MB EC block boundary returned `Content-Length` correctly but an empty body) crash-looped Spark on every parquet >1 MB. SeaweedFS runs as a thin wrapper image at `docker/seaweedfs/` whose entrypoint envsubsts `s3.json` from `SEAWEEDFS_ADMIN_*` / `SEAWEEDFS_READER_*` env vars before `exec weed server -s3`. Host port 9000 is preserved (`9000:8333`); internal endpoint is `http://seaweedfs:8333`. Memory limit dropped from 3 GB → 1 GB.
- **Remaining CH work:** CH-08 smoke-test rewrite to drive ClickHouse instead of `query-api`, CH-09 docs pass cleanup.
- The Python monitor **Heimdall** (`scripts/heimdall/`) is in progress under CLH-206. The architecture is a **three-layer gated model**: L1 machine health (containers, host saturation, HTTP probes) → L2 operational fidelity (Spark streaming metrics via Dropwizard `/metrics/json`) → L3 data fidelity (DuckDB-embedded reads of Iceberg, deferred). Higher layers short-circuit to `skip` when any lower-layer check breaches. State lives in a DuckDB `:memory:` store backed by a JSONL WAL at `logs/heimdall-wal.jsonl` (crash-recoverable; doubles as the future offline buffer for Iceberg cold-remote sync). Secrets load from repo-root `.env` via `python-dotenv`. L1+L2 are the current ship target (18 checks across `checks/probes.py`, `containers.py`, `saturation.py`, `spark.py`); L3 modules (`data_quality.py`, `iceberg_health.py`) stay on disk tagged `layer="l3"` but are unimported until the DuckDB embedded Iceberg reader lands in a follow-up plan. The old `scripts/monitor/` bash suite, the `SqlEngine` protocol + `engines/` directory, the `pyiceberg`/`clickhouse-connect` deps, and `HEIMDALL_STACK` are all removed. Structured Streaming gauges require `spark.sql.streaming.metricsEnabled=true` in `spark/conf/spark-defaults.conf` (already set) since Spark 4 removed the DStreams-era `/streaming/statistics` REST endpoint Heimdall used to read.

## Key Files

| File | Purpose |
|---|---|
| `spark/unified.py` | Consolidated entrypoint — starts all 5 streaming queries and bootstraps ClickHouse views |
| `spark/sources/jetstream_source.py` | Custom PySpark DataSource V2 — WebSocket with reconnection, failover, cursor rewind |
| `spark/ingestion/ingest_raw.py` | Ingest layer — raw event capture to Iceberg |
| `spark/transforms/staging.py` | Staging layer — collection parsing (6 tables via foreachBatch) |
| `spark/transforms/core.py` | Core layer — enrichment (no longer registers a pipeline_health view; that lives in ClickHouse now) |
| `spark/transforms/sentiment.py` | Sentiment layer — GPU inference via mapInPandas |
| `spark/transforms/sql/` | SQL transforms: `staging/`, `core/` subdirectories |
| `spark/serving/clickhouse_views.py` | Bootstraps the `atmosphere` ClickHouse database and creates 11 base + 17 panel views from `views/*.sql`. Called from `unified.py` after streaming queries start. |
| `spark/serving/views/` | View DDL: 11 `mart_*.sql` base views + 17 `panel_*.sql` per-panel views. Every file ends with the mandatory cache `SETTINGS` clause. |
| `spark/analysis/maintenance.py` | Iceberg table stats + compaction (rewrite_data_files + expire_snapshots) |
| `spark/analysis/sizing.py` | Sizing analysis job (table metadata, mart query latencies) |
| `spark/conf/spark-defaults.conf` | Spark + Iceberg catalog + memory tuning config |
| `spark/Dockerfile.sentiment` | Unified CUDA + Spark + baked XLM-RoBERTa model image (sole Spark image) |
| `grafana/provisioning/` | Grafana datasource + dashboard provisioning configs |
| `infra/init/setup.py` | Idempotent S3 bucket + Polaris catalog bootstrap |
| `docker-compose.yml` | All services, dependency chain, resource limits (~22 GB total) |
| `docs/TRD.md` | Technical Requirements — FR/NFR IDs, acceptance criteria |
| `docs/TDD.md` | Technical Design — architecture, data model |
| `docs/BRD.md` | Business Requirements |
| `docs/mart-sizing-analysis.md` | Pre/post materialization latency + storage baseline |

## Commands

```bash
make up                                # Build and start all services
make down                              # Stop containers (volumes preserved)
make logs                              # Tail all logs
make status                            # Show container status
make clean                             # Full teardown (removes volumes)
make smoke-test                        # Acceptance test suite (waits for data, validates all layers)
make reset-checkpoint LAYER=<layer>    # Wipe a layer's checkpoint state
make replay TIME=<timestamp>           # Replay Jetstream from a cursor
make heimdall                          # Launch Heimdall TUI (primary monitor interface)
make heimdall-run                      # One-shot Heimdall check cycle
make heimdall-watch                    # Headless Heimdall watch loop
```

Heimdall targets run on the host from a local `.venv` (not inside a container).

## Conventions

- **Catalog:** `atmosphere` (Iceberg REST via Polaris). Default catalog in Spark config.
- **Namespaces:** `atmosphere.raw`, `atmosphere.staging`, `atmosphere.core` — the `mart` namespace was dropped along with the materialized mart tables.
- **Temp views:** Use `createOrReplaceGlobalTempView` + `global_temp.*` prefix (required because default catalog is Iceberg, not spark_catalog)
- **Array access:** Use `get(array, index)` not `array[index]` (Spark 4.x throws on empty arrays)
- **SQL transforms:** Parameterized with `{source}` placeholder, formatted at runtime
- **Partitioning:** `days(event_time)` for staging/core tables; `raw_events` is `days(ingested_at), collection`.
- **Table creation:** `CREATE TABLE IF NOT EXISTS` — idempotent on every startup
- **Streaming trigger:** `processingTime="5 seconds"` on all streaming queries
- **Unified process:** All 4 streaming layers (ingest, staging, core, sentiment) run in one `spark` container. Each module exposes `start_queries(spark)` for the unified entrypoint and `main()` for standalone debugging. After streaming queries start, `spark/serving/clickhouse_views.py` bootstraps the ClickHouse view layer.
- **ClickHouse views must always cache:** Every view under `atmosphere.*` ends with `SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1`. The DataLakeCatalog read path does not pick up profile-level cache settings, so the SETTINGS clause must be attached at the view (or query) level. Empirically verified: without it the cache stays empty and reads scan SeaweedFS on every refresh.
- **Query serving:** ClickHouse 26.1 with `DataLakeCatalog` engine pointing at Polaris. Two view tiers in `atmosphere.*`: 11 `mart_*` base views over `polaris_catalog.{raw,staging,core}.*`, and 17 `panel_*` views (one per Grafana panel) that compose on the base views. Grafana panels query `SELECT * FROM atmosphere.panel_<name>` only — all SQL lives in `spark/serving/views/`.
- **Maintenance:** `spark/analysis/maintenance.py` contains `rewrite_data_files` + `expire_snapshots` logic with threshold `file_count > 500 OR avg_file_kb < 30`, per-table 10-minute rate limit, and 1-day snapshot retention. Currently invoked ad-hoc inside the `spark` container; targets the 12 raw/staging/core tables.
- **Memory budget:** ~22 GB total stack (14 GB spark, 1 GB SeaweedFS, 2 GB ClickHouse, 1 GB each for Postgres/Polaris, 512 MB each for init/Grafana). Designed for 32 GB workstations. Fits inside the 24 GB Docker budget.

## Linear Workflow

Issues follow: Backlog → In Progress → In Review → Done (after push).
