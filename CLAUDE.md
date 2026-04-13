# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Atmosphere is a real-time Bluesky social network analytics platform. It ingests the full firehose (~240 events/sec) via Jetstream WebSocket, transforms it through a four-layer medallion architecture (raw → staging → core → mart), runs GPU-accelerated multilingual sentiment analysis, and surfaces live dashboards via Grafana.

Project 3 in a portfolio trilogy. Work is tracked in Linear (project: Atmosphere) across 8 milestones.

## Architecture

Single unified Spark process (`spark-unified`) running 16 streaming queries in one JVM, reading upstream Iceberg tables with 5-second micro-batch triggers:

```
Jetstream → ingest → raw_events                                       (1 query)
  → staging → 6 stg_* tables                                          (1 query, foreachBatch → 6)
    → core → 5 core_* tables + pipeline_health view                   (2 queries: posts + engagement)
      → sentiment → core_post_sentiment (GPU inference)               (1 query)
        → marts → 10 materialized mart_* tables                       (10 queries)
```

All layers run in one SparkSession (14 GB container, ~20 GB total stack). Infrastructure: RustFS (S3-compatible storage), Polaris (Iceberg REST catalog), Postgres (catalog backing store), Docker Compose orchestration.

## Current State

The project is **mid-migration from `query-api` to ClickHouse** for Grafana query serving. In the current working tree:

- `spark/serving/query_api.py`, `spark/Dockerfile`, and `cloudflared` have been removed.
- **CH-01..03 landed** (CLH-194/195/196): `clickhouse` service runs on `clickhouse/clickhouse-server:26.1-alpine` via a thin `docker/clickhouse/Dockerfile` that bakes `config.d/` + `users.d/` (the entrypoint needs to write `default-user.xml` into `users.d/`, which a RO bind mount blocks). Joined to both `atmosphere-data` and `atmosphere-frontend`, 2 GB limit, persistent `clickhouse-data` volume. Two users: `atmosphere_reader` (readonly profile, Grafana login) and `atmosphere_admin` (DDL, init bootstrap only, network-restricted).
- `infra/init/setup.py` is now a **7-step bootstrap**: RustFS wait → bucket → Polaris wait → catalog → namespaces → **ClickHouse reader principal + RBAC chain** (`clickhouse` principal → `clickhouse_reader` principal role → `atmosphere_reader` catalog role with 6 read grants at catalog scope) → **`CREATE DATABASE polaris_catalog`** via the DataLakeCatalog engine on ClickHouse. Polaris generates the principal's client secret server-side on first create; it's persisted to the new `polaris-creds` named volume (`/var/polaris-creds/clickhouse.json`) and reused across restarts.
- **Remaining CH work:** CH-04 Grafana datasource plugin swap + datasource provisioning, CH-05..07 dashboard panel rewrites, CH-08 smoke-test rewrite, CH-09 docs pass. Known follow-up queued for CH-04: first mart read under `vended_credentials=true` produced a doubled S3 key path (`mart/mart_events_per_second/warehouse/mart/mart_events_per_second/...`); fix is to flip to `vended_credentials=false` since RustFS accepts static root creds.
- The Python monitor **Heimdall** (`scripts/heimdall/`) is in progress under CLH-206: package skeleton (`registry`, `runner`, `state`, `alerter`, `result`, `config`, `engines/`, `checks/`) is in place; individual check modules are being authored. The old `scripts/monitor/` bash suite has been deleted.

Active planning lives in `reference/clickhouse-migration-plan.md` and `reference/heimdall-plan.md`.

## Key Files

| File | Purpose |
|---|---|
| `spark/unified.py` | Consolidated entrypoint — starts all 16 streaming queries in one process |
| `spark/sources/jetstream_source.py` | Custom PySpark DataSource V2 — WebSocket with reconnection, failover, cursor rewind |
| `spark/ingestion/ingest_raw.py` | Ingest layer — raw event capture to Iceberg |
| `spark/transforms/staging.py` | Staging layer — collection parsing (6 tables via foreachBatch) |
| `spark/transforms/core.py` | Core layer — enrichment + pipeline_health view registration |
| `spark/transforms/sentiment.py` | Sentiment layer — GPU inference via mapInPandas |
| `spark/transforms/marts.py` | Mart layer — 10 streaming materialized marts (1-min tumbling windows, append-only) |
| `spark/transforms/sql/` | SQL transforms: `staging/`, `core/`, `mart/` subdirectories (mart/ holds `mart_pipeline_health.sql` plus 10 `read_*.sql` query templates) |
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
| `reference/clickhouse-migration-plan.md` | In-flight plan to replace query-api with ClickHouse `DataLakeCatalog` |
| `reference/heimdall-plan.md` | In-flight plan for the Python monitor (Heimdall) |
| `reference/iceberg-maintenance-procedures.md` | Iceberg 1.10.1 rewrite_data_files + expire_snapshots reference |

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
- **Namespaces:** `atmosphere.raw`, `atmosphere.staging`, `atmosphere.core`, `atmosphere.mart`
- **Temp views:** Use `createOrReplaceGlobalTempView` + `global_temp.*` prefix (required because default catalog is Iceberg, not spark_catalog)
- **Array access:** Use `get(array, index)` not `array[index]` (Spark 4.x throws on empty arrays)
- **SQL transforms:** Parameterized with `{source}` placeholder (and `{window}` for `trending_hashtags`), formatted at runtime
- **Partitioning:** All tables use `days(event_time)` except `raw_events` (`days(ingested_at), collection`) and most mart tables (`hours(bucket_min)` — one day per hour for time-series marts, `days(bucket_min)` for `language_distribution`, unpartitioned for the small key-lookup marts)
- **Table creation:** `CREATE TABLE IF NOT EXISTS` (FR-25) — idempotent on every startup
- **Mart nullability:** Mart DDL columns are declared without `NOT NULL` because `window.start` and grouping columns read from upstream Iceberg tables are `optional` at the Spark schema level; Iceberg's schema-compatibility check rejects writing optional → required even when rows are never null in practice. Keep columns nullable in DDL; the streaming SQL is the source of truth for non-null semantics.
- **Streaming trigger:** `processingTime="5 seconds"` on all streaming queries (FR-10)
- **Unified process:** All 5 streaming layers run in one `spark-unified` container. Each module exposes `start_queries(spark)` for the unified entrypoint, and `main()` for standalone debugging.
- **Materialized marts:** 10 streaming mart tables written append-only under `atmosphere.mart.*` via 1-minute tumbling windows + 15-minute event-time watermark. No top-N at write time — reads sort and limit. `pipeline_health` is the exception: it stays as a query-time Iceberg view (metadata, not data).
- **Query serving:** Mid-migration. The original `query-api` (FastAPI + PySpark) has been removed and replaced by a `clickhouse` service (build context `docker/clickhouse/`) joined to both `atmosphere-data` and `atmosphere-frontend`. ClickHouse's `DataLakeCatalog` engine reads Iceberg via Polaris; Grafana will consume it through the native `grafana-clickhouse-datasource` plugin. Datasource wiring and dashboard SQL compilation are the outstanding work (see `reference/clickhouse-migration-plan.md`).
- **Maintenance:** `spark/analysis/maintenance.py` contains `rewrite_data_files` + `expire_snapshots` logic with threshold `file_count > 500 OR avg_file_kb < 30`, per-table 10-minute rate limit, 1-day snapshot retention, and `min-input-files=2` override so small mart tables still get compacted. Currently invoked ad-hoc inside the `spark-unified` container; a `make` target will return once the ClickHouse migration lands.
- **Memory budget:** ~22 GB total stack (14 GB spark-unified, 3 GB RustFS, 2 GB ClickHouse, 1 GB each for Postgres/Polaris, 512 MB each for init/Grafana). Designed for 32 GB workstations. Fits inside the 24 GB Docker budget (NFR-09).

## Linear Workflow

Issues follow: Backlog → In Progress → In Review → Done (after push).
