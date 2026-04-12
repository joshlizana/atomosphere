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

All layers run in one SparkSession (14 GB container, ~22 GB total stack). Infrastructure: RustFS (S3-compatible storage), Polaris (Iceberg REST catalog), Postgres (catalog backing store), Docker Compose orchestration.

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
| `spark/transforms/sql/` | SQL transforms: `staging/`, `core/`, `mart/` subdirectories |
| `spark/analysis/maintenance.py` | Iceberg table stats + compaction (rewrite_data_files + expire_snapshots) |
| `spark/analysis/sizing.py` | Sizing analysis job (table metadata, mart query latencies) |
| `spark/conf/spark-defaults.conf` | Spark + Iceberg catalog + memory tuning config |
| `spark/Dockerfile.sentiment` | Unified CUDA + Spark + baked XLM-RoBERTa model image |
| `spark/Dockerfile` | Base Spark image (query-api) |
| `spark/serving/query_api.py` | REST API serving mart queries + analysis/maintenance jobs to Grafana |
| `spark/conf/spark-defaults-query.conf` | Spark config for query-api (lighter weight, no streaming) |
| `grafana/provisioning/` | Grafana datasource + dashboard provisioning configs |
| `infra/init/setup.py` | Idempotent S3 bucket + Polaris catalog bootstrap |
| `docker-compose.yml` | All services, dependency chain, resource limits (~22 GB total) |
| `docs/TRD.md` | Technical Requirements — FR/NFR IDs, acceptance criteria |
| `docs/TDD.md` | Technical Design — architecture, data model |
| `docs/BRD.md` | Business Requirements |
| `docs/mart-sizing-analysis.md` | Pre/post materialization latency + storage baseline |
| `reference/iceberg-maintenance-procedures.md` | Iceberg 1.10.1 rewrite_data_files + expire_snapshots reference |

## Commands

```bash
make up        # Build and start all services
make down      # Stop and remove containers + volumes
make logs      # Tail all logs
make status    # Show container status
make clean     # Full teardown
make monitor   # Single health check
make maintain  # Trigger Iceberg compaction via query-api /api/maintenance/run (polls to completion)
make sizing-report  # Run sizing analysis (table stats + mart latencies) → reports/
make smoke-test # Run acceptance test suite (waits for data, then validates all layers)
```

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
- **Query serving:** `query-api` container runs FastAPI + PySpark, reads Iceberg tables directly via Polaris, serves JSON to Grafana's Infinity datasource plugin. Replaces the originally-planned Spark Thrift Server (no Hive plugin exists for Grafana).
- **Maintenance:** `/api/maintenance/table-stats` reports file_count / avg_kb / needs_compaction per table. `/api/maintenance/run` triggers compaction (rewrite_data_files + expire_snapshots) inside query-api's SparkSession. Threshold: `file_count > 500 OR avg_file_kb < 30`. Per-table 10-minute rate limit. Snapshot retention: 1 day. `min-input-files=2` override so small mart tables still get compacted.
- **Memory budget:** ~22 GB total stack (14 GB spark-unified, 3 GB RustFS, 2 GB query-api, 1 GB each for Postgres/Polaris, 512 MB Grafana). Designed for 32 GB workstations.

## Linear Workflow

Issues follow: Backlog → In Progress → In Review → Done (after push).
