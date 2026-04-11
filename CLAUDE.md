# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Atmosphere is a real-time Bluesky social network analytics platform. It ingests the full firehose (~240 events/sec) via Jetstream WebSocket, transforms it through a four-layer medallion architecture (raw → staging → core → mart), runs GPU-accelerated multilingual sentiment analysis, and surfaces live dashboards via Grafana.

Project 3 in a portfolio trilogy. Work is tracked in Linear (project: Atmosphere) across 8 milestones.

## Architecture

Single unified Spark process (`spark-unified`) running 5 streaming queries in one JVM, reading upstream Iceberg tables with 5-second micro-batch triggers:

```
Jetstream → ingest → raw_events
  → staging → 6 stg_* tables
    → core → 4 core_* tables + 9 mart views (2 queries: posts + engagement)
      → sentiment → core_post_sentiment (GPU inference)
```

All layers run in one SparkSession (14 GB container, ~22 GB total stack). Infrastructure: RustFS (S3-compatible storage), Polaris (Iceberg REST catalog), Postgres (catalog backing store), Docker Compose orchestration.

## Key Files

| File | Purpose |
|---|---|
| `spark/unified.py` | Consolidated entrypoint — starts all 5 streaming queries in one process |
| `spark/sources/jetstream_source.py` | Custom PySpark DataSource V2 — WebSocket with reconnection, failover, cursor rewind |
| `spark/ingestion/ingest_raw.py` | Ingest layer — raw event capture to Iceberg |
| `spark/transforms/staging.py` | Staging layer — collection parsing (6 tables via foreachBatch) |
| `spark/transforms/core.py` | Core layer — enrichment + mart view registration |
| `spark/transforms/sentiment.py` | Sentiment layer — GPU inference via mapInPandas |
| `spark/transforms/sql/` | SQL transforms: `staging/`, `core/`, `mart/` subdirectories |
| `spark/conf/spark-defaults.conf` | Spark + Iceberg catalog + memory tuning config |
| `spark/Dockerfile.sentiment` | Unified CUDA + Spark + baked XLM-RoBERTa model image |
| `spark/Dockerfile` | Base Spark image (spark-thrift only) |
| `infra/init/setup.py` | Idempotent S3 bucket + Polaris catalog bootstrap |
| `docker-compose.yml` | All services, dependency chain, resource limits (~22 GB total) |
| `docs/TRD.md` | Technical Requirements — FR/NFR IDs, acceptance criteria |
| `docs/TDD.md` | Technical Design — architecture, data model |
| `docs/BRD.md` | Business Requirements |

## Commands

```bash
make up        # Build and start all services
make down      # Stop and remove containers + volumes
make logs      # Tail all logs
make status    # Show container status
make clean     # Full teardown
make monitor   # Single health check
make maintain  # Iceberg table maintenance (snapshot expiry + compaction)
make smoke-test # Run acceptance test suite (waits for data, then validates all layers)
```

## Conventions

- **Catalog:** `atmosphere` (Iceberg REST via Polaris). Default catalog in Spark config.
- **Namespaces:** `atmosphere.raw`, `atmosphere.staging`, `atmosphere.core`, `atmosphere.mart`
- **Temp views:** Use `createOrReplaceGlobalTempView` + `global_temp.*` prefix (required because default catalog is Iceberg, not spark_catalog)
- **Array access:** Use `get(array, index)` not `array[index]` (Spark 4.x throws on empty arrays)
- **SQL transforms:** Parameterized with `{source}` placeholder, formatted at runtime
- **Partitioning:** All tables use `days(event_time)` except `raw_events` which uses `days(ingested_at), collection`
- **Table creation:** `CREATE TABLE IF NOT EXISTS` (FR-25) — idempotent on every startup
- **Streaming trigger:** `processingTime="5 seconds"` on all streaming queries (FR-10)
- **Unified process:** All 4 streaming layers run in one `spark-unified` container. Each module exposes `start_queries(spark)` for the unified entrypoint, and `main()` for standalone debugging.
- **Memory budget:** ~22 GB total stack (14 GB spark-unified, 3 GB RustFS, 1 GB each for Postgres/Polaris). Designed for 32 GB workstations.

## Linear Workflow

Issues follow: Backlog → In Progress → In Review → Done (after push).
