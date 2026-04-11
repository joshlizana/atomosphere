# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Atmosphere is a real-time Bluesky social network analytics platform. It ingests the full firehose (~240 events/sec) via Jetstream WebSocket, transforms it through a four-layer medallion architecture (raw → staging → core → mart), runs GPU-accelerated multilingual sentiment analysis, and surfaces live dashboards via Grafana.

Project 3 in a portfolio trilogy. Work is tracked in Linear (project: Atmosphere) across 8 milestones.

## Architecture

Five chained Spark Structured Streaming applications, each reading upstream Iceberg tables with 5-second micro-batch triggers:

```
Jetstream → spark-ingest → raw_events
  → spark-staging → 6 stg_* tables
    → spark-core → 4 core_* tables + 9 mart views
      → spark-sentiment → core_post_sentiment (GPU inference)
```

Infrastructure: RustFS (S3-compatible storage), Polaris (Iceberg REST catalog), Postgres (catalog backing store), Docker Compose orchestration.

## Key Files

| File | Purpose |
|---|---|
| `spark/sources/jetstream_source.py` | Custom PySpark DataSource V2 — WebSocket with reconnection, failover, cursor rewind |
| `spark/ingestion/ingest_raw.py` | Raw event capture to Iceberg |
| `spark/transforms/staging.py` | Collection parsing (6 tables via foreachBatch) |
| `spark/transforms/core.py` | Enrichment + mart view registration |
| `spark/transforms/sentiment.py` | GPU sentiment inference via mapInPandas |
| `spark/transforms/sql/` | SQL transforms: `staging/`, `core/`, `mart/` subdirectories |
| `spark/conf/spark-defaults.conf` | Spark + Iceberg catalog config |
| `spark/Dockerfile` | Base Spark image (ingest, staging, core) |
| `spark/Dockerfile.sentiment` | CUDA + Spark + baked XLM-RoBERTa model |
| `infra/init/setup.py` | Idempotent S3 bucket + Polaris catalog bootstrap |
| `docker-compose.yml` | All services, dependency chain, resource limits |
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
- **Docker dependencies:** `service_started` (not `service_healthy`) between Spark jobs for faster startup

## Linear Workflow

Issues follow: Backlog → In Progress → In Review → Done (after push).
