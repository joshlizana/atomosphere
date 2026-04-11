# Atmosphere

Real-time Bluesky social network analytics with multilingual sentiment analysis.

Ingests the full [Bluesky](https://bsky.app) firehose (~240 events/sec), transforms it through a four-layer medallion architecture, runs GPU-accelerated sentiment analysis, and surfaces live analytics through Grafana.

**"A lot with a little."** Spark handles ingestion, streaming, transformation, ML inference, and query serving. Iceberg stores it. Grafana displays it.

## Architecture

```mermaid
flowchart LR
    JS[Jetstream WebSocket] --> SI[spark-ingest]
    SI --> RAW[Raw]
    RAW --> SS[spark-staging]
    SS --> STG[Staging]
    STG --> SC[spark-core]
    SC --> CORE[Core / Mart]
    CORE --> SENT[spark-sentiment]
    SENT --> SOUT[Sentiment]

    style JS fill:#0085ff,color:#fff
    style SENT fill:#76b900,color:#fff
```

Five Spark containers chained via Structured Streaming with 5-second micro-batch triggers, each reading upstream Iceberg tables.

## Tech Stack

| Technology | Role |
|---|---|
| Apache Spark 4.x | Unified engine (ingest, stream, transform, ML, serve) |
| Apache Iceberg | Table format (ACID, time travel) |
| Grafana | Live dashboards |
| RustFS | S3-compatible object storage |
| Apache Polaris | Iceberg REST catalog |
| XLM-RoBERTa | Multilingual sentiment model (100+ languages) |
| Docker Compose | Orchestration |
| Cloudflare Tunnel | Public access |

## Key Features

- **Custom DataSource V2** -- PySpark WebSocket source with reconnection, failover, and cursor rewind
- **Medallion architecture** -- Raw (JSON) -> Staging (6 typed tables) -> Core (enriched + extracted) -> Mart (aggregated analytics)
- **GPU sentiment analysis** -- XLM-RoBERTa baked into a CUDA container, batch inference via `mapInPandas`

## Project Structure

```
spark/
  sources/jetstream_source.py    # Custom DataSource V2
  ingestion/ingest_raw.py        # Raw event capture
  transforms/
    staging.py                   # Collection parsing
    core.py                      # Enrichment + mart creation
    sentiment.py                 # GPU inference
    sql/                         # 6 staging + 4 core + 9 mart SQL transforms
infra/init/                      # S3 bucket + catalog bootstrap
docs/                            # TDD, TRD, BRD, Roadmap
```

## Quickstart

```bash
git clone https://github.com/joshlizana/atmosphere.git
cd atmosphere
cp .env.example .env
make up
```

The first build takes ~10 minutes (downloads Spark, Iceberg JARs, PyTorch, and the 1.1 GB sentiment model). Subsequent starts are instant.

Once running, open Grafana at [http://localhost:3000](http://localhost:3000) to see live analytics. Data begins flowing within 30 seconds of startup.

### Prerequisites

| Requirement | Notes |
|---|---|
| Docker with Compose V2 | |
| NVIDIA GPU + drivers + [nvidia-container-toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html) | For GPU sentiment inference; falls back to CPU without it |
| ~60 GB RAM | For all containers |

### Commands

| Command | Description |
|---|---|
| `make up` | Build and start all services |
| `make down` | Stop and remove containers + volumes |
| `make logs` | Tail all logs |
| `make status` | Show container status |
| `make clean` | Full teardown (containers + volumes) |

## Progress

| Milestone | Status |
|---|---|
| M1: Foundation | Done |
| M2: Ingestion | Done |
| M3: Staging | Done |
| M4: Core + Mart | Done |
| M5: Sentiment | In progress |
| M6: Dashboard | Not started |
| M7: Public Access | Not started |
| M8: Hardening | Not started |

## Documentation

- [Technical Design Document](docs/TDD.md)
- [Technical Requirements Document](docs/TRD.md)
- [Business Requirements Document](docs/BRD.md)
- [Roadmap](docs/ROADMAP.md)
