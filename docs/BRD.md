# Business Requirements Document
## Atmosphere

| | |
|---|---|
| **Author** | Joshua |
| **Status** | Draft |
| **Created** | 2026-04-11 |
| **Last updated** | 2026-04-11 |

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Business Case](#2-business-case)
3. [Stakeholder Analysis](#3-stakeholder-analysis)
4. [Objectives](#4-objectives)
5. [Scope](#5-scope)
6. [Requirements](#6-requirements)
7. [Assumptions and Dependencies](#7-assumptions-and-dependencies)
8. [Constraints](#8-constraints)
9. [Success Metrics](#9-success-metrics)
10. [Solution Approach](#10-solution-approach)
11. [Timeline](#11-timeline)
12. [Risk Assessment](#12-risk-assessment)
13. [Glossary](#13-glossary)

---

## 1. Executive Summary

Atmosphere is a real-time streaming analytics platform that ingests the full Bluesky social network firehose, enriches posts with multilingual sentiment analysis, and delivers live dashboards accessible via a public URL.

The platform processes approximately 240 events per second — posts, likes, reposts, follows, and blocks — through a three-layer Iceberg medallion (raw → staging → core). A GPU-accelerated transformer model scores every post for sentiment across 100+ languages. ClickHouse, reading the Iceberg tables through the Polaris REST catalog, exposes 11 base mart views and 17 per-panel views to Grafana, which renders five continuously updating dashboard sections covering sentiment trends, firehose throughput, language distribution, engagement velocity, trending hashtags, and pipeline health.

The distinguishing characteristic of Atmosphere is architectural consolidation. Apache Spark handles ingestion, stream processing, transformation, and ML inference. Apache Iceberg is the storage format. ClickHouse serves Grafana through cached SQL views. Four technologies address a dozen engineering concerns.

For comprehensive technical details — data model, container architecture, custom data source design, and ML pipeline — refer to the [Technical Design Document](TDD.md).

---

## 2. Business Case

### 2.1 Problem Statement

Social networks generate high-volume, multilingual event streams that require purpose-built infrastructure to capture, process, and analyze in real time. Building such a system typically demands expertise across many specialized tools — message brokers, stream processors, orchestrators, transformation frameworks, ML serving layers, and dashboard platforms.

Atmosphere addresses the question: **how much analytical capability can a single platform deliver?**

### 2.2 Strategic Alignment

Atmosphere demonstrates three capabilities valued in data engineering roles:

| Capability | How Atmosphere Demonstrates It |
|---|---|
| **Streaming architecture design** | End-to-end ownership of a real-time pipeline from WebSocket ingestion through dashboard delivery, with sub-10-second latency across five chained Spark Structured Streaming applications |
| **Platform depth** | Mastery of Spark across six distinct roles — ingestion, streaming, SQL transformation, scheduling, data quality, and ML inference — paired with ClickHouse as a purpose-fit Iceberg query engine for low-latency dashboard serving |
| **Applied ML in data engineering** | GPU-accelerated multilingual sentiment analysis (XLM-RoBERTa, 100+ languages) integrated into a streaming pipeline via `mapInPandas`, bridging data engineering and ML engineering |

### 2.3 Expected Outcomes

| Outcome | Measure |
|---|---|
| Live public dashboard | A URL accessible to anyone showing real-time Bluesky analytics with 5-second refresh |
| End-to-end streaming pipeline | Data flows from Jetstream WebSocket to Grafana dashboard within 10 seconds |
| Multilingual sentiment analysis | Every post scored for sentiment — English, Japanese, Korean, Spanish, German, and 95+ additional languages |
| Reproducible environment | Any engineer can clone the repository and run `make up` to start the full 8-container stack (seaweedfs, postgres, polaris, clickhouse, init, spark, grafana, plus the polaris-bootstrap one-shot) |

---

## 3. Stakeholders

| Stakeholder | Interest | Role |
|---|---|---|
| **Operators** | Run, monitor, and extend the platform; reason about trade-offs and failure modes via the BRD and TDD | Primary user |
| **Peer engineers** | Understand design trade-offs and implementation patterns — custom DataSource V2, GPU inference integration, chained streaming queries | Collaborator |
| **Dashboard visitors** | Experience a live, interactive dashboard showing real-time social network activity | End user |

### 3.1 What Stakeholders Evaluate

**Operators and peer engineers** look for:
- A working artifact: `git clone` followed by `make up` produces a running system
- Spark depth — a custom DataSource V2 streaming source implemented entirely in Python, with offset tracking, checkpoint integration, and micro-batch boundary management
- Architectural reasoning — documented rationale for each design decision (TDD §15)
- Clean, well-organized code with SQL transformations in standalone files
- Interesting technical patterns — custom Spark sources, GPU-accelerated `mapInPandas`, chained streaming queries across Iceberg tables
- Honest documentation of trade-offs and scope boundaries

---

## 4. Objectives

Each objective follows SMART criteria (specific, measurable, achievable, relevant, time-bound).

| # | Objective | Measure of Success |
|---|---|---|
| O1 | Ingest the full Bluesky Jetstream firehose in real time | Sustained ingestion of ~240 events/sec across all collections with gapless cursor-based recovery on disconnect |
| O2 | Process events through a three-layer Iceberg medallion | `atmosphere.raw`, `atmosphere.staging`, and `atmosphere.core` namespaces populated continuously with 5-second micro-batch cadence |
| O3 | Score every post for multilingual sentiment | `core_post_sentiment` table contains a sentiment label (`positive`, `negative`, `neutral`) and confidence score for every row in `core_posts` |
| O4 | Deliver live analytics through a Grafana dashboard | Five dashboard sections — sentiment, firehose activity, language/content, engagement velocity, pipeline health — refreshing every 5 seconds against ClickHouse views over Iceberg |
| O5 | Expose the dashboard via a public URL | **Deferred** post-ClickHouse migration. Originally targeted Cloudflare Tunnel; will be reinstated once the new serving layer stabilizes |
| O6 | Provide a single-command reproducible environment | `make up` starts the full 8-container stack (seaweedfs, postgres, polaris-bootstrap, polaris, clickhouse, init, spark, grafana) from a clean clone |

---

## 5. Scope

### 5.1 In Scope

| Area | Detail |
|---|---|
| Data ingestion | All Bluesky Jetstream event types: posts, likes, reposts, follows, blocks, profile updates (~240 events/sec, ~20.7M/day) |
| Custom data source | PySpark DataSource V2 implementation wrapping the Jetstream WebSocket with offset tracking and checkpoint integration |
| Stream processing | Five Structured Streaming queries running in a single unified Spark process across the three Iceberg medallion layers + sentiment |
| Sentiment analysis | GPU-accelerated XLM-RoBERTa inference on all posts via `mapInPandas`, with automatic CPU adaptation |
| Data storage | 12 Apache Iceberg tables (1 raw + 6 staging + 5 core) on SeaweedFS with Polaris REST catalog |
| Query serving | ClickHouse 26.1 with `DataLakeCatalog` engine over Polaris; 11 base mart views + 17 panel views in `atmosphere.*`, all cached against the named filesystem cache `s3_cache` |
| Dashboard | Five-section Grafana dashboard (19 panels) provisioned as code via the native `grafana-clickhouse-datasource` plugin |
| Public access | **Deferred** — Cloudflare Tunnel will be reinstated post-migration |
| Infrastructure | Docker Compose orchestration within a ~22 GB memory budget on a 32 GB workstation |
| Documentation | BRD (this document), TDD, README |

### 5.2 Scope Boundaries

| Area | Boundary |
|---|---|
| Deployment | Single-node local deployment via Docker Compose on a 32 GB workstation |
| Analytics | Aggregate network-level analytics — firehose throughput, sentiment distribution, engagement rates, trending hashtags |
| Data | Publicly available Bluesky data via the Jetstream WebSocket |
| History | Live events from the current stream forward; 30-day retention window with depth growing organically |
| Schema enforcement | Inline Spark transformations at the staging layer |
| Testing | Informal validation and pipeline self-monitoring during initial development; formal test suites in a later phase |

---

## 6. Requirements

### 6.1 Functional Requirements

| ID | Priority | Requirement |
|---|---|---|
| FR-01 | Critical | The system ingests all event types from the Bluesky Jetstream WebSocket (~240 events/sec) via a custom PySpark DataSource V2 source |
| FR-02 | Critical | Raw events are preserved verbatim as JSON in `atmosphere.raw.raw_events`, partitioned by `days(ingested_at)` and `collection` |
| FR-03 | Critical | Events are parsed by collection type into six typed staging tables: `stg_posts`, `stg_likes`, `stg_reposts`, `stg_follows`, `stg_blocks`, `stg_profiles` |
| FR-04 | Critical | Posts are enriched in the core layer with extracted hashtags, mention DIDs, link URLs, primary language, and content type classification |
| FR-05 | Critical | Every post receives a three-class sentiment score (`positive`, `negative`, `neutral`) from the XLM-RoBERTa model via GPU-accelerated `mapInPandas` |
| FR-06 | Critical | Eleven ClickHouse base mart views (`mart_events_per_second`, `mart_engagement_velocity`, `mart_sentiment_timeseries`, `mart_trending_hashtags`, `mart_most_mentioned`, `mart_language_distribution`, `mart_content_breakdown`, `mart_embed_usage`, `mart_firehose_stats`, `mart_top_posts`, `mart_pipeline_health`) read live from the Iceberg core/staging/raw layers and serve dashboard queries within 5 seconds |
| FR-07 | Critical | The Grafana dashboard displays five analytical rows with sub-5-second query latency via the ClickHouse view layer |
| FR-08 | Critical | The pipeline recovers automatically from WebSocket disconnects using exponential backoff and cursor-based replay |
| FR-09 | High | Trending hashtags are identified by comparing current frequency against a historical baseline, computed live in `atmosphere.panel_trending_hashtags` |
| FR-10 | High | The dashboard is publicly accessible via Cloudflare Tunnel at a custom domain **(Deferred until post-ClickHouse migration)** |
| FR-11 | High | The full stack starts from `make up` with idempotent initialization (init container creates SeaweedFS buckets, the Polaris catalog + namespaces, and the ClickHouse reader principal + `polaris_catalog` database) |
| FR-12 | Medium | Dashboard panels display the top 5 most positive and most negative recent posts with full text via `atmosphere.panel_top_posts_positive` and `atmosphere.panel_top_posts_negative` |
| FR-13 | Medium | The pipeline health row shows processing lag, events ingested per second, and last event timestamps via `atmosphere.panel_pipeline_health` |
| FR-14 | Medium | Seventeen `panel_*` ClickHouse views (one per Grafana panel) compose on the base mart views and are queried as `SELECT * FROM atmosphere.panel_<name>`. Every view ends with `SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1` |

### 6.2 Non-Functional Requirements

| ID | Category | Requirement |
|---|---|---|
| NFR-01 | Latency | End-to-end latency from Jetstream event to dashboard display is under 10 seconds |
| NFR-02 | Throughput | The pipeline sustains 240+ events/sec ingestion; the sentiment model processes ~26 posts/sec at batch_size=64 on GPU with substantial headroom |
| NFR-03 | Resilience | All long-running containers restart automatically (`restart: unless-stopped`); WebSocket disconnects recover with gapless cursor replay within Jetstream's 24-hour retention |
| NFR-04 | Resource efficiency | The unified stack operates within ~22 GB on a 32 GB workstation, leaving ~8 GB for the host and ~1.7 GB headroom |
| NFR-05 | Reproducibility | The environment is fully defined in version-controlled `docker-compose.yml`, `Makefile`, and `.env.example` |
| NFR-06 | Observability | Pipeline health is self-monitored via `mart_pipeline_health` and a dedicated Grafana dashboard row |
| NFR-07 | Portability | Sentiment inference adapts automatically between GPU (`device=0`) and CPU (`device=-1`) at container startup via `torch.cuda.is_available()` |
| NFR-08 | Graceful degradation | Grafana displays last known data when any upstream container falls behind; dashboard panels show stale timestamps rather than empty panels |

### 6.3 Integration Requirements

| ID | Requirement |
|---|---|
| IR-01 | spark connects to the Jetstream WebSocket via a custom Python DataSource V2 implementation (`JetstreamDataSource` + `JetstreamStreamReader`) |
| IR-02 | spark and clickhouse share table metadata through the Polaris REST catalog at port 8181; ClickHouse uses a dedicated reader principal provisioned by the init container and persisted in the `polaris-creds` named volume |
| IR-03 | Grafana connects to ClickHouse via the native `grafana-clickhouse-datasource` plugin (HTTP, port 8123) using the `atmosphere_reader` user |
| IR-04 | The Cloudflare Tunnel integration is deferred until after the ClickHouse migration stabilizes |
| IR-05 | spark accesses the host NVIDIA GPU via `nvidia-container-toolkit` with `deploy.resources.reservations.devices: [capabilities: [gpu]]` |

---

## 7. Assumptions and Dependencies

### 7.1 Assumptions

| ID | Assumption | Risk if Invalid |
|---|---|---|
| A-01 | Bluesky Jetstream public endpoints (`jetstream[1-2].us-[east\|west].bsky.network`) remain available | Medium — four instances provide redundancy; Jetstream is open-source and self-hostable from the official Docker image |
| A-02 | Jetstream event volume remains in the 100–1,000 events/sec range | Low — at observed ~240 events/sec the pipeline uses ~5% of the server's 5,000 events/sec subscriber cap; Spark handles volume increases with container memory tuning |
| A-03 | The AT Protocol collection schemas (post, like, repost, follow, block) remain stable | Low — the staging layer isolates downstream tables from raw format changes; schema evolution is handled at the staging boundary |
| A-04 | The cardiffnlp/twitter-xlm-roberta-base-sentiment model remains available on HuggingFace | Low — the model is baked into the Docker image at build time; once built, the image is self-contained |
| A-05 | The host machine has an NVIDIA GPU with CUDA support and nvidia-container-toolkit installed | Medium — the pipeline adapts to CPU inference automatically, with reduced throughput (~5-15 texts/sec vs. 200-500 texts/sec on GPU) |

### 7.2 Dependencies

| ID | Dependency | Type | Detail |
|---|---|---|---|
| D-01 | Bluesky Jetstream WebSocket | External service | `wss://jetstream2.us-east.bsky.network/subscribe` — public, unauthenticated, rate-limit-free |
| D-02 | Apache Spark 4.x | Open-source software | Built-in Iceberg support, Python DataSource V2 API |
| D-03 | cardiffnlp/twitter-xlm-roberta-base-sentiment | Open-source ML model | ~1.1 GB, fine-tuned on ~198M tweets, 100+ languages |
| D-04 | NVIDIA GPU + nvidia-container-toolkit | Host hardware/software | Required for GPU inference; CPU fallback available |
| D-05 | Cloudflare account + registered domain | External service | ~$10/year domain + free Cloudflare plan for tunnel |
| D-06 | Docker Engine + Docker Compose | Host software | Container orchestration for the 8-container stack |

---

## 8. Constraints

| ID | Category | Constraint |
|---|---|---|
| C-01 | Hardware | 32 GB RAM workstation with NVIDIA GPU, running WSL2 on Linux 5.15 |
| C-02 | Compute | ~24 GB allocated to Docker; ~8 GB reserved for the host workstation |
| C-03 | Memory budget | Unified architecture: spark (14 GB), seaweedfs (1 GB), clickhouse (2 GB), polaris (1 GB), postgres (1 GB), grafana (512 MB), init (512 MB) — total ~22 GB |
| C-04 | Storage | All data stored locally in SeaweedFS within Docker volumes; 30-day retention window |
| C-05 | Network | Single outbound WebSocket to Jetstream; public access via Cloudflare Tunnel deferred |
| C-06 | Technology | Four core technologies — Spark, Iceberg, ClickHouse, Grafana — plus supporting infrastructure (SeaweedFS, Polaris, PostgreSQL) |
| C-07 | Networking | Two Docker networks: `atmosphere-data` (internal compute/storage) and `atmosphere-frontend` (serving/public access) |

---

## 9. Success Metrics

### 9.1 Key Performance Indicators

| KPI | Target | Measurement Method |
|---|---|---|
| Ingestion throughput | ~240 events/sec sustained | `mart_pipeline_health` — events ingested per second |
| End-to-end latency | < 10 seconds | `mart_pipeline_health` — delta between event `time_us` and dashboard query time |
| Sentiment coverage | 100% of posts scored | Row count: `core_post_sentiment` matches `core_posts` |
| Dashboard availability | Available during all pipeline runtime | Grafana `/api/health` endpoint |
| Cold start time | Full stack operational within 5 minutes of `make up` | Time from `make up` to first Grafana panel rendering data |
| Language coverage | Sentiment scores across all observed languages | Spot-check labels for Japanese (~26% of posts), Korean, Spanish, German |

### 9.2 Acceptance Criteria

The project is complete when:

1. **The pipeline runs continuously** — all containers healthy, data flowing through all four medallion layers (`atmosphere.raw` → `atmosphere.staging` → `atmosphere.core` → `atmosphere.mart`)
2. **The dashboard is live** — five sections rendering real-time data with 5-second refresh via Query API
3. **Sentiment analysis is active** — every post receives a multilingual sentiment score with visible positive/negative/neutral distribution on the Sentiment Live Feed row
4. **The public URL is accessible** — `atmosphere.yourdomain.com` loads the Grafana dashboard through Cloudflare Tunnel
5. **The environment is reproducible** — `git clone` followed by `make up` produces a working system
6. **Documentation is complete** — BRD, TDD, and README are published and internally consistent

---

## 10. Solution Approach

### 10.1 Architecture Summary

| Technology | Responsibilities |
|---|---|
| **Apache Spark 4.x** | WebSocket ingestion (custom DataSource V2), stream processing (Structured Streaming, 5-second micro-batches), SQL transformation, ML inference (`mapInPandas` with GPU) |
| **Apache Iceberg** | ACID table format across three medallion layers — `atmosphere.raw`, `atmosphere.staging`, `atmosphere.core` — on SeaweedFS with Polaris REST catalog |
| **ClickHouse 26.1** | Query serving via the `DataLakeCatalog` engine over Polaris. 11 base mart views + 17 panel views in `atmosphere.*`, all cached against the named filesystem cache `s3_cache` |
| **Grafana** | Live dashboard with five analytical sections, provisioned as code, connected via the native `grafana-clickhouse-datasource` plugin |

### 10.2 Data Flow

```mermaid
flowchart LR
    JS[Jetstream\nWebSocket] --> SU[spark]
    SU --> RAW[(raw_events)]
    RAW --> SU
    SU --> STG[(stg_posts\nstg_likes\nstg_reposts\nstg_follows\nstg_blocks\nstg_profiles)]
    STG --> SU
    SU --> CORE[(core_posts\ncore_mentions\ncore_hashtags\ncore_engagement)]
    SU --> SENT[(core_post_sentiment)]
    CORE & SENT --> CH[ClickHouse\n11 base + 17 panel views]
    CH --> GF[Grafana]
```

Five Structured Streaming queries run inside a single unified `spark` process, chained via Iceberg `readStream`. Each processes 5-second micro-batches. ClickHouse reads the resulting Iceberg tables through the Polaris REST catalog and exposes them as a two-tier view layer: 11 base `mart_*` views over the medallion layers, and 17 `panel_*` views (one per Grafana panel) that compose on the base views. Every view caches results against the named filesystem cache `s3_cache`.

### 10.3 Key Technical Differentiators

| Component | What It Demonstrates |
|---|---|
| **Custom PySpark DataSource V2** | Deep Spark internals — implementing the streaming source contract (`initialOffset`, `read`, `commit`, `schema`) entirely in Python, with dual offset tracking (Spark checkpoint + Jetstream cursor) |
| **GPU-accelerated `mapInPandas`** | ML engineering integrated into a data pipeline — vectorized batch inference at batch_size=64 with automatic GPU/CPU adaptation, processing ~200-500 texts/sec on GPU |
| **Five chained streaming queries** | Production streaming architecture — independent applications coordinated through Iceberg table state, each with its own lifecycle, memory allocation, and checkpoint |
| **ClickHouse view layer over Iceberg** | Engine-level fit-for-purpose — one streaming engine for ingest/transform, a separate purpose-built OLAP engine for query serving, sharing the same Iceberg/Polaris metadata. Two view tiers (base mart + per-panel) keep dashboard SQL out of the dashboard JSON. |

---

## 11. Timeline

### 11.1 Phases

| Phase | Deliverables | Dependencies |
|---|---|---|
| **Phase 1: Foundation** | `docker-compose.yml` with infrastructure containers (SeaweedFS, Polaris, PostgreSQL). Init container creates `warehouse` bucket, `atmosphere` catalog, and the three Iceberg namespaces (`raw`, `staging`, `core`). | — |
| **Phase 2: Ingestion** | Custom `JetstreamDataSource` + `JetstreamStreamReader` (Python DataSource V2). Ingest layer writing `raw_events` to `atmosphere.raw` with dual offset tracking. | Phase 1 |
| **Phase 3: Staging** | Staging layer parsing all six collection types into typed staging tables with daily partitioning. | Phase 2 |
| **Phase 4: Core** | Core layer producing five enriched core tables (`core_posts`, `core_mentions`, `core_hashtags`, `core_engagement`, `core_post_sentiment`). | Phase 3 |
| **Phase 5: Sentiment** | Sentiment layer running XLM-RoBERTa inference on GPU via `mapInPandas` (batch_size=64). Docker image with CUDA + embedded model weights (~1.1 GB). All four streaming layers consolidated into the unified spark process. | Phase 4 |
| **Phase 6: ClickHouse Serving** | ClickHouse 26.1 with `DataLakeCatalog` engine over Polaris. Init bootstraps the reader principal + `polaris_catalog` database. Spark bootstraps 11 base mart views + 17 panel views from `spark/serving/views/`. Grafana provisioned with the native ClickHouse datasource and a 19-panel dashboard. | Phase 5 |
| **Phase 7: Public Access** | Cloudflare Tunnel reinstatement, public URL, README and documentation finalized. **Deferred until ClickHouse migration stabilizes.** | Phase 6 |

### 11.2 Phase Dependencies

```mermaid
flowchart TD
    P1[Phase 1\nFoundation] --> P2[Phase 2\nIngestion]
    P2 --> P3[Phase 3\nStaging]
    P3 --> P4[Phase 4\nCore + Mart]
    P4 --> P5[Phase 5\nSentiment]
    P4 --> P6[Phase 6\nDashboard]
    P5 --> P6
    P6 --> P7[Phase 7\nPublic Access]
```

---

## 12. Risk Assessment

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R-01 | Jetstream public endpoints become unavailable | Low | High | Four public instances across US-East and US-West provide redundancy. Jetstream is open-source (Go); self-hosting from the official Docker image is a documented fallback. |
| R-02 | Event volume spikes beyond pipeline capacity | Low | Medium | At ~240 events/sec the pipeline operates well within the server's 5,000 events/sec subscriber cap. Spark container memory is configurable; the 50,000-event buffer in the custom data source provides backpressure. |
| R-03 | GPU becomes unavailable | Medium | Low | The pipeline adapts automatically to CPU inference at startup. Throughput decreases (~5-15 texts/sec vs. ~200-500 on GPU) but the pipeline continues operating. |
| R-04 | Spark 4.x Python DataSource V2 API has undocumented limitations | Medium | Medium | The custom source is isolated in a single file (`jetstream_source.py`). The API is stable as of Spark 4.x GA. Changes are contained to one component. |
| R-05 | Iceberg streaming reads introduce latency between layers | Low | Medium | Each layer operates independently with its own checkpoint. Latency between layers is visible in the `mart_pipeline_health` table and the Pipeline Health dashboard row. |
| R-06 | XLM-RoBERTa produces low-quality sentiment for underrepresented languages | Medium | Low | The model covers 100+ languages but accuracy varies by language. The dashboard displays sentiment distribution, making quality visible. The model can be swapped via a single configuration change in the Dockerfile. |
| R-07 | Docker Compose resource contention on 32 GB machine | Low | Medium | Memory allocations are pre-budgeted per container (~22 GB total), leaving ~8 GB for the host and ~1.7 GB headroom. The unified architecture consolidates 4 Spark containers into 1, saving ~3 GB JVM overhead. |

---

## 13. Glossary

| Term | Definition |
|---|---|
| **AT Protocol** | The Authenticated Transfer Protocol — the decentralized social networking protocol underlying Bluesky |
| **Bluesky** | A decentralized social network built on the AT Protocol |
| **BRD** | Business Requirements Document — this document; defines the project's objectives, scope, requirements, and success criteria |
| **Collection** | A named set of records within an AT Protocol user repository, identified by an NSID (e.g., `app.bsky.feed.post`) |
| **DataSource V2** | Spark's provider API for implementing custom data sources with full streaming support (offset tracking, checkpointing) |
| **DID** | Decentralized Identifier — a globally unique, persistent identifier for a user account in the AT Protocol |
| **Iceberg** | Apache Iceberg — an open table format providing ACID transactions, schema evolution, and time travel |
| **Jetstream** | A service that converts the AT Protocol's binary CBOR firehose into lightweight JSON events delivered over WebSocket |
| **mapInPandas** | A Spark API that applies a Python function to batches of rows as Pandas DataFrames, enabling vectorized batch ML inference |
| **Medallion architecture** | A data organization pattern with progressively refined layers: raw, staging, core, mart |
| **Polaris** | Apache Polaris — an open-source Iceberg REST catalog for multi-engine metadata management |
| **SeaweedFS** | An Apache 2.0-licensed, S3-API-compatible object storage server |
| **Structured Streaming** | Spark's stream processing engine that treats live data streams as continuously appended tables |
| **TDD** | Technical Design Document — the companion document covering implementation details, data model, and system architecture |
| **ClickHouse `DataLakeCatalog`** | A ClickHouse database engine that mounts an Iceberg REST catalog (Polaris) as a queryable database, exposing every catalog table to ClickHouse SQL without ingestion. |
| **`s3_cache`** | The named filesystem cache configured on the ClickHouse storage policy. Every `atmosphere.*` view ends with `SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1` because the DataLakeCatalog read path does not propagate profile-level cache settings. |
| **XLM-RoBERTa** | A cross-lingual transformer model trained on 100+ languages, used for multilingual sentiment classification |
