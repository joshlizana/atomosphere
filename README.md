# Atmosphere

Real-time Bluesky social network analytics with multilingual sentiment analysis.

## Tech Stack

| Technology | Role |
|---|---|
| Apache Spark 4.x | Ingestion, streaming, transformation, ML inference, query serving |
| Apache Iceberg | Open table format (ACID, schema evolution, time travel) |
| Grafana | Live dashboards with 5-second refresh |
| RustFS | S3-compatible object storage |
| Apache Polaris | Iceberg REST catalog |
| HuggingFace Transformers | GPU-accelerated multilingual sentiment analysis (XLM-RoBERTa) |
| Docker Compose | Container orchestration (12 containers) |
| Cloudflare Tunnel | Public dashboard access |

## Getting Started

```bash
make up
```

## Documentation

For comprehensive technical information — architecture, data model, design decisions, and implementation details — refer to the [Technical Design Document](TDD.md).
# atomosphere
