# Mart Sizing Analysis — Pre vs Post Materialization

Captured: 2026-04-12 05:07 UTC (pre-materialization baseline)
Source: `reports/sizing-20260412-051026.json`

## Why this exists

M6 Group 2 (CLH-137–CLH-150) replaced the view-based mart layer with streaming
materialized Iceberg tables. This document captures the measured baseline that
justified the change, the design targets, and the acceptance thresholds.

## Pre-materialization baseline (view-based marts)

Each mart was a SQL view over `core.*` / `raw.*` tables. Grafana panels hit
`query-api`, which ran the full aggregation on every refresh. Measured latencies
after ~1h of ingestion (~3.2 M raw events, ~3.3 M core rows):

| Mart                    | Cold (s) | Warm avg (s) | >5s SLA? |
|-------------------------|---------:|-------------:|:--------:|
| pipeline_health         |    11.67 |         3.03 | ⚠ close  |
| most_mentioned          |    20.79 |        18.75 | ❌        |
| events_per_second       |    28.68 |        16.35 | ❌        |
| language_distribution   |    23.77 |        19.67 | ❌        |
| content_breakdown       |    25.39 |        19.86 | ❌        |
| embed_usage             |    25.47 |        19.99 | ❌        |
| trending_hashtags       |    27.04 |        20.40 | ❌        |
| sentiment_timeseries    |    54.40 |        22.00 | ❌        |
| top_posts               |    60.38 |        21.13 | ❌        |
| engagement_velocity     |    88.88 |        16.90 | ❌        |

Every mart except `pipeline_health` blew the 5 s dashboard-read SLA on both
cold and warm passes. Warm-cache speedup was modest (roughly 2×), confirming
the bottleneck is compute (window aggregation over millions of rows), not I/O.

## Upstream source sizes at capture time

| Layer    |     Rows | Files   | Size (MB) | Avg file KB |
|----------|---------:|--------:|----------:|------------:|
| raw      | 3,248,772 | 41,017 |     692.5 |        17.3 |
| staging  | 3,164,718 |  7,468 |     267.9 |        36.7 |
| core     | 3,287,764 | 12,144 |     197.8 |        16.7 |
| **total**| **9.7 M** | **60,629** | **1,158.2** |     — |

~60 k files across ~1.1 GB — dominated by streaming's micro-batch churn.
Per-file averages well below Iceberg's 512 MB default target: the exact
symptom the new maintenance endpoint was built to measure and compact.

## Post-materialization design targets

Replace the 9 view-based marts with 10 streaming materialized tables
(split `firehose_stats` out of `events_per_second`). Each mart runs as its own
streaming query with a 1-minute tumbling window and 15-minute event-time
watermark, and writes append-only to an Iceberg table under `atmosphere.mart`.

- **Read SLA:** <5 s dashboard refresh, cold or warm. Dashboard reads scan
  at most the last 5–15 minutes of pre-aggregated 1-minute buckets, so query
  cost is O(buckets) ≈ O(15), not O(upstream rows).
- **Write SLA:** <5 s mart materialization per micro-batch. Streaming queries
  all use `processingTime="5 seconds"` triggers (FR-10).
- **Storage cost:** trivial. 10 marts × ~1 row/min × 4 grouping-keys × 24 h
  × 7 days ≈ 0.8 M rows → ~16 MB stack-wide. Many orders of magnitude below
  the upstream source layers.
- **No top-N at write time.** Storing every `(bucket, key)` pair and letting
  the read SQL apply `ORDER BY ... LIMIT N` at query time avoids streaming
  top-N brittleness and keeps mart tables append-only.

## Acceptance thresholds

- Every mart read endpoint returns in <5 s on a warm query-api.
- Dashboards refresh at 5 s without timeouts (Grafana Infinity default).
- Streaming queries all keep up (no growing query lag in Spark UI :4040).
- Total mart layer storage stays under 50 MB after 24 h of steady state.

## Maintenance threshold (approved 2026-04-12)

Since materialized marts are themselves Iceberg tables, they need the same
compaction treatment as upstream layers. The new `/api/maintenance/table-stats`
and `/api/maintenance/run` endpoints apply to the full set of 22
`atmosphere.*` tables (raw + 6 staging + 5 core + 10 mart).

Compaction triggers when either condition holds:

- `file_count > 500`, or
- `avg_file_kb < 30`

Per-table 10-minute rate limit prevents thrashing. Snapshot retention is 1 day.
`rewrite_data_files` runs with `min-input-files=2` (so small mart tables with
2–4 files still get compacted) and `partial-progress.enabled=true` (so a single
file-group failure on the big raw/staging tables doesn't roll back the whole
rewrite). See `reference/iceberg-maintenance-procedures.md`.

## Post-materialization re-measurement

After the streaming marts stabilize, re-run `make sizing-report` and compare:

- `mart_queries.*.latency.warm_seconds_avg` should be <0.5 s across all 10
  marts (typical Iceberg point-read on a tiny partitioned table).
- `tables.mart.*.files.value.file_count` should stay small because the
  maintenance endpoint keeps `file_count ≤ 500` per table.
- Storage under `atmosphere.mart.*` should remain a negligible fraction of
  total warehouse size.

Append the new report as a `## Post-materialization measurement` section once
captured — keep both the before and the after in this file so the delta is
visible in git history.
