-- mart_pipeline_health: Pipeline self-monitoring metrics (FR-15, NFR-13)
-- Tracks ingestion throughput, processing lag, and last batch times

SELECT
    current_timestamp()                                                     AS measured_at,
    COUNT(*)                                                                AS events_last_minute,
    COUNT(*) / 60.0                                                         AS events_per_second,
    MAX(ingested_at)                                                        AS last_event_ingested,
    CAST(
        unix_timestamp(current_timestamp()) - unix_timestamp(MAX(ingested_at))
        AS DOUBLE
    )                                                                       AS processing_lag_seconds,
    MAX(CAST(time_us / 1000000 AS TIMESTAMP))                               AS max_event_time
FROM atmosphere.raw.raw_events
WHERE ingested_at >= current_timestamp() - INTERVAL 1 MINUTE
