CREATE OR REPLACE VIEW atmosphere.mart_pipeline_health AS
SELECT
    now()                                                                          AS measured_at,
    count()                                                                        AS events_last_minute,
    count() / 60.0                                                                 AS events_per_second,
    max(ingested_at)                                                               AS last_event_ingested,
    toFloat64(toUnixTimestamp(now()) - toUnixTimestamp(max(ingested_at)))          AS processing_lag_seconds,
    toDateTime64(max(time_us) / 1000000, 6)                                        AS max_event_time
FROM polaris_catalog.`raw.raw_events`
WHERE ingested_at >= now() - INTERVAL 1 MINUTE
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
