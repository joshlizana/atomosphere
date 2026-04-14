CREATE OR REPLACE VIEW atmosphere.panel_pipeline_health AS
SELECT
    measured_at,
    events_last_minute,
    events_per_second,
    last_event_ingested,
    processing_lag_seconds,
    max_event_time
FROM atmosphere.mart_pipeline_health
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
