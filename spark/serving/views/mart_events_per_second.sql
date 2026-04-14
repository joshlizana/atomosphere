CREATE OR REPLACE VIEW atmosphere.mart_events_per_second AS
SELECT
    toStartOfMinute(ingested_at) AS bucket_min,
    count() AS count
FROM polaris_catalog.`raw.raw_events`
GROUP BY bucket_min
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
