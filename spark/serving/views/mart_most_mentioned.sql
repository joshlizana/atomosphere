CREATE OR REPLACE VIEW atmosphere.mart_most_mentioned AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    mentioned_did,
    count() AS count
FROM polaris_catalog.`core.core_mentions`
GROUP BY bucket_min, mentioned_did
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
