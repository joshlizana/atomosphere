CREATE OR REPLACE VIEW atmosphere.mart_trending_hashtags AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    tag,
    count() AS count
FROM polaris_catalog.`core.core_hashtags`
GROUP BY bucket_min, tag
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
