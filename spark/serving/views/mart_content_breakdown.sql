CREATE OR REPLACE VIEW atmosphere.mart_content_breakdown AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    content_type,
    count() AS count
FROM polaris_catalog.`core.core_posts`
GROUP BY bucket_min, content_type
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
