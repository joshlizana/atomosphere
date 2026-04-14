CREATE OR REPLACE VIEW atmosphere.mart_embed_usage AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    embed_type,
    count() AS count
FROM polaris_catalog.`core.core_posts`
GROUP BY bucket_min, embed_type
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
