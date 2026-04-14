CREATE OR REPLACE VIEW atmosphere.panel_content_breakdown AS
SELECT
    coalesce(content_type, 'unknown') AS content_type,
    toInt64(sum(count))                AS post_count
FROM atmosphere.mart_content_breakdown
WHERE bucket_min >= now() - INTERVAL 15 MINUTE
GROUP BY content_type
ORDER BY post_count DESC
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
