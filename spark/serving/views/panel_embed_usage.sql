CREATE OR REPLACE VIEW atmosphere.panel_embed_usage AS
SELECT
    coalesce(embed_type, 'unknown') AS embed_category,
    toInt64(sum(count))              AS post_count
FROM atmosphere.mart_embed_usage
WHERE bucket_min >= now() - INTERVAL 15 MINUTE
GROUP BY embed_type
ORDER BY post_count DESC
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
