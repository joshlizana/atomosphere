CREATE OR REPLACE VIEW atmosphere.panel_language_distribution AS
SELECT
    primary_lang,
    sum(count) AS post_count
FROM atmosphere.mart_language_distribution
WHERE bucket_min >= now() - INTERVAL 15 MINUTE
GROUP BY primary_lang
ORDER BY post_count DESC
LIMIT 10
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
