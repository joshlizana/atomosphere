CREATE OR REPLACE VIEW atmosphere.panel_most_mentioned AS
SELECT
    mentioned_did,
    sum(count) AS mention_count
FROM atmosphere.mart_most_mentioned
WHERE bucket_min >= now() - INTERVAL 15 MINUTE
GROUP BY mentioned_did
ORDER BY mention_count DESC
LIMIT 20
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
