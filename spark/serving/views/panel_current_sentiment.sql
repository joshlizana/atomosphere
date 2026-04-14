CREATE OR REPLACE VIEW atmosphere.panel_current_sentiment AS
SELECT
    sentiment_label,
    count() AS post_count
FROM polaris_catalog.`core.core_post_sentiment`
WHERE event_time >= toStartOfDay(now())
GROUP BY sentiment_label
ORDER BY post_count DESC
LIMIT 1
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
