CREATE OR REPLACE VIEW atmosphere.panel_top_posts_positive AS
SELECT
    text,
    sentiment_positive,
    sentiment_label,
    primary_lang,
    content_type
FROM atmosphere.mart_top_posts
WHERE bucket_min >= toStartOfDay(now())
ORDER BY sentiment_positive DESC
LIMIT 5
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
