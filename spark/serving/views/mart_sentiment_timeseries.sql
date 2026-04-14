CREATE OR REPLACE VIEW atmosphere.mart_sentiment_timeseries AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    sentiment_label,
    count() AS count
FROM polaris_catalog.`core.core_post_sentiment`
WHERE event_time >= toStartOfDay(now())
GROUP BY bucket_min, sentiment_label
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
