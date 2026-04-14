CREATE OR REPLACE VIEW atmosphere.panel_sentiment_timeseries AS
SELECT
    toStartOfMinute(event_time)         AS window_start,
    avg(sentiment_positive)             AS avg_positive,
    avg(sentiment_negative)             AS avg_negative,
    avg(sentiment_neutral)              AS avg_neutral,
    count()                             AS post_count
FROM polaris_catalog.`core.core_post_sentiment`
WHERE event_time >= toStartOfDay(now())
GROUP BY window_start
ORDER BY window_start
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
