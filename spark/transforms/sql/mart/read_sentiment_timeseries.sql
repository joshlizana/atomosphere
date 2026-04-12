-- read_sentiment_timeseries: sentiment label counts per minute (15 min window)
-- Source: atmosphere.mart.mart_sentiment_timeseries

SELECT
    bucket_min,
    sentiment_label,
    count
FROM {source}
WHERE bucket_min >= now() - INTERVAL 15 MINUTES
ORDER BY bucket_min DESC, sentiment_label
