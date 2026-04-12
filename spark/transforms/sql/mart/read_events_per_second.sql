-- read_events_per_second: time-series of pipeline throughput (5 min window)
-- Source: atmosphere.mart.mart_events_per_second (1-min tumbling buckets)

SELECT
    bucket_min,
    count
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
ORDER BY bucket_min DESC
