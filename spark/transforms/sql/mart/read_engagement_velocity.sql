-- read_engagement_velocity: likes/reposts/follows per minute (15 min window)
-- Source: atmosphere.mart.mart_engagement_velocity

SELECT
    bucket_min,
    event_type,
    count
FROM {source}
WHERE bucket_min >= now() - INTERVAL 15 MINUTES
ORDER BY bucket_min DESC, event_type
