CREATE OR REPLACE VIEW atmosphere.mart_engagement_velocity AS
SELECT
    toStartOfInterval(event_time, INTERVAL 10 SECOND) AS bucket,
    event_type,
    count()                                            AS count
FROM polaris_catalog.`core.core_engagement`
WHERE event_time >= now() - INTERVAL 2 HOUR
GROUP BY bucket, event_type
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
