CREATE OR REPLACE VIEW atmosphere.panel_likes_per_second AS
SELECT
    bucket                              AS window_start,
    count / 10.0                        AS events_per_second
FROM atmosphere.mart_engagement_velocity
WHERE event_type = 'like'
  AND bucket >= now() - INTERVAL 60 MINUTE
ORDER BY window_start
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
