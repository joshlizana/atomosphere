CREATE OR REPLACE VIEW atmosphere.panel_follows_per_second AS
SELECT
    toStartOfInterval(event_time, INTERVAL 10 SECOND) AS window_start,
    'follows'                                          AS metric,
    count() / 10.0                                     AS events_per_second
FROM polaris_catalog.`staging.stg_follows`
WHERE event_time >= now() - INTERVAL 60 MINUTE
  AND operation = 'create'
GROUP BY window_start
ORDER BY window_start
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
