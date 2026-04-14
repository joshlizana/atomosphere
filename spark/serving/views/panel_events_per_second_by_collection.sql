CREATE OR REPLACE VIEW atmosphere.panel_events_per_second_by_collection AS
SELECT
    bucket                              AS window_start,
    collection,
    count / 10.0                        AS events_per_second
FROM atmosphere.mart_firehose_stats
WHERE bucket >= now() - INTERVAL 60 MINUTE
ORDER BY window_start
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
