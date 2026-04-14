CREATE OR REPLACE VIEW atmosphere.panel_total_events AS
SELECT sum(count) AS total_events
FROM atmosphere.mart_firehose_stats
WHERE bucket >= now() - INTERVAL 15 MINUTE
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
