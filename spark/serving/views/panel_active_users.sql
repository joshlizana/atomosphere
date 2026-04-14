CREATE OR REPLACE VIEW atmosphere.panel_active_users AS
SELECT sum(unique_dids_approx) AS active_users
FROM atmosphere.mart_firehose_stats
WHERE bucket >= now() - INTERVAL 15 MINUTE
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
