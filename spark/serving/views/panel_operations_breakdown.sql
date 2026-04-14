CREATE OR REPLACE VIEW atmosphere.panel_operations_breakdown AS
SELECT
    toStartOfInterval(ingested_at, INTERVAL 10 SECOND) AS window_start,
    operation,
    count()                                             AS op_count
FROM polaris_catalog.`raw.raw_events`
WHERE ingested_at >= now() - INTERVAL 60 MINUTE
GROUP BY window_start, operation
ORDER BY window_start
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
