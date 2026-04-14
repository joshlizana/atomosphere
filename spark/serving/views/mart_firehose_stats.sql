CREATE OR REPLACE VIEW atmosphere.mart_firehose_stats AS
SELECT
    toStartOfInterval(ingested_at, INTERVAL 10 SECOND) AS bucket,
    collection,
    count()                                            AS count,
    uniq(did)                                          AS unique_dids_approx
FROM polaris_catalog.`raw.raw_events`
WHERE ingested_at >= now() - INTERVAL 2 HOUR
GROUP BY bucket, collection
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
