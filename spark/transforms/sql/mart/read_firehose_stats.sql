-- read_firehose_stats: per-collection event count + approx unique DIDs
-- over the last 5 minutes. Source: atmosphere.mart.mart_firehose_stats
-- unique_dids_approx is summed across 1-min buckets — slight upper bound,
-- so the dashboard panel is labeled 'Active Users' (not 'Unique Users').

SELECT
    SUM(count)              AS total_events,
    SUM(unique_dids_approx) AS active_users
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
