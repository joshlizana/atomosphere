CREATE OR REPLACE VIEW atmosphere.panel_trending_hashtags AS
WITH
    current_window AS (
        SELECT tag, sum(count) AS current_count
        FROM atmosphere.mart_trending_hashtags
        WHERE bucket_min >= now() - INTERVAL 5 MINUTE
        GROUP BY tag
    ),
    baseline_window AS (
        SELECT tag, avg(window_count) AS baseline_count
        FROM (
            SELECT
                tag,
                toStartOfFiveMinute(bucket_min) AS bucket5,
                sum(count) AS window_count
            FROM atmosphere.mart_trending_hashtags
            WHERE bucket_min >= now() - INTERVAL 30 MINUTE
              AND bucket_min <  now() - INTERVAL 5 MINUTE
            GROUP BY tag, bucket5
        )
        GROUP BY tag
    )
SELECT
    now()                                                AS computed_at,
    c.tag                                                AS tag,
    c.current_count                                      AS current_count,
    coalesce(b.baseline_count, 0)                        AS baseline_count,
    c.current_count / nullIf(b.baseline_count, 0)        AS spike_ratio
FROM current_window AS c
LEFT JOIN baseline_window AS b ON c.tag = b.tag
WHERE c.current_count >= 5
ORDER BY spike_ratio DESC NULLS LAST
LIMIT 50
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
