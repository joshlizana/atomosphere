-- read_trending_hashtags: trending hashtags ranked by spike ratio.
-- current_count  = SUM(count) over the last {window} minutes
-- baseline_count = SUM(count) over the 25 minutes preceding the current window
-- spike_ratio    = current_count / baseline_count (NULL-safe)
-- Source: atmosphere.mart.mart_trending_hashtags
-- {window} is parameterized by the query-api endpoint (?window=N, default 5)

WITH current_window AS (
    SELECT tag, SUM(count) AS current_count
    FROM {source}
    WHERE bucket_min >= now() - INTERVAL {window} MINUTES
    GROUP BY tag
),
baseline_window AS (
    SELECT tag, SUM(count) AS baseline_count
    FROM {source}
    WHERE bucket_min <  now() - INTERVAL {window} MINUTES
      AND bucket_min >= now() - INTERVAL ({window} + 25) MINUTES
    GROUP BY tag
)
SELECT
    now()                                              AS computed_at,
    c.tag,
    c.current_count,
    COALESCE(b.baseline_count, 0)                      AS baseline_count,
    CASE
        WHEN COALESCE(b.baseline_count, 0) = 0 THEN c.current_count * 1.0
        ELSE c.current_count * 1.0 / b.baseline_count
    END                                                AS spike_ratio
FROM current_window c
LEFT JOIN baseline_window b USING (tag)
WHERE c.current_count >= 3
ORDER BY spike_ratio DESC, c.current_count DESC
LIMIT 50
