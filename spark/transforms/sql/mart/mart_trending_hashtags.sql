-- mart_trending_hashtags: Top hashtags with spike detection (FR-16)
-- Compares current 5-min count vs. 30-min baseline to compute spike ratio

SELECT
    current_timestamp()                                                     AS computed_at,
    c.tag,
    c.current_count,
    COALESCE(b.baseline_count, 0)                                           AS baseline_count,
    CASE
        WHEN COALESCE(b.baseline_count, 0) = 0 THEN c.current_count
        ELSE ROUND(c.current_count * 6.0 / b.baseline_count, 2)
    END                                                                     AS spike_ratio
FROM (
    SELECT tag, COUNT(*) AS current_count
    FROM atmosphere.core.core_hashtags
    WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
    GROUP BY tag
) c
LEFT JOIN (
    SELECT tag, COUNT(*) AS baseline_count
    FROM atmosphere.core.core_hashtags
    WHERE event_time >= current_timestamp() - INTERVAL 30 MINUTES
    GROUP BY tag
) b ON c.tag = b.tag
ORDER BY spike_ratio DESC
LIMIT 50
