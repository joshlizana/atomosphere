-- v_language_distribution: Post count by primary language (FR-17)
-- Computes language breakdown for the most recent 5-minute window

SELECT
    primary_lang,
    COUNT(*)                                                                AS post_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)                     AS pct
FROM atmosphere.core.core_posts
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY primary_lang
ORDER BY post_count DESC
LIMIT 25
