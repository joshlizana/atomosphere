-- v_content_breakdown: Post distribution by content type (FR-17)
-- Breaks down posts into original/reply/quote/media_post categories

SELECT
    content_type,
    COUNT(*)                                                                AS post_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)                     AS pct
FROM atmosphere.core.core_posts
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY content_type
ORDER BY post_count DESC
