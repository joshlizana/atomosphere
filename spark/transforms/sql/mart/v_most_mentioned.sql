-- v_most_mentioned: Top mentioned DIDs (FR-18)
-- Ranks DIDs by mention count over the most recent 5-minute window

SELECT
    mentioned_did,
    COUNT(*)                                                                AS mention_count
FROM atmosphere.core.core_mentions
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY mentioned_did
ORDER BY mention_count DESC
LIMIT 25
