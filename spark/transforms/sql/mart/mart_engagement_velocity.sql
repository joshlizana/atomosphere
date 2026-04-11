-- mart_engagement_velocity: Likes/reposts/follows per second (FR-15)
-- Computes engagement rates over 5-second windows for the recent 5 minutes

SELECT
    window.start                                                            AS window_start,
    window.end                                                              AS window_end,
    event_type,
    COUNT(*)                                                                AS event_count,
    COUNT(*) / 5.0                                                          AS events_per_second
FROM atmosphere.core.core_engagement
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY window(event_time, '5 seconds'), event_type
