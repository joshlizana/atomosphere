-- mart_events_per_second: Events per second by collection (FR-15)
-- Computes event throughput for the most recent 5-minute window

SELECT
    window.start                                                            AS window_start,
    window.end                                                              AS window_end,
    collection,
    COUNT(*)                                                                AS event_count,
    COUNT(*) / 5.0                                                          AS events_per_second,
    COUNT(DISTINCT did)                                                     AS unique_dids
FROM atmosphere.raw.raw_events
WHERE ingested_at >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY window(ingested_at, '5 seconds'), collection
