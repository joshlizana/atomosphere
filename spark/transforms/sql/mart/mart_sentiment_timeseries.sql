-- mart_sentiment_timeseries: Rolling sentiment averages (FR-15)
-- Depends on atmosphere.core.core_post_sentiment (populated by M5: Sentiment)
-- Computes avg positive/negative/neutral over 5-second windows

SELECT
    window.start                                                            AS window_start,
    window.end                                                              AS window_end,
    COUNT(*)                                                                AS post_count,
    AVG(sentiment_positive)                                                 AS avg_positive,
    AVG(sentiment_negative)                                                 AS avg_negative,
    AVG(sentiment_neutral)                                                  AS avg_neutral,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END)          AS positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END)          AS negative_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END)           AS neutral_count
FROM atmosphere.core.core_post_sentiment
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
GROUP BY window(event_time, '5 seconds')
