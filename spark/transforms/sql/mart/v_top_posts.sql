-- v_top_posts: Most positive and most negative recent posts (FR-18)
-- Joins core_posts with core_post_sentiment for sentiment-ranked display

WITH scored_posts AS (
    SELECT
        p.did,
        p.rkey,
        p.text,
        p.created_at,
        p.event_time,
        p.primary_lang,
        p.content_type,
        size(p.hashtags)                                                    AS hashtag_count,
        size(p.mention_dids)                                                AS mention_count,
        size(p.link_urls)                                                   AS link_count,
        s.sentiment_positive,
        s.sentiment_negative,
        s.sentiment_neutral,
        s.sentiment_label,
        s.sentiment_confidence
    FROM atmosphere.core.core_posts p
    INNER JOIN atmosphere.core.core_post_sentiment s
        ON p.did = s.did AND p.time_us = s.time_us
    WHERE p.event_time >= current_timestamp() - INTERVAL 5 MINUTES
),
top_positive AS (
    SELECT *, 'most_positive' AS rank_type
    FROM scored_posts
    ORDER BY sentiment_positive DESC
    LIMIT 25
),
top_negative AS (
    SELECT *, 'most_negative' AS rank_type
    FROM scored_posts
    ORDER BY sentiment_negative DESC
    LIMIT 25
)
SELECT * FROM top_positive
UNION ALL
SELECT * FROM top_negative
