-- read_top_posts: most positive + most negative posts in the last 15 minutes
-- Source: atmosphere.mart.mart_top_posts (pre-joined posts ⨝ sentiment)

WITH recent AS (
    SELECT *
    FROM {source}
    WHERE bucket_min >= now() - INTERVAL 15 MINUTES
),
top_positive AS (
    SELECT *, 'most_positive' AS rank_type
    FROM recent
    ORDER BY sentiment_positive DESC
    LIMIT 25
),
top_negative AS (
    SELECT *, 'most_negative' AS rank_type
    FROM recent
    ORDER BY sentiment_negative DESC
    LIMIT 25
)
SELECT * FROM top_positive
UNION ALL
SELECT * FROM top_negative
