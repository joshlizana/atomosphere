CREATE OR REPLACE VIEW atmosphere.mart_top_posts AS
SELECT
    toStartOfMinute(p.event_time) AS bucket_min,
    p.did                          AS did,
    p.time_us                      AS time_us,
    p.rkey                         AS rkey,
    p.text                         AS text,
    p.primary_lang                 AS primary_lang,
    p.content_type                 AS content_type,
    s.sentiment_positive           AS sentiment_positive,
    s.sentiment_negative           AS sentiment_negative,
    s.sentiment_neutral            AS sentiment_neutral,
    s.sentiment_label              AS sentiment_label,
    s.sentiment_confidence         AS sentiment_confidence
FROM polaris_catalog.`core.core_posts` AS p
INNER JOIN polaris_catalog.`core.core_post_sentiment` AS s
    ON p.did = s.did AND p.time_us = s.time_us
WHERE p.event_time >= toStartOfDay(now())
  AND s.event_time >= toStartOfDay(now())
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
