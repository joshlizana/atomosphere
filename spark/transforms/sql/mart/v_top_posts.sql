-- v_top_posts: Recent posts with metadata (FR-18)
-- Shows the most recent posts with their enriched fields

SELECT
    did,
    rkey,
    text,
    created_at,
    event_time,
    primary_lang,
    content_type,
    size(hashtags)                                                          AS hashtag_count,
    size(mention_dids)                                                      AS mention_count,
    size(link_urls)                                                         AS link_count
FROM atmosphere.core.core_posts
WHERE event_time >= current_timestamp() - INTERVAL 5 MINUTES
ORDER BY event_time DESC
LIMIT 50
