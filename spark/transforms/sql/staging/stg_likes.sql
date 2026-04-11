-- stg_likes: Extract like fields from raw_events (FR-05)
-- Source: atmosphere.raw.raw_events WHERE collection = 'app.bsky.feed.like'
-- Record schema: reference/lexicon-feed-like.json

SELECT
    did,
    time_us,
    CAST(time_us / 1000000 AS TIMESTAMP)                                    AS event_time,
    get_json_object(raw_json, '$.commit.rkey')                              AS rkey,
    get_json_object(raw_json, '$.commit.operation')                         AS operation,
    get_json_object(raw_json, '$.commit.record.subject.uri')                AS subject_uri,
    get_json_object(raw_json, '$.commit.record.subject.cid')                AS subject_cid,
    CAST(get_json_object(raw_json, '$.commit.record.createdAt') AS TIMESTAMP) AS created_at,
    get_json_object(raw_json, '$.commit.record.via') IS NOT NULL            AS has_via
FROM {source}
WHERE collection = 'app.bsky.feed.like'
