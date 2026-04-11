-- stg_reposts: Extract repost fields from raw_events (FR-05)
-- Source: atmosphere.raw.raw_events WHERE collection = 'app.bsky.feed.repost'
-- Record schema: reference/lexicon-feed-repost.json

SELECT
    did,
    time_us,
    CAST(time_us / 1000000 AS TIMESTAMP)                                    AS event_time,
    get_json_object(raw_json, '$.commit.rkey')                              AS rkey,
    get_json_object(raw_json, '$.commit.operation')                         AS operation,
    get_json_object(raw_json, '$.commit.record.subject.uri')                AS subject_uri,
    get_json_object(raw_json, '$.commit.record.subject.cid')                AS subject_cid,
    CAST(get_json_object(raw_json, '$.commit.record.createdAt') AS TIMESTAMP) AS created_at
FROM {source}
WHERE collection = 'app.bsky.feed.repost'
