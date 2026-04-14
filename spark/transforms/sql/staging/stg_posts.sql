-- stg_posts: Extract post fields from raw_events (FR-05)
-- Source: atmosphere.raw.raw_events WHERE collection = 'app.bsky.feed.post'
-- Record schema: reference/lexicon-feed-post.json

SELECT
    did,
    time_us,
    CAST(time_us / 1000000 AS TIMESTAMP)                                    AS event_time,
    get_json_object(raw_json, '$.commit.rkey')                              AS rkey,
    get_json_object(raw_json, '$.commit.operation')                         AS operation,
    get_json_object(raw_json, '$.commit.record.text')                       AS text,
    CAST(get_json_object(raw_json, '$.commit.record.createdAt') AS TIMESTAMP) AS created_at,
    from_json(get_json_object(raw_json, '$.commit.record.langs'), 'ARRAY<STRING>') AS langs,
    get_json_object(raw_json, '$.commit.record.embed') IS NOT NULL          AS has_embed,
    CASE
        WHEN get_json_object(raw_json, '$.commit.record.embed.$type') LIKE '%images%' THEN 'images'
        WHEN get_json_object(raw_json, '$.commit.record.embed.$type') LIKE '%video%' THEN 'video'
        WHEN get_json_object(raw_json, '$.commit.record.embed.$type') LIKE '%external%' THEN 'external'
        WHEN get_json_object(raw_json, '$.commit.record.embed.$type') LIKE '%recordWithMedia%' THEN 'recordWithMedia'
        WHEN get_json_object(raw_json, '$.commit.record.embed.$type') LIKE '%record%' THEN 'record'
        ELSE NULL
    END                                                                     AS embed_type,
    get_json_object(raw_json, '$.commit.record.reply') IS NOT NULL          AS is_reply,
    get_json_object(raw_json, '$.commit.record.reply.root.uri')             AS reply_root_uri,
    get_json_object(raw_json, '$.commit.record.reply.parent.uri')           AS reply_parent_uri,
    get_json_object(raw_json, '$.commit.record.facets')                     AS facets_json,
    get_json_object(raw_json, '$.commit.record.labels')                     AS labels_json,
    from_json(get_json_object(raw_json, '$.commit.record.tags'), 'ARRAY<STRING>') AS tags,
    current_timestamp()                                                     AS ingested_at
FROM {source}
WHERE collection = 'app.bsky.feed.post'
