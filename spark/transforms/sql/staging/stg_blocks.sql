-- stg_blocks: Extract block fields from raw_events (FR-05)
-- Source: atmosphere.raw.raw_events WHERE collection = 'app.bsky.graph.block'
-- Record schema: reference/lexicon-graph-block.json

SELECT
    did,
    time_us,
    CAST(time_us / 1000000 AS TIMESTAMP)                                    AS event_time,
    get_json_object(raw_json, '$.commit.rkey')                              AS rkey,
    get_json_object(raw_json, '$.commit.operation')                         AS operation,
    get_json_object(raw_json, '$.commit.record.subject')                    AS subject_did,
    CAST(get_json_object(raw_json, '$.commit.record.createdAt') AS TIMESTAMP) AS created_at
FROM {source}
WHERE collection = 'app.bsky.graph.block'
