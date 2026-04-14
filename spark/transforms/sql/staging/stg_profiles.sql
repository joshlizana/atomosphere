-- stg_profiles: Extract profile fields from raw_events (FR-05)
-- Source: atmosphere.raw.raw_events WHERE collection = 'app.bsky.actor.profile'
-- Profile records have display_name and description at the record level

SELECT
    did,
    time_us,
    CAST(time_us / 1000000 AS TIMESTAMP)                                    AS event_time,
    get_json_object(raw_json, '$.commit.rkey')                              AS rkey,
    get_json_object(raw_json, '$.commit.operation')                         AS operation,
    get_json_object(raw_json, '$.commit.record.displayName')                AS display_name,
    get_json_object(raw_json, '$.commit.record.description')                AS description,
    current_timestamp()                                                     AS ingested_at
FROM {source}
WHERE collection = 'app.bsky.actor.profile'
