-- core_engagement: Unify likes and reposts into single engagement table (FR-09)
-- Sources: stg_likes_batch, stg_reposts_batch (batch views)

SELECT
    'like'          AS event_type,
    did             AS actor_did,
    subject_uri,
    event_time
FROM {likes_source}
WHERE operation = 'create'

UNION ALL

SELECT
    'repost'        AS event_type,
    did             AS actor_did,
    subject_uri,
    event_time
FROM {reposts_source}
WHERE operation = 'create'
