-- core_mentions: Extract mention edges from staged posts (FR-07)
-- Source: atmosphere.staging.stg_posts (batch view)
-- Explodes mention facets into individual (author_did, mentioned_did) rows

SELECT
    did                                                                     AS author_did,
    exploded_did                                                            AS mentioned_did,
    rkey                                                                    AS post_rkey,
    event_time,
    current_timestamp()                                                     AS ingested_at
FROM (
    SELECT
        did, rkey, event_time,
        explode(
            regexp_extract_all(COALESCE(facets_json, ''), '"\\$type"\\s*:\\s*"app\\.bsky\\.richtext\\.facet#mention"\\s*,\\s*"did"\\s*:\\s*"(did:[^"]+)"', 1)
        ) AS exploded_did
    FROM {source}
    WHERE operation = 'create'
      AND facets_json IS NOT NULL
      AND facets_json LIKE '%facet#mention%'
)
