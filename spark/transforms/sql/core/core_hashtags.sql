-- core_hashtags: Extract hashtags from staged posts (FR-08)
-- Source: atmosphere.staging.stg_posts (batch view)
-- Combines facet tag features + tags field, lowercase, no #

SELECT
    lower(exploded_tag)                                                     AS tag,
    did                                                                     AS author_did,
    rkey                                                                    AS post_rkey,
    event_time,
    current_timestamp()                                                     AS ingested_at
FROM (
    -- Tags from facets
    SELECT did, rkey, event_time,
        explode(
            regexp_extract_all(COALESCE(facets_json, ''), '"\\$type"\\s*:\\s*"app\\.bsky\\.richtext\\.facet#tag"\\s*,\\s*"tag"\\s*:\\s*"([^"]+)"', 1)
        ) AS exploded_tag
    FROM {source}
    WHERE operation = 'create'
      AND facets_json IS NOT NULL
      AND facets_json LIKE '%facet#tag%'

    UNION ALL

    -- Tags from the tags array field
    SELECT did, rkey, event_time,
        explode(tags) AS exploded_tag
    FROM {source}
    WHERE operation = 'create'
      AND tags IS NOT NULL
      AND size(tags) > 0
)
