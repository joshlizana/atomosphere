-- core_posts: Enrich staged posts with derived fields (FR-06)
-- Source: atmosphere.staging.stg_posts (batch view)
-- Extracts hashtags, mention_dids, link_urls from facets_json via regexp
-- Derives primary_lang and content_type

SELECT
    did,
    time_us,
    event_time,
    rkey,
    text,
    created_at,
    COALESCE(get(langs, 0), 'unknown')                                       AS primary_lang,
    is_reply,
    has_embed,
    embed_type,
    CASE
        WHEN is_reply THEN 'reply'
        WHEN embed_type = 'record' THEN 'quote'
        WHEN has_embed THEN 'media_post'
        ELSE 'original'
    END                                                                     AS content_type,
    -- Hashtags: combine facet tags + tags field, all lowercase
    array_union(
        COALESCE(transform(
            regexp_extract_all(COALESCE(facets_json, ''), '"\\$type"\\s*:\\s*"app\\.bsky\\.richtext\\.facet#tag"\\s*,\\s*"tag"\\s*:\\s*"([^"]+)"', 1),
            x -> lower(x)
        ), array()),
        COALESCE(transform(tags, x -> lower(x)), array())
    )                                                                       AS hashtags,
    -- Mention DIDs from facets
    regexp_extract_all(COALESCE(facets_json, ''), '"\\$type"\\s*:\\s*"app\\.bsky\\.richtext\\.facet#mention"\\s*,\\s*"did"\\s*:\\s*"(did:[^"]+)"', 1) AS mention_dids,
    -- Link URLs from facets
    regexp_extract_all(COALESCE(facets_json, ''), '"\\$type"\\s*:\\s*"app\\.bsky\\.richtext\\.facet#link"\\s*,\\s*"uri"\\s*:\\s*"([^"]+)"', 1) AS link_urls
FROM {source}
WHERE operation = 'create'
