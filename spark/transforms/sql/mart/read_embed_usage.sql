-- read_embed_usage: posts per embed_type over the last 5 minutes
-- Source: atmosphere.mart.mart_embed_usage

SELECT
    embed_type,
    SUM(count) AS total
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
GROUP BY embed_type
ORDER BY total DESC
