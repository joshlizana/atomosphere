-- read_content_breakdown: posts per content_type over the last 5 minutes
-- Source: atmosphere.mart.mart_content_breakdown

SELECT
    content_type,
    SUM(count) AS total
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
GROUP BY content_type
ORDER BY total DESC
