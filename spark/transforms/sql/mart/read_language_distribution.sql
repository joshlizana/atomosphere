-- read_language_distribution: posts per language over the last 5 minutes
-- Source: atmosphere.mart.mart_language_distribution

SELECT
    primary_lang,
    SUM(count) AS total
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
GROUP BY primary_lang
ORDER BY total DESC
LIMIT 25
