-- read_most_mentioned: top mentioned DIDs over the last 5 minutes
-- Source: atmosphere.mart.mart_most_mentioned

SELECT
    mentioned_did,
    SUM(count) AS total
FROM {source}
WHERE bucket_min >= now() - INTERVAL 5 MINUTES
GROUP BY mentioned_did
ORDER BY total DESC
LIMIT 25
