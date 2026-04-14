CREATE OR REPLACE VIEW atmosphere.mart_language_distribution AS
SELECT
    toStartOfMinute(event_time) AS bucket_min,
    primary_lang,
    count() AS count
FROM polaris_catalog.`core.core_posts`
GROUP BY bucket_min, primary_lang
SETTINGS filesystem_cache_name = 's3_cache', enable_filesystem_cache = 1
