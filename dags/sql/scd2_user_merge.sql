-- ==============================================================================
-- SCD TYPE-2 USER MERGE LOGIC (PostgreSQL)
-- Parameter: :execution_date (e.g. '2026-10-06')
-- ==============================================================================

-- 1. Identify existing users whose attributes have changed in the incoming batch
WITH stage_users AS (
    SELECT DISTINCT ON (user_id)
        user_id,
        country,
        preferred_platform,
        'standard' AS user_tier,
        event_timestamp
    FROM staging.stg_events
    WHERE event_date = :batch_date::DATE
    ORDER BY user_id, event_timestamp DESC
),
users_to_close AS (
    SELECT 
        d.user_sk,
        s.event_timestamp AS close_timestamp
    FROM analytics.dim_user d
    JOIN stage_users s ON d.user_id = s.user_id
    WHERE d.is_current = TRUE
      AND (d.country <> s.country OR d.preferred_platform <> s.preferred_platform)
)
-- Expire outdated historical records
UPDATE analytics.dim_user d
SET 
    valid_to = u.close_timestamp,
    is_current = FALSE
FROM users_to_close u
WHERE d.user_sk = u.user_sk;

-- 2. Insert new user profiles or new versions for updated users
INSERT INTO analytics.dim_user (
    user_id, country, preferred_platform, user_tier, valid_from, valid_to, is_current
)
SELECT 
    s.user_id,
    s.country,
    s.preferred_platform,
    'standard',
    s.event_timestamp,
    '9999-12-31 23:59:59+00'::TIMESTAMPTZ,
    TRUE
FROM (
    SELECT DISTINCT ON (user_id)
        user_id, country, preferred_platform, event_timestamp
    FROM staging.stg_events
    WHERE event_date = :batch_date::DATE
    ORDER BY user_id, event_timestamp DESC
) s
LEFT JOIN analytics.dim_user d 
    ON s.user_id = d.user_id AND d.is_current = TRUE
WHERE d.user_sk IS NULL;
