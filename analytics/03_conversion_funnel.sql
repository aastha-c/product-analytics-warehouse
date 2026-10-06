-- ==============================================================================
-- 3. PRODUCT CONVERSION FUNNEL (Signup -> Login -> Post/Like -> Purchase)
-- Tracks chronological stage drop-off and conversion rates.
-- Target Engine: PostgreSQL / DuckDB
-- ==============================================================================

WITH stage_1_signup AS (
    -- Stage 1: Earliest signup timestamp per user
    SELECT
        user_id,
        MIN(event_timestamp) AS signup_time
    FROM analytics.fact_events
    WHERE event_type = 'signup'
    GROUP BY user_id
),
stage_2_login AS (
    -- Stage 2: First login timestamp occurring AFTER signup
    SELECT
        s.user_id,
        MIN(e.event_timestamp) AS login_time
    FROM stage_1_signup s
    JOIN analytics.fact_events e 
      ON s.user_id = e.user_id 
     AND e.event_type = 'login' 
     AND e.event_timestamp >= s.signup_time
    GROUP BY s.user_id
),
stage_3_engagement AS (
    -- Stage 3: First active engagement (post or like) occurring AFTER login
    SELECT
        l.user_id,
        MIN(e.event_timestamp) AS engagement_time
    FROM stage_2_login l
    JOIN analytics.fact_events e 
      ON l.user_id = e.user_id 
     AND e.event_type IN ('post', 'like') 
     AND e.event_timestamp >= l.login_time
    GROUP BY l.user_id
),
stage_4_purchase AS (
    -- Stage 4: First purchase transaction occurring AFTER engagement
    SELECT
        eng.user_id,
        MIN(e.event_timestamp) AS purchase_time
    FROM stage_3_engagement eng
    JOIN analytics.fact_events e 
      ON eng.user_id = e.user_id 
     AND e.event_type = 'purchase' 
     AND e.event_timestamp >= eng.engagement_time
    GROUP BY eng.user_id
),
funnel_counts AS (
    SELECT
        (SELECT COUNT(*) FROM stage_1_signup)     AS total_signups,
        (SELECT COUNT(*) FROM stage_2_login)      AS total_logins,
        (SELECT COUNT(*) FROM stage_3_engagement) AS total_engagements,
        (SELECT COUNT(*) FROM stage_4_purchase)   AS total_purchases
)
SELECT
    stage_name,
    step_order,
    users_reached,
    -- Step-over-step conversion rate (% of prior stage)
    ROUND(
        (users_reached::NUMERIC / NULLIF(LAG(users_reached, 1) OVER (ORDER BY step_order), 0)) * 100.0, 
        2
    ) AS step_conversion_pct,
    -- Drop-off rate (% that dropped from prior step)
    ROUND(
        100.0 - ((users_reached::NUMERIC / NULLIF(LAG(users_reached, 1) OVER (ORDER BY step_order), 0)) * 100.0), 
        2
    ) AS step_dropoff_pct,
    -- Cumulative conversion from top of funnel (Stage 1)
    ROUND(
        (users_reached::NUMERIC / NULLIF(FIRST_VALUE(users_reached) OVER (ORDER BY step_order), 0)) * 100.0, 
        2
    ) AS funnel_conversion_pct
FROM (
    SELECT '1_signup' AS stage_name, 1 AS step_order, total_signups AS users_reached FROM funnel_counts
    UNION ALL
    SELECT '2_login' AS stage_name, 2 AS step_order, total_logins AS users_reached FROM funnel_counts
    UNION ALL
    SELECT '3_post_or_like' AS stage_name, 3 AS step_order, total_engagements AS users_reached FROM funnel_counts
    UNION ALL
    SELECT '4_purchase' AS stage_name, 4 AS step_order, total_purchases AS users_reached FROM funnel_counts
) f
ORDER BY step_order ASC;
