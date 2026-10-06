-- ==============================================================================
-- 4. TOP USER SEGMENTATION (Power User / RFM Distribution)
-- Classifies users into activity tiers using window ranking functions (NTILE / PERCENT_RANK).
-- Target Engine: PostgreSQL / DuckDB
-- ==============================================================================

WITH user_activity_aggregates AS (
    -- Compute engagement depth and monetary value per user over the trailing 30 days
    SELECT
        e.user_id,
        u.country,
        u.preferred_platform,
        COUNT(e.event_sk) AS total_events,
        COUNT(DISTINCT e.event_date) AS active_days,
        COUNT(CASE WHEN e.event_type = 'post' THEN 1 END) AS post_count,
        COUNT(CASE WHEN e.event_type = 'like' THEN 1 END) AS like_count,
        COUNT(CASE WHEN e.event_type = 'purchase' THEN 1 END) AS purchase_count,
        COALESCE(SUM(o.order_amount), 0.0) AS total_spend,
        MAX(e.event_timestamp) AS last_active_at
    FROM analytics.dim_user u
    JOIN analytics.fact_events e 
      ON u.user_id = e.user_id 
     AND e.event_date >= CURRENT_DATE - INTERVAL '30 days'
    LEFT JOIN analytics.fact_orders o 
      ON e.event_id = o.event_id
    WHERE u.is_current = TRUE
    GROUP BY e.user_id, u.country, u.preferred_platform
),
user_percentiles AS (
    -- Rank users based on activity frequency and active days
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY total_events DESC) AS activity_percentile_rank,
        NTILE(10) OVER (ORDER BY total_events DESC) AS activity_decile
    FROM user_activity_aggregates
),
segmented_users AS (
    -- Assign business tiers based on activity percentiles
    SELECT
        user_id,
        country,
        preferred_platform,
        total_events,
        active_days,
        post_count,
        like_count,
        purchase_count,
        total_spend,
        last_active_at,
        activity_decile,
        CASE
            WHEN activity_percentile_rank <= 0.05 THEN '1_POWER_CREATOR'      -- Top 5%
            WHEN activity_percentile_rank <= 0.20 THEN '2_CORE_ENGAGER'       -- Next 15%
            WHEN activity_percentile_rank <= 0.60 THEN '3_CASUAL_CONSUMER'    -- Middle 40%
            ELSE '4_LIGHT_USER'                                              -- Bottom 40%
        END AS user_tier
    FROM user_percentiles
)
-- Aggregate summary showing tier distribution and contribution to platform revenue/events
SELECT
    user_tier,
    COUNT(user_id) AS total_users,
    ROUND((COUNT(user_id)::NUMERIC / SUM(COUNT(user_id)) OVER ()) * 100.0, 2) AS pct_of_userbase,
    ROUND(AVG(total_events), 1) AS avg_events_per_user,
    ROUND(AVG(active_days), 1) AS avg_active_days_30d,
    ROUND(AVG(post_count), 1) AS avg_posts,
    ROUND(AVG(like_count), 1) AS avg_likes,
    ROUND(SUM(total_spend), 2) AS tier_total_revenue,
    -- Revenue concentration: Pareto Principle check
    ROUND((SUM(total_spend) / NULLIF(SUM(SUM(total_spend)) OVER (), 0)) * 100.0, 2) AS pct_of_total_revenue
FROM segmented_users
GROUP BY user_tier
ORDER BY user_tier ASC;
