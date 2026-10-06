-- ==============================================================================
-- 2. USER RETENTION COHORTS (Day-1, Day-7, Day-14, Day-30)
-- Standard Meta Growth Accounting: Tracks retention curve of signup cohorts over time.
-- Target Engine: PostgreSQL / DuckDB
-- ==============================================================================

WITH user_first_interaction AS (
    -- Identify the user's cohort creation date (first observed event or explicit signup)
    SELECT
        user_id,
        MIN(event_date) AS cohort_date
    FROM analytics.fact_events
    GROUP BY user_id
),
user_activity_days AS (
    -- Distinct active days for each user
    SELECT DISTINCT
        user_id,
        event_date
    FROM analytics.fact_events
),
cohort_user_retention AS (
    -- Calculate days elapsed between cohort date and subsequent active dates
    SELECT
        c.cohort_date,
        c.user_id,
        (a.event_date - c.cohort_date) AS day_diff
    FROM user_first_interaction c
    JOIN user_activity_days a ON c.user_id = a.user_id
    WHERE a.event_date >= c.cohort_date
),
cohort_aggregations AS (
    -- Aggregate cohort sizes and retention benchmarks
    SELECT
        cohort_date,
        COUNT(DISTINCT user_id) AS cohort_size,
        
        -- D1 Retention (Active exactly 1 day after signup)
        COUNT(DISTINCT CASE WHEN day_diff = 1 THEN user_id END) AS retained_d1,
        
        -- D7 Retention (Active on day 7)
        COUNT(DISTINCT CASE WHEN day_diff = 7 THEN user_id END) AS retained_d7,
        
        -- D14 Retention (Active on day 14)
        COUNT(DISTINCT CASE WHEN day_diff = 14 THEN user_id END) AS retained_d14,
        
        -- D30 Retention (Active on day 30)
        COUNT(DISTINCT CASE WHEN day_diff = 30 THEN user_id END) AS retained_d30
    FROM cohort_user_retention
    GROUP BY cohort_date
)
SELECT
    cohort_date,
    cohort_size,
    
    -- Retention Percentages
    ROUND((retained_d1::NUMERIC / NULLIF(cohort_size, 0)) * 100.0, 2) AS d1_retention_pct,
    ROUND((retained_d7::NUMERIC / NULLIF(cohort_size, 0)) * 100.0, 2) AS d7_retention_pct,
    ROUND((retained_d14::NUMERIC / NULLIF(cohort_size, 0)) * 100.0, 2) AS d14_retention_pct,
    ROUND((retained_d30::NUMERIC / NULLIF(cohort_size, 0)) * 100.0, 2) AS d30_retention_pct
FROM cohort_aggregations
WHERE cohort_date <= CURRENT_DATE - INTERVAL '30 days' -- Matured cohorts only
ORDER BY cohort_date DESC;
