-- ==============================================================================
-- 1. DAU / MAU RATIO (User Stickiness Metric)
-- Meta's Core Engagement KPI: Measures how frequently monthly active users return daily.
-- Target Engine: PostgreSQL / DuckDB
-- ==============================================================================

WITH daily_active_users AS (
    -- Grain: Unique active users per day
    SELECT
        event_date,
        COUNT(DISTINCT user_id) AS dau
    FROM analytics.fact_events
    WHERE event_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY event_date
),
calendar_spine AS (
    -- Ensure continuous date series
    SELECT d.calendar_date AS metric_date
    FROM analytics.dim_date d
    WHERE d.calendar_date BETWEEN CURRENT_DATE - INTERVAL '60 days' AND CURRENT_DATE
),
mau_calculation AS (
    -- Calculate trailing 30-day active users for each day
    -- In standard SQL, a sliding distinct count requires a self-join or window aggregation
    SELECT
        c.metric_date,
        COUNT(DISTINCT e.user_id) AS mau
    FROM calendar_spine c
    JOIN analytics.fact_events e 
      ON e.event_date BETWEEN c.metric_date - INTERVAL '29 days' AND c.metric_date
    GROUP BY c.metric_date
)
SELECT
    m.metric_date,
    COALESCE(d.dau, 0) AS dau,
    m.mau,
    -- DAU / MAU Ratio expressed as percentage
    ROUND(
        (COALESCE(d.dau, 0)::NUMERIC / NULLIF(m.mau, 0)) * 100.0, 
        2
    ) AS stickiness_ratio_pct,
    -- 7-day Moving Average of Stickiness to smooth weekday/weekend seasonality
    ROUND(
        AVG((COALESCE(d.dau, 0)::NUMERIC / NULLIF(m.mau, 0)) * 100.0) 
        OVER (ORDER BY m.metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
        2
    ) AS stickiness_7d_ma
FROM mau_calculation m
LEFT JOIN daily_active_users d ON m.metric_date = d.event_date
ORDER BY m.metric_date DESC;
