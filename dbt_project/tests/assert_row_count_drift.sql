-- ==============================================================================
-- DBT TEST: ASSERT ROW COUNT DRIFT WITHIN ACCEPTABLE BOUNDS
-- Flags if daily event count deviates by more than +/- 30% from 7-day rolling average
-- Returns failing rows (dbt test fails if any records returned)
-- ==============================================================================

WITH daily_counts AS (
    SELECT
        event_date,
        COUNT(*) AS row_count
    FROM {{ ref('fact_events') }}
    GROUP BY event_date
),
rolling_stats AS (
    SELECT
        event_date,
        row_count,
        AVG(row_count) OVER (
            ORDER BY event_date 
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS baseline_7d_avg
    FROM daily_counts
)
SELECT
    event_date,
    row_count,
    baseline_7d_avg,
    ABS(row_count - baseline_7d_avg) / NULLIF(baseline_7d_avg, 0) AS drift_ratio
FROM rolling_stats
WHERE baseline_7d_avg IS NOT NULL
  AND ABS(row_count - baseline_7d_avg) / NULLIF(baseline_7d_avg, 0) > 0.30;
