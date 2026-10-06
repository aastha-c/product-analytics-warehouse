-- ==============================================================================
-- DUCKDB LOCAL ANALYTICS WAREHOUSE INITIALIZATION
-- Vectorized execution engine reading directly from partitioned Parquet files
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- 1. Create View directly over partitioned Parquet files
CREATE OR REPLACE VIEW analytics.v_raw_events AS 
SELECT 
    event_id::UUID as event_id,
    user_id,
    event_type,
    strptime(event_timestamp, '%Y-%m-%dT%H:%M:%S.%fZ') AS event_timestamp,
    event_date::DATE as event_date,
    platform,
    country,
    product_id,
    amount::DECIMAL(10,2) as amount,
    session_id,
    metadata
FROM read_parquet('data/raw/events/*/*.parquet');

-- 2. DuckDB Dimension User
CREATE OR REPLACE TABLE analytics.dim_user AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY user_id) AS user_sk,
    user_id,
    FIRST(country) AS country,
    FIRST(platform) AS preferred_platform,
    'standard' AS user_tier,
    MIN(event_timestamp) AS valid_from,
    TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
    TRUE AS is_current
FROM analytics.v_raw_events
GROUP BY user_id;

-- 3. DuckDB Fact Events
CREATE OR REPLACE TABLE analytics.fact_events AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY r.event_timestamp) AS event_sk,
    r.event_id,
    r.event_timestamp,
    r.event_date,
    u.user_sk,
    r.user_id,
    r.platform,
    r.country,
    r.event_type,
    r.session_id,
    r.metadata
FROM analytics.v_raw_events r
JOIN analytics.dim_user u ON r.user_id = u.user_id;

-- 4. DuckDB Fact Orders
CREATE OR REPLACE TABLE analytics.fact_orders AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY r.event_timestamp) AS order_sk,
    'ORD_' || r.event_id::VARCHAR AS order_id,
    r.event_id,
    r.event_timestamp AS order_timestamp,
    r.event_date AS order_date,
    u.user_sk,
    r.user_id,
    CAST(SUBSTR(r.product_id, 5) AS INT) AS product_sk,
    r.amount AS order_amount,
    'USD' AS currency,
    json_extract_string(r.metadata, '$.payment_gateway') AS payment_gateway,
    'completed' AS order_status
FROM analytics.v_raw_events r
JOIN analytics.dim_user u ON r.user_id = u.user_id
WHERE r.event_type = 'purchase' AND r.amount > 0;
