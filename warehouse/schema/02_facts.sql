-- ==============================================================================
-- DIMENSIONAL STAR SCHEMA: FACT TABLES WITH DECLARATIVE PARTITIONING
-- Target: PostgreSQL 14+ (Range Partitioning by Date)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. fact_events: High-Volume Granular Interaction Events
-- Grain: One record per user interaction (signup, login, post, like, purchase click).
-- Partition Key: event_date (daily range partitioning)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_events (
    event_sk            BIGSERIAL,
    event_id            UUID NOT NULL,
    event_timestamp     TIMESTAMPTZ NOT NULL,
    event_date          DATE NOT NULL,
    user_sk             BIGINT NOT NULL,                  -- FK to analytics.dim_user(user_sk)
    user_id             VARCHAR(32) NOT NULL,             -- Denormalized for high-speed cohort scan
    platform            VARCHAR(16) NOT NULL,
    country             VARCHAR(4) NOT NULL,
    event_type          VARCHAR(32) NOT NULL,             -- signup, login, post, like, purchase
    session_id          VARCHAR(64),
    metadata            JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_date, event_sk)
) PARTITION BY RANGE (event_date);

-- Composite unique constraint on partition key + event_id
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_events_id 
    ON analytics.fact_events (event_date, event_id);


-- ------------------------------------------------------------------------------
-- 2. fact_orders: Monetary Transaction Fact Table
-- Grain: One record per completed purchase/order transaction.
-- Partition Key: order_date (daily range partitioning)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_sk            BIGSERIAL,
    order_id            VARCHAR(64) NOT NULL,
    event_id            UUID,                             -- Link back to triggering event
    order_timestamp     TIMESTAMPTZ NOT NULL,
    order_date          DATE NOT NULL,
    user_sk             BIGINT NOT NULL,                  -- FK to analytics.dim_user
    user_id             VARCHAR(32) NOT NULL,
    product_sk          INT NOT NULL,                     -- FK to analytics.dim_product
    order_amount        NUMERIC(10, 2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'USD',
    payment_gateway     VARCHAR(32) NOT NULL,             -- stripe, apple_pay, google_pay, meta_pay
    order_status        VARCHAR(20) DEFAULT 'completed',  -- completed, refunded, disputed
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (order_date, order_sk)
) PARTITION BY RANGE (order_date);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_orders_id 
    ON analytics.fact_orders (order_date, order_id);


-- ------------------------------------------------------------------------------
-- Sample Automated Partition Provisioning (Past 7 days + Next 3 days)
-- In production, an Airflow maintenance task or pg_partman creates partitions.
-- ------------------------------------------------------------------------------
DO $$
DECLARE
    curr_d DATE;
    start_d DATE := CURRENT_DATE - INTERVAL '30 days';
    end_d DATE := CURRENT_DATE + INTERVAL '5 days';
    tbl_name TEXT;
BEGIN
    curr_d := start_d;
    WHILE curr_d <= end_d LOOP
        -- fact_events partitions
        tbl_name := 'fact_events_' || to_char(curr_d, 'YYYY_MM_DD');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS analytics.%I PARTITION OF analytics.fact_events 
             FOR VALUES FROM (%L) TO (%L);',
            tbl_name,
            curr_d,
            curr_d + INTERVAL '1 day'
        );

        -- fact_orders partitions
        tbl_name := 'fact_orders_' || to_char(curr_d, 'YYYY_MM_DD');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS analytics.%I PARTITION OF analytics.fact_orders 
             FOR VALUES FROM (%L) TO (%L);',
            tbl_name,
            curr_d,
            curr_d + INTERVAL '1 day'
        );

        curr_d := curr_d + INTERVAL '1 day';
    END LOOP;
END $$;
