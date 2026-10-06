-- ==============================================================================
-- DIMENSIONAL STAR SCHEMA: DIMENSION TABLES
-- Target: PostgreSQL 14+ / Compatible with Modern Data Warehouses
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ------------------------------------------------------------------------------
-- 1. dim_date: Standard Gregorian Calendar Dimension
-- Grain: One record per calendar day. Enables fast joins and precomputed calendar hierarchies.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id             INT PRIMARY KEY,                -- Format: YYYYMMDD (e.g., 20261006)
    calendar_date       DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL,
    month               SMALLINT NOT NULL,
    month_name          VARCHAR(12) NOT NULL,
    day_of_month        SMALLINT NOT NULL,
    day_of_week         SMALLINT NOT NULL,              -- 1 (Monday) to 7 (Sunday)
    day_name            VARCHAR(12) NOT NULL,
    week_of_year        SMALLINT NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_holiday          BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_dim_date_calendar_date ON analytics.dim_date (calendar_date);


-- ------------------------------------------------------------------------------
-- 2. dim_user (SCD Type-2): Slowly Changing Dimension for User Profile History
-- Captures profile migrations (e.g., tier upgrades, platform changes, country relocation)
-- Grain: One record per user profile revision interval.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_user (
    user_sk             BIGSERIAL PRIMARY KEY,          -- Surrogate Key
    user_id             VARCHAR(32) NOT NULL,           -- Natural / Business Key
    country             VARCHAR(4) NOT NULL,
    preferred_platform  VARCHAR(16) NOT NULL,           -- ios, android, web
    user_tier           VARCHAR(20) DEFAULT 'standard', -- standard, verified, creator, vip
    signup_channel      VARCHAR(32),
    valid_from          TIMESTAMPTZ NOT NULL,           -- SCD Type-2 effective start
    valid_to            TIMESTAMPTZ NOT NULL DEFAULT '9999-12-31 23:59:59+00', -- SCD-2 effective end
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,  -- Active record flag
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optimize SCD lookups: active version lookup vs historical point-in-time joins
CREATE INDEX IF NOT EXISTS idx_dim_user_active_lookup 
    ON analytics.dim_user (user_id) 
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_user_pit 
    ON analytics.dim_user (user_id, valid_from, valid_to);


-- ------------------------------------------------------------------------------
-- 3. dim_product: Product & Service Catalog Dimension
-- Grain: One record per discrete digital or physical product sku.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_sk          SERIAL PRIMARY KEY,             -- Surrogate Key
    product_id          VARCHAR(32) NOT NULL UNIQUE,    -- Natural Key (PRD_xxxx)
    product_name        VARCHAR(100) NOT NULL,
    category            VARCHAR(50) NOT NULL,           -- digital_goods, subscription, ad_credit
    base_price          NUMERIC(10, 2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'USD',
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_product_cat ON analytics.dim_product (category);
