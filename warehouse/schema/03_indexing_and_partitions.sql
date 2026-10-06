-- ==============================================================================
-- INDEXING AND PARTITION PRUNING OPTIMIZATION STRATEGY
-- ==============================================================================

/*
ARCHITECTURAL RATIONALE (Meta Data Engineering Principles):

1. Declarative Range Partitioning by Day:
   - Analytical queries at Meta almost universally filter by time windows (`event_date >= CURRENT_DATE - 30`).
   - By partitioning on `event_date`, PostgreSQL and modern MPP engines perform "Partition Pruning".
   - The query planner skips scanning partitions outside the predicate, turning an O(Total Records) 
     table scan into an O(Daily Partition) scan, reducing I/O by 95%+.

2. BRIN (Block Range Indexing) vs B-Tree on Time Series:
   - For sequentially inserted fact data (where physical page ordering mirrors chronological order),
     BRIN indexes require orders of magnitude less RAM than B-Tree (< 1% of table size).
   - We apply BRIN on `event_timestamp` for intra-day range filtering.

3. Composite B-Tree for Multi-Dimensional Slicing:
   - `(event_type, user_sk)` allows lightning-fast funnel and cohort aggregations.

4. GIN Indexing on Semi-Structured Metadata:
   - JSONB columns store variable client attributes. A GIN index with `jsonb_path_ops` enables
     sub-millisecond lookups on JSON payloads (e.g. `metadata @> '{"payment_gateway": "stripe"}'`).
*/

-- 1. Enable Runtime Partition Pruning
SET enable_partition_pruning = on;

-- 2. BRIN Index for High-Throughput Intra-day Timestamp Filtering
CREATE INDEX IF NOT EXISTS idx_fact_events_ts_brin 
    ON analytics.fact_events USING BRIN (event_timestamp) 
    WITH (pages_per_range = 128);

-- 3. Composite B-Tree Indexes on High-Cardinality Filtering Columns
CREATE INDEX IF NOT EXISTS idx_fact_events_user_type 
    ON analytics.fact_events (user_id, event_type);

CREATE INDEX IF NOT EXISTS idx_fact_events_session 
    ON analytics.fact_events (session_id);

-- 4. GIN Index on JSONB Metadata for Payload Path Queries
CREATE INDEX IF NOT EXISTS idx_fact_events_metadata_gin 
    ON analytics.fact_events USING GIN (metadata jsonb_path_ops);

-- 5. Fact Orders Indexes
CREATE INDEX IF NOT EXISTS idx_fact_orders_user 
    ON analytics.fact_orders (user_sk, order_date);

CREATE INDEX IF NOT EXISTS idx_fact_orders_prod 
    ON analytics.fact_orders (product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_orders_status 
    ON analytics.fact_orders (order_status) 
    WHERE order_status = 'completed';
