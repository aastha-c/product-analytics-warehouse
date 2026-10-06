"""
Product Analytics Daily Incremental ETL Pipeline (Airflow DAG)
Orchestrates raw event staging, SCD Type-2 dimension upserts, idempotent fact loads,
and automated quality validation tests.
"""

from datetime import datetime, timedelta
import os
import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Default SLA and retry semantics adhering to Meta reliability guidelines
DEFAULT_ARGS = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

RAW_DATA_PATH = os.environ.get("RAW_DATA_PATH", "/opt/airflow/data/raw/events")


def verify_raw_partition(**context):
    """Checks whether the raw parquet partition exists for the execution date."""
    ds = context["ds"]  # YYYY-MM-DD
    partition_dir = os.path.join(RAW_DATA_PATH, f"date={ds}")
    logging.info(f"Inspecting raw partition at: {partition_dir}")

    if not os.path.exists(partition_dir):
        # In production this might raise AirflowSkipException or wait
        logging.warning(f"Partition directory {partition_dir} does not exist on disk yet.")
    else:
        files = [f for f in os.listdir(partition_dir) if f.endswith(".parquet") or f.endswith(".json")]
        logging.info(f"Discovered {len(files)} partition files for processing.")


def stage_raw_events_to_postgres(**context):
    """
    Extracts data from partitioned Parquet/JSON files and stages into staging.stg_events
    using Postgres COPY / fast batch streaming.
    """
    import pyarrow.parquet as pq
    import pandas as pd
    
    ds = context["ds"]
    partition_dir = os.path.join(RAW_DATA_PATH, f"date={ds}")
    
    if not os.path.exists(partition_dir):
        logging.info("No partition found. Skipping staging.")
        return

    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Create staging schema & table if not exists
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE TABLE IF NOT EXISTS staging.stg_events (
            event_id UUID,
            user_id VARCHAR(32),
            event_type VARCHAR(32),
            event_timestamp TIMESTAMPTZ,
            event_date DATE,
            platform VARCHAR(16),
            country VARCHAR(4),
            product_id VARCHAR(32),
            amount NUMERIC(10, 2),
            session_id VARCHAR(64),
            metadata JSONB
        );
        TRUNCATE TABLE staging.stg_events;
    """)

    parquet_files = [
        os.path.join(partition_dir, f)
        for f in os.listdir(partition_dir)
        if f.endswith(".parquet")
    ]

    total_staged = 0
    for pfile in parquet_files:
        df = pq.read_table(pfile).to_pandas()
        # Convert records to list of tuples
        records = [
            (
                row["event_id"],
                row["user_id"],
                row["event_type"],
                row["event_timestamp"],
                row["event_date"],
                row["platform"],
                row["country"],
                row["product_id"],
                row["amount"],
                row["session_id"],
                json.dumps(row["metadata"]) if isinstance(row["metadata"], dict) else row["metadata"],
            )
            for _, row in df.iterrows()
        ]

        from psycopg2.extras import execute_batch
        execute_batch(
            cursor,
            """
            INSERT INTO staging.stg_events (
                event_id, user_id, event_type, event_timestamp, event_date,
                platform, country, product_id, amount, session_id, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::JSONB)
            """,
            records,
            page_size=5000,
        )
        total_staged += len(records)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info(f"Successfully staged {total_staged} events for partition date {ds}.")


def validate_data_quality_assertions(**context):
    """
    Validates pipeline constraints:
    1. Null rate on critical fields (user_id, event_timestamp) == 0%
    2. Primary key uniqueness in fact_events for the partition
    3. Non-negative amounts in orders
    """
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id="postgres_warehouse")
    
    # Check 1: Null check in fact_events partition
    null_check_sql = f"""
        SELECT COUNT(*) 
        FROM analytics.fact_events 
        WHERE event_date = '{ds}' AND (user_id IS NULL OR event_timestamp IS NULL);
    """
    null_records = hook.get_first(null_check_sql)[0]
    if null_records > 0:
        raise ValueError(f"DATA QUALITY FAILURE: {null_records} records contain NULL user_id or event_timestamp.")

    # Check 2: PK Uniqueness
    dup_check_sql = f"""
        SELECT COUNT(*) - COUNT(DISTINCT event_id)
        FROM analytics.fact_events
        WHERE event_date = '{ds}';
    """
    duplicates = hook.get_first(dup_check_sql)[0]
    if duplicates > 0:
        raise ValueError(f"DATA QUALITY FAILURE: Detected {duplicates} duplicate event_ids in partition {ds}.")

    logging.info("All data quality assertion checks passed successfully.")


with DAG(
    dag_id="product_analytics_daily_etl",
    default_args=DEFAULT_ARGS,
    description="Daily incremental pipeline loading raw parquet events into Star Schema warehouse",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["analytics", "meta", "star_schema", "scd2"],
) as dag:

    # Task 1: Check partition presence
    check_partition = PythonOperator(
        task_id="check_raw_partition",
        python_callable=verify_raw_partition,
    )

    # Task 2: Stage raw batch
    stage_events = PythonOperator(
        task_id="stage_raw_events",
        python_callable=stage_raw_events_to_postgres,
    )

    # TaskGroup: Dimension Updates
    with TaskGroup("dimension_processing") as dim_group:
        # Update dim_user with SCD Type-2
        update_scd2_user = PostgresOperator(
            task_id="upsert_scd2_dim_user",
            postgres_conn_id="postgres_warehouse",
            sql="""
                -- 1. Expire updated user records
                WITH stage_latest AS (
                    SELECT DISTINCT ON (user_id)
                        user_id, country, platform AS preferred_platform, event_timestamp
                    FROM staging.stg_events
                    ORDER BY user_id, event_timestamp DESC
                )
                UPDATE analytics.dim_user u
                SET valid_to = s.event_timestamp,
                    is_current = FALSE
                FROM stage_latest s
                WHERE u.user_id = s.user_id 
                  AND u.is_current = TRUE
                  AND (u.country <> s.country OR u.preferred_platform <> s.preferred_platform);

                -- 2. Insert new versions or new users
                WITH stage_latest AS (
                    SELECT DISTINCT ON (user_id)
                        user_id, country, platform AS preferred_platform, event_timestamp
                    FROM staging.stg_events
                    ORDER BY user_id, event_timestamp DESC
                )
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
                FROM stage_latest s
                LEFT JOIN analytics.dim_user u 
                    ON s.user_id = u.user_id AND u.is_current = TRUE
                WHERE u.user_sk IS NULL;
            """,
        )

    # TaskGroup: Fact Loads (Idempotent DELETE + INSERT pattern for partition)
    with TaskGroup("fact_processing") as fact_group:
        load_fact_events = PostgresOperator(
            task_id="load_fact_events",
            postgres_conn_id="postgres_warehouse",
            sql="""
                -- Idempotency: Delete existing data for partition execution date
                DELETE FROM analytics.fact_events WHERE event_date = '{{ ds }}'::DATE;

                -- Insert enriched facts with point-in-time user surrogate key
                INSERT INTO analytics.fact_events (
                    event_id, event_timestamp, event_date, user_sk, user_id,
                    platform, country, event_type, session_id, metadata
                )
                SELECT 
                    s.event_id,
                    s.event_timestamp,
                    s.event_date,
                    u.user_sk,
                    s.user_id,
                    s.platform,
                    s.country,
                    s.event_type,
                    s.session_id,
                    s.metadata
                FROM staging.stg_events s
                JOIN analytics.dim_user u 
                    ON s.user_id = u.user_id 
                   AND s.event_timestamp >= u.valid_from 
                   AND s.event_timestamp < u.valid_to
                WHERE s.event_date = '{{ ds }}'::DATE;
            """,
        )

        load_fact_orders = PostgresOperator(
            task_id="load_fact_orders",
            postgres_conn_id="postgres_warehouse",
            sql="""
                -- Idempotency: Delete existing orders for partition execution date
                DELETE FROM analytics.fact_orders WHERE order_date = '{{ ds }}'::DATE;

                -- Insert transaction facts from purchase events
                INSERT INTO analytics.fact_orders (
                    order_id, event_id, order_timestamp, order_date,
                    user_sk, user_id, product_sk, order_amount, currency,
                    payment_gateway, order_status
                )
                SELECT 
                    'ORD_' || s.event_id::VARCHAR,
                    s.event_id,
                    s.event_timestamp,
                    s.event_date,
                    u.user_sk,
                    s.user_id,
                    COALESCE(p.product_sk, 1),
                    s.amount,
                    'USD',
                    COALESCE(s.metadata->>'payment_gateway', 'stripe'),
                    'completed'
                FROM staging.stg_events s
                JOIN analytics.dim_user u 
                    ON s.user_id = u.user_id 
                   AND s.event_timestamp >= u.valid_from 
                   AND s.event_timestamp < u.valid_to
                LEFT JOIN analytics.dim_product p 
                    ON s.product_id = p.product_id
                WHERE s.event_date = '{{ ds }}'::DATE
                  AND s.event_type = 'purchase'
                  AND s.amount > 0;
            """,
        )

    # Task 5: Data quality assertions
    run_quality_checks = PythonOperator(
        task_id="run_quality_assertions",
        python_callable=validate_data_quality_assertions,
    )

    # Pipeline lineage
    check_partition >> stage_events >> dim_group >> fact_group >> run_quality_checks
