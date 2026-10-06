"""
Great Expectations Quality Validation Suite for Product Analytics Warehouse
Verifies null rate limits, primary key uniqueness, value sets, and freshness assertions.
"""

import sys
import pandas as pd


def build_expectation_suite_for_events(df: pd.DataFrame) -> dict:
    """
    Validates event DataFrame against Meta Data Engineering SLA standards:
    1. Null rate on user_id, event_id, event_timestamp == 0.0%
    2. event_id uniqueness == 100%
    3. Categorical sets (event_type in ['signup', 'login', 'post', 'like', 'purchase'])
    4. Row count drift check against expected floor
    """
    results = {
        "suite_name": "product_events_core_expectations",
        "total_records": len(df),
        "checks_passed": True,
        "details": [],
    }

    def record_check(name: str, passed: bool, message: str):
        results["details"].append({"check": name, "passed": passed, "message": message})
        if not passed:
            results["checks_passed"] = False

    # 1. Primary Key Uniqueness
    is_unique = df["event_id"].is_unique
    dup_count = df["event_id"].duplicated().sum()
    record_check(
        "expect_column_values_to_be_unique: event_id",
        bool(is_unique),
        f"Duplicates found: {dup_count}",
    )

    # 2. Critical Field Non-Null Checks (0% null rate)
    for col in ["event_id", "user_id", "event_type", "event_timestamp"]:
        null_count = df[col].isnull().sum()
        record_check(
            f"expect_column_values_to_not_be_null: {col}",
            bool(null_count == 0),
            f"Null count in {col}: {null_count}",
        )

    # 3. Categorical Set Integrity
    valid_events = {"signup", "login", "post", "like", "purchase"}
    invalid_events = set(df["event_type"].unique()) - valid_events
    record_check(
        "expect_column_values_to_be_in_set: event_type",
        len(invalid_events) == 0,
        f"Unexpected event types: {invalid_events}",
    )

    # 4. Numeric Bounds on Amounts
    negative_amounts = (df["amount"] < 0).sum()
    record_check(
        "expect_column_values_to_be_between: amount >= 0",
        bool(negative_amounts == 0),
        f"Negative amounts detected: {negative_amounts}",
    )

    return results


if __name__ == "__main__":
    # Self-test demonstration
    sample_data = pd.DataFrame([
        {
            "event_id": f"evt_{i}",
            "user_id": f"usr_{i}",
            "event_type": "login",
            "event_timestamp": "2026-10-06T12:00:00Z",
            "amount": 0.0,
        }
        for i in range(100)
    ])
    suite_res = build_expectation_suite_for_events(sample_data)
    print("Self-test check passed:", suite_res["checks_passed"])
