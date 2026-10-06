"""
Product Analytics Data Generator
Simulates realistic Meta-scale product interaction logs (signup, login, post, like, purchase)
with power-law activity distribution and partitioned Parquet/JSON output.
"""

import os
import sys
import uuid
import random
import argparse
from datetime import datetime, timedelta
from typing import Generator, List, Dict, Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from tqdm import tqdm

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

EVENT_TYPES = ["signup", "login", "post", "like", "purchase"]
EVENT_WEIGHTS = [0.03, 0.35, 0.17, 0.40, 0.05]  # Meta engagement profile
PLATFORMS = ["ios", "android", "web"]
PLATFORM_WEIGHTS = [0.48, 0.42, 0.10]
COUNTRIES = ["US", "CA", "GB", "DE", "IN", "BR", "JP", "FR"]
COUNTRY_WEIGHTS = [0.30, 0.08, 0.12, 0.07, 0.22, 0.11, 0.05, 0.05]
PRODUCT_CATALOG = [
    {"product_id": f"PRD_{i:04d}", "category": cat, "base_price": price}
    for i, (cat, price) in enumerate([
        ("digital_goods", 4.99), ("digital_goods", 9.99), ("subscription", 14.99),
        ("merchandise", 29.99), ("digital_goods", 1.99), ("tipping", 5.00),
        ("creator_pass", 19.99), ("verified_badge", 11.99), ("promoted_post", 50.00),
        ("ad_credit", 100.00)
    ], start=1)
]


class SyntheticDataGenerator:
    def __init__(
        self,
        num_users: int = 50_000,
        total_records: int = 1_000_000,
        days_history: int = 30,
        output_dir: str = "data/raw",
        output_format: str = "parquet",
        batch_size: int = 100_000,
    ):
        self.num_users = num_users
        self.total_records = total_records
        self.days_history = days_history
        self.output_dir = output_dir
        self.output_format = output_format.lower()
        self.batch_size = batch_size
        self.start_date = datetime.utcnow() - timedelta(days=days_history)

        # Precompute user profiles
        print(f"[*] Initializing {self.num_users:,} mock user profiles...")
        self.users = self._generate_user_pool()

    def _generate_user_pool(self) -> List[Dict[str, Any]]:
        users = []
        for i in range(1, self.num_users + 1):
            account_created = self.start_date + timedelta(
                days=random.uniform(0, self.days_history * 0.8)
            )
            users.append({
                "user_id": f"USR_{i:07d}",
                "country": np.random.choice(COUNTRIES, p=COUNTRY_WEIGHTS),
                "created_at": account_created,
                "preferred_platform": np.random.choice(PLATFORMS, p=PLATFORM_WEIGHTS),
                # Power-law activity skew (Pareto distribution)
                "activity_skew": np.random.pareto(a=2.0) + 1.0,
            })
        return users

    def generate_batch(self, count: int) -> List[Dict[str, Any]]:
        """Generates a batch of event dictionaries using vectorized selection."""
        # Select users weighted by activity_skew (power-law: few power users generate most traffic)
        skews = np.array([u["activity_skew"] for u in self.users])
        probs = skews / skews.sum()
        chosen_user_indices = np.random.choice(len(self.users), size=count, p=probs)

        events_batch = []
        for idx in chosen_user_indices:
            user = self.users[idx]
            event_type = np.random.choice(EVENT_TYPES, p=EVENT_WEIGHTS)

            # Ensure event timestamp occurs after user account creation
            min_offset = (user["created_at"] - self.start_date).total_seconds()
            max_offset = self.days_history * 86400
            if min_offset >= max_offset:
                event_time = user["created_at"]
            else:
                event_sec = random.uniform(min_offset, max_offset)
                event_time = self.start_date + timedelta(seconds=event_sec)

            # Metadata context based on event type
            metadata: Dict[str, Any] = {
                "client_version": random.choice(["v14.2.0", "v14.3.1", "v15.0.0"]),
                "ip_address": fake.ipv4_public(),
            }

            product_id = None
            amount = 0.0

            if event_type == "purchase":
                product = random.choice(PRODUCT_CATALOG)
                product_id = product["product_id"]
                # Slight variation in price (discounts/taxes)
                amount = round(product["base_price"] * random.uniform(0.9, 1.1), 2)
                metadata["payment_gateway"] = random.choice(["stripe", "apple_pay", "google_pay", "meta_pay"])
            elif event_type == "post":
                metadata["post_type"] = random.choice(["text", "image", "reel", "story"])
                metadata["media_count"] = random.randint(0, 5)
            elif event_type == "like":
                metadata["target_type"] = random.choice(["post", "comment", "reel"])
                metadata["target_author_id"] = f"USR_{random.randint(1, self.num_users):07d}"
            elif event_type == "login":
                metadata["auth_method"] = random.choice(["password", "sso_google", "biometric", "2fa_sms"])
            elif event_type == "signup":
                metadata["referral_channel"] = random.choice(["organic", "fb_ads", "influencer", "app_store"])

            record = {
                "event_id": str(uuid.uuid4()),
                "user_id": user["user_id"],
                "event_type": event_type,
                "event_timestamp": event_time.isoformat() + "Z",
                "event_date": event_time.strftime("%Y-%m-%d"),
                "platform": user["preferred_platform"] if random.random() < 0.85 else np.random.choice(PLATFORMS),
                "country": user["country"],
                "product_id": product_id,
                "amount": float(amount),
                "session_id": f"SES_{random.randint(1000000, 9999999)}",
                "metadata": metadata,
            }
            events_batch.append(record)

        return events_batch

    def run(self):
        """Execute stream-writing partitioned by event_date."""
        os.makedirs(self.output_dir, exist_ok=True)
        num_batches = (self.total_records + self.batch_size - 1) // self.batch_size
        print(f"[*] Commencing generation of {self.total_records:,} events in {num_batches} batches...")

        date_partitions: Dict[str, List[Dict[str, Any]]] = {}

        for b in tqdm(range(num_batches), desc="Generating event records"):
            records_in_batch = min(self.batch_size, self.total_records - (b * self.batch_size))
            batch_data = self.generate_batch(records_in_batch)

            # Group into date partitions
            for row in batch_data:
                d = row["event_date"]
                if d not in date_partitions:
                    date_partitions[d] = []
                date_partitions[d].append(row)

            # Flush large date buffers to avoid RAM pressure
            if (b + 1) % 2 == 0 or (b + 1) == num_batches:
                self._flush_partitions(date_partitions)
                date_partitions.clear()

        print(f"[+] Generation complete. Files written to {self.output_dir}")

    def _flush_partitions(self, date_partitions: Dict[str, List[Dict[str, Any]]]):
        for date_key, rows in date_partitions.items():
            partition_path = os.path.join(self.output_dir, f"date={date_key}")
            os.makedirs(partition_path, exist_ok=True)
            file_id = uuid.uuid4().hex[:8]

            if self.output_format == "parquet":
                file_path = os.path.join(partition_path, f"events_{file_id}.parquet")
                # Flatten metadata JSON string for parquet portability
                flat_rows = []
                for r in rows:
                    r_copy = r.copy()
                    import json
                    r_copy["metadata"] = json.dumps(r_copy["metadata"])
                    flat_rows.append(r_copy)

                table = pa.Table.from_pylist(flat_rows)
                pq.write_table(table, file_path, compression="SNAPPY")
            else:
                import json
                file_path = os.path.join(partition_path, f"events_{file_id}.json")
                with open(file_path, "a", encoding="utf-8") as f:
                    for r in rows:
                        f.write(json.dumps(r) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Meta Product Analytics Synthetic Data Generator")
    parser.add_argument("--records", type=int, default=1_000_000, help="Number of records to generate (default: 1,000,000)")
    parser.add_argument("--users", type=int, default=50_000, help="Unique user pool size (default: 50,000)")
    parser.add_argument("--days", type=int, default=30, help="Days of history to simulate (default: 30)")
    parser.add_argument("--format", type=str, default="parquet", choices=["parquet", "json"], help="Output format")
    parser.add_argument("--outdir", type=str, default="data/raw/events", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=100_000, help="Batch generation size")
    args = parser.parse_args()

    generator = SyntheticDataGenerator(
        num_users=args.users,
        total_records=args.records,
        days_history=args.days,
        output_dir=args.outdir,
        output_format=args.format,
        batch_size=args.batch_size,
    )
    generator.run()


if __name__ == "__main__":
    main()
