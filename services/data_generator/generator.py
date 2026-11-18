"""Generates synthetic customer, product, and order datasets."""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Evan", "Fiona", "George", "Hannah"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", None]

CITY_LOCATION_MAP = {
    "New York": {"state": "NY", "region": "Northeast"},
    "Los Angeles": {"state": "CA", "region": "West"},
    "Chicago": {"state": "IL", "region": "Midwest"},
    "Houston": {"state": "TX", "region": "South"},
    "Phoenix": {"state": "AZ", "region": "West"},
    "Philadelphia": {"state": "PA", "region": "Northeast"},
    "San Antonio": {"state": "TX", "region": "South"},
}

PRODUCTS = [
    {"id": 101, "name": "Wireless Ergonomic Mouse", "category": "Electronics", "price": 29.99},
    {"id": 102, "name": "Mechanical Gaming Keyboard", "category": "Electronics", "price": 89.99},
    {"id": 103, "name": "4K Ultra HD Monitor", "category": "Electronics", "price": 329.50},
    {"id": 104, "name": "Noise Canceling Headphones", "category": "Audio", "price": 199.99},
    {"id": 105, "name": "Standing Desk Mat", "category": "Office", "price": 45.00},
    {"id": 106, "name": "USB-C Hub Multiport Adapter", "category": "Electronics", "price": 24.95},
    {"id": 107, "name": "Leather Office Chair", "category": "Furniture", "price": 249.00},
]


def generate_random_date() -> str:
    """Generates a random date formatted as a string."""
    start_date = datetime(2025, 1, 1)
    random_days = random.randint(0, 365)
    random_seconds = random.randint(0, 86400)
    dt = start_date + timedelta(days=random_days, seconds=random_seconds)

    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ]
    return dt.strftime(random.choice(formats))


def generate_customers(num_customers: int = 50, output_path: str = "data/customers.csv") -> int:
    """Generates customer records and saves to CSV."""
    records = []
    valid_cities = [c for c in CITIES if c is not None]
    for cid in range(1, num_customers + 1):
        city = random.choice(valid_cities)
        loc = CITY_LOCATION_MAP[city]
        records.append({
            "customer_id": cid,
            "first_name": random.choice(FIRST_NAMES),
            "last_name": random.choice(LAST_NAMES),
            "phone": f"+1-555-01{cid:02d}",
            "city": city,
            "state": loc["state"],
            "region": loc["region"],
            "country": "USA",
        })
    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    return len(df)


def generate_products(output_path: str = "data/products.csv") -> int:
    """Generates product records and saves to CSV."""
    records = [
        {
            "product_id": p["id"],
            "product_name": p["name"],
            "category": p["category"],
            "unit_price": p["price"],
        }
        for p in PRODUCTS
    ]
    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    return len(df)


def generate_raw_orders(
    num_records: int = 950,
    output_path: str = "data/raw_orders.csv",
    start_order_id: int = 10001,
    append: bool = False,
    duplicate_seed: int | None = 42,
    num_customers: int = 50,
    duplicate_count: int = 50,
    use_guids: bool = False,
) -> int:
    """Generates raw orders with random data quality issues and saves to CSV."""
    records = []

    for n in range(num_records):
        # GUIDs need no coordination with earlier batches; integer ids continue from start_order_id.
        order_id = str(uuid.uuid4()) if use_guids else start_order_id + n
        customer_id = random.randint(1, num_customers)
        full_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        city = random.choice(CITIES)

        product = random.choice(PRODUCTS)
        product_id = product["id"]
        product_name = product["name"] if random.random() > 0.05 else None
        category = product["category"]

        unit_price = product["price"] if random.random() > 0.05 else -product["price"]
        quantity = random.randint(1, 10) if random.random() > 0.08 else random.randint(-5, -1)
        discount = round(random.uniform(0, 15), 2) if random.random() > 0.10 else None
        order_date = generate_random_date()

        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_full_name": full_name,
            "customer_city": city,
            "product_id": product_id,
            "product_name": product_name,
            "product_category": category,
            "unit_price": unit_price,
            "quantity": quantity,
            "discount": discount,
            "order_date": order_date,
        })

    df = pd.DataFrame(records)

    duplicates = df.sample(n=min(duplicate_count, len(df)), random_state=duplicate_seed)
    df = pd.concat([df, duplicates], ignore_index=True)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    write_header = not (append and os.path.exists(output_path) and os.path.getsize(output_path) > 0)
    df.to_csv(output_path, mode="a" if append else "w", header=write_header, index=False)
    return len(df)


def generate_incremental_batch(
    records: int = 5000,
    customers: int = 500,
    output_dir: str = "data",
) -> dict[str, int]:
    """Appends a fresh batch of orders with GUID order_ids (unseeded, so data differs each call).

    Customers and products are static master data and are only written if missing.
    """
    orders_path = f"{output_dir}/raw_orders.csv"
    if not os.path.exists(f"{output_dir}/customers.csv"):
        generate_customers(num_customers=customers, output_path=f"{output_dir}/customers.csv")
    if not os.path.exists(f"{output_dir}/products.csv"):
        generate_products(output_path=f"{output_dir}/products.csv")

    # Customer ids in the master file bound the ids orders may reference.
    num_customers = int(pd.read_csv(f"{output_dir}/customers.csv", usecols=["customer_id"])["customer_id"].max())
    count = generate_raw_orders(
        num_records=records,
        output_path=orders_path,
        use_guids=True,
        append=True,
        duplicate_seed=None,
        num_customers=num_customers,
        duplicate_count=max(1, records // 20),  # ~5% duplicate rows for Silver to dedupe
    )
    return {"raw_orders_appended": count}


def generate_all(
    records: int = 5000,
    customers: int = 500,
    output_dir: str = "data",
    seed: int = 42,
) -> dict[str, int]:
    """Generates customers, products, and raw orders."""
    random.seed(seed)
    c_count = generate_customers(num_customers=customers, output_path=f"{output_dir}/customers.csv")
    p_count = generate_products(output_path=f"{output_dir}/products.csv")
    o_count = generate_raw_orders(num_records=records, output_path=f"{output_dir}/raw_orders.csv", num_customers=customers)
    return {"customers": c_count, "products": p_count, "raw_orders": o_count}
