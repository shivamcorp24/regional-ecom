import os
import random
from datetime import datetime, timedelta
import pandas as pd

# Set fixed seed for reproducible test dataset
random.seed(42)

# Sample master data pools
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
    """Generates dates across different formats to simulate multi-system logging."""
    start_date = datetime(2025, 1, 1)
    random_days = random.randint(0, 365)
    random_seconds = random.randint(0, 86400)
    dt = start_date + timedelta(days=random_days, seconds=random_seconds)

    # Inconsistent formats
    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ]
    return dt.strftime(random.choice(formats))


def generate_customers(num_customers: int = 50, output_path: str = "data/customers.csv") -> None:
    """Generates the customers master CSV matching CUSTOMERS_SCHEMA."""
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
    print(f"Generated {len(df)} customers -> {output_path}")


def generate_products(output_path: str = "data/products.csv") -> None:
    """Generates the products master CSV."""
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
    print(f"Generated {len(df)} products -> {output_path}")



def generate_raw_orders(num_records: int = 950, output_path: str = "data/raw_orders.csv") -> None:
    """Generates a raw dataset with intentional edge cases and writes to CSV."""
    records = []

    for order_id in range(10001, 10001 + num_records):
        customer_id = random.randint(1, 50)
        full_name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        city = random.choice(CITIES)

        product = random.choice(PRODUCTS)
        product_id = product["id"]
        product_name = product["name"] if random.random() > 0.05 else None  # 5% missing names
        category = product["category"]
        
        # Out-of-bounds: 5% chance of negative unit price error
        unit_price = product["price"] if random.random() > 0.05 else -product["price"]

        # Out-of-bounds: 8% chance of negative quantity
        quantity = random.randint(1, 10) if random.random() > 0.08 else random.randint(-5, -1)

        # Missing values: 10% chance of NULL discount
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
            "order_date": order_date
        })

    df = pd.DataFrame(records)

    # Inject exact duplicates (~50 rows)
    duplicates = df.sample(n=min(50, len(df)), random_state=42)
    df = pd.concat([df, duplicates], ignore_index=True)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} raw records (including flaws & duplicates) -> {output_path}")


def generate_all(records: int = 950, customers: int = 50, output_dir: str = "data") -> None:
    """Generates orders, products, and customer files."""
    generate_customers(num_customers=customers, output_path=f"{output_dir}/customers.csv")
    generate_products(f"{output_dir}/products.csv")
    generate_raw_orders(num_records=records, output_path=f"{output_dir}/raw_orders.csv")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce datasets.")
    parser.add_argument("--records", type=int, default=950, help="Number of unique order records to generate (default: 950)")
    parser.add_argument("--customers", type=int, default=50, help="Number of customer master records to generate (default: 50)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility (default: 42)")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory for CSV files (default: data)")
    args = parser.parse_args()

    random.seed(args.seed)
    generate_all(records=args.records, customers=args.customers, output_dir=args.output_dir)