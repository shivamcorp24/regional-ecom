"""CLI entry point to generate synthetic data in one-shot or continuous mode."""

import argparse
import time

from services.data_generator.generator import generate_all, generate_incremental_batch, generate_raw_orders


def run_service(
    mode: str = "oneshot",
    records: int = 5000,
    customers: int = 500,
    output_dir: str = "data",
    interval: int = 300,
    seed: int = 42,
) -> None:
    """Runs data generation in one-shot or continuous mode."""
    print("=" * 70)
    print("DATA GENERATOR SERVICE")
    print(f"Mode: {mode} | Output: {output_dir} | Seed: {seed}")
    print("=" * 70)

    if mode == "append":
        print(f"Appended batch: {generate_incremental_batch(records=records, customers=customers, output_dir=output_dir)}")
        return

    if mode == "oneshot":
        summary = generate_all(records=records, customers=customers, output_dir=output_dir, seed=seed)
        print(f"Generated data: {summary}")
        return

    print(f"Running in continuous mode (interval: {interval}s)...")
    batch_num = 1
    current_order_id = 10001 + records  # (continuous mode)

    generate_all(records=records, customers=customers, output_dir=output_dir, seed=seed)
    print(f"Batch {batch_num} completed.")

    try:
        while True:
            time.sleep(interval)
            batch_num += 1
            batch_records = 100
            print(f"Generating batch {batch_num} ({batch_records} orders)...")
            count = generate_raw_orders(
                num_records=batch_records,
                output_path=f"{output_dir}/raw_orders.csv",
                start_order_id=current_order_id,
                append=True,
                duplicate_seed=None,
            )
            current_order_id += batch_records
            print(f"Batch {batch_num} completed ({count} total records).")
    except KeyboardInterrupt:
        print("\nGenerator stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    parser.add_argument("--mode", choices=["oneshot", "append", "continuous"], default="oneshot", help="Execution mode")
    parser.add_argument("--records", type=int, default=5000, help="Order records per batch")
    parser.add_argument("--customers", type=int, default=500, help="Customer master count")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--interval", type=int, default=300, help="Interval in seconds for continuous mode")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()
    run_service(
        mode=args.mode,
        records=args.records,
        customers=args.customers,
        output_dir=args.output_dir,
        interval=args.interval,
        seed=args.seed,
    )
