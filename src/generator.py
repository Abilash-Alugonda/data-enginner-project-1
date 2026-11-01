"""Synthetic dataset generator for e-commerce orders, customers, and products.

Produces byte-identical datasets for a given seed across runs.
Includes normal rows, boundary values, updates, schema anomalies, and malformed records.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
from typing import Any

STATUS_FLOW = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]

COUNTRIES = ["USA", "CAN", "GBR", "DEU", "FRA", "JPN", "AUS"]

CATEGORIES = [
    ("Electronics", 299.99),
    ("Books", 19.99),
    ("Apparel", 49.99),
    ("Home", 89.99),
    ("Beauty", 24.99),
    ("Sports", 59.99),
]


def generate_customers(rng: random.Random, count: int) -> list[dict[str, Any]]:
    """Generate deterministic customer records."""
    customers = []
    first_names = [
        "Alex",
        "Jordan",
        "Taylor",
        "Morgan",
        "Sam",
        "Chris",
        "Pat",
        "Robin",
        "Avery",
        "Casey",
        "Riley",
        "Jamie",
        "Logan",
        "Dana",
        "Cameron",
    ]
    last_names = [
        "Smith",
        "Johnson",
        "Williams",
        "Brown",
        "Jones",
        "Garcia",
        "Miller",
        "Davis",
        "Rodriguez",
        "Martinez",
        "Hernandez",
        "Lopez",
        "Gonzalez",
    ]
    for i in range(1, count + 1):
        cust_id = f"CUST-{i:05d}"
        fn = first_names[(i * 3 + rng.randint(0, 100)) % len(first_names)]
        ln = last_names[(i * 7 + rng.randint(0, 100)) % len(last_names)]
        name = f"{fn} {ln}"
        email = f"{fn.lower()}.{ln.lower()}{i}@example.com"
        country = COUNTRIES[i % len(COUNTRIES)]
        day = (i % 28) + 1
        created_at = f"2025-11-{day:02d}T08:00:00Z"
        customers.append(
            {
                "customer_id": cust_id,
                "name": name,
                "email": email,
                "country": country,
                "created_at": created_at,
            }
        )
    return customers


def generate_products(rng: random.Random, count: int) -> list[dict[str, Any]]:
    """Generate deterministic product records."""
    products = []
    for i in range(1, count + 1):
        prod_id = f"PROD-{i:05d}"
        cat_name, base_price = CATEGORIES[(i - 1) % len(CATEGORIES)]
        prod_name = f"{cat_name} Item {i}"
        price_offset = round(((i * 13) % 25) * 1.5, 2)
        unit_price = round(base_price + price_offset, 2)
        products.append(
            {
                "product_id": prod_id,
                "product_name": prod_name,
                "category": cat_name,
                "unit_price": unit_price,
            }
        )
    return products


def generate_dataset(
    total_rows: int = 100000,
    seed: int = 42,
    out_dir: str | Path = "data/sample",
) -> dict[str, int]:
    """Generate complete synthetic dataset with deterministic pseudo-random stream."""
    rng = random.Random(seed)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    # Scale entity counts based on total_rows
    if total_rows <= 300:
        # Starter sample mode (about 200 rows)
        num_customers = 25
        num_products = 15
        num_initial = 140
        num_updates = 40
        num_schema_change = 15
        num_malformed = 5
    else:
        num_customers = max(50, total_rows // 20)
        num_products = max(20, total_rows // 200)
        num_malformed = max(10, int(total_rows * 0.02))
        num_schema_change = max(10, int(total_rows * 0.03))
        num_updates = max(20, int(total_rows * 0.15))
        num_initial = total_rows - num_updates - num_schema_change - num_malformed

    customers = generate_customers(rng, num_customers)
    products = generate_products(rng, num_products)

    # Write customers.csv
    cust_file = out_path / "customers.csv"
    with open(cust_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["customer_id", "name", "email", "country", "created_at"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(customers)

    # Write products.csv
    prod_file = out_path / "products.csv"
    with open(prod_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["product_id", "product_name", "category", "unit_price"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(products)

    # Generate initial orders
    initial_orders: list[dict[str, Any]] = []
    # Dates between 2026-01-01 and 2026-01-10 (or up to 2026-01-20 for larger sets)
    date_range_days = 10 if total_rows <= 300 else 30

    for idx in range(1, num_initial + 1):
        order_id = f"ORD-{idx:06d}"
        cust = customers[idx % len(customers)]
        prod = products[(idx * 3) % len(products)]
        day = (idx % date_range_days) + 1
        order_date = f"2026-01-{day:02d}"
        hour = (idx * 2) % 24
        minute = (idx * 7) % 60
        second = (idx * 11) % 60
        updated_at = f"{order_date}T{hour:02d}:{minute:02d}:{second:02d}Z"
        status = "PENDING"
        quantity = ((idx * 3 + rng.randint(1, 7)) % 5) + 1
        unit_price = prod["unit_price"]
        order_amount = round(quantity * unit_price, 2)

        record: dict[str, Any] = {
            "order_id": order_id,
            "customer_id": cust["customer_id"],
            "product_id": prod["product_id"],
            "quantity": quantity,
            "order_amount": order_amount,
            "order_status": status,
            "order_date": order_date,
            "updated_at": updated_at,
        }

        # Inject specific edge cases at deterministic indices
        if idx == 1:
            # Boundary value: quantity 0, amount 0.00
            record["quantity"] = 0
            record["order_amount"] = 0.00
        elif idx == 2:
            # Boundary value: quantity 9999, high amount
            record["quantity"] = 9999
            record["order_amount"] = 99990.00
        elif idx == 3:
            # Invalid foreign key: customer_id
            record["customer_id"] = "CUST-99999"
        elif idx == 4:
            # Invalid foreign key: product_id
            record["product_id"] = "PROD-99999"
        elif idx == 5:
            # Missing required field: customer_id is empty/null
            record["customer_id"] = ""
        elif idx == 6:
            # Missing required field: order_id is empty/null
            record["order_id"] = ""
        elif idx == 7:
            # Business rule violation: negative quantity
            record["quantity"] = -3
        elif idx == 8:
            # Business rule violation: negative order_amount
            record["order_amount"] = -15.50
        elif idx == 9:
            # Invalid status
            record["order_status"] = "INVALID_STATUS_CODE"
        elif idx == 10:
            # Invalid date format
            record["order_date"] = "2026/01/01"

        initial_orders.append(record)

    # Write orders_initial.jsonl in buffered chunks
    init_file = out_path / "orders_initial.jsonl"
    with open(init_file, "w", encoding="utf-8", newline="\n") as f:
        buffer: list[str] = []
        for rec in initial_orders:
            buffer.append(json.dumps(rec, separators=(",", ":")) + "\n")
            if len(buffer) >= 10000:
                f.writelines(buffer)
                buffer.clear()
        if buffer:
            f.writelines(buffer)
            buffer.clear()

    # Generate updates (CSV format)
    update_orders: list[dict[str, Any]] = []
    # Map initial orders by id for O(1) lookup (valid base orders only)
    initial_map = {
        r["order_id"]: r
        for r in initial_orders
        if r.get("order_id")
        and "-" in r.get("order_date", "")
        and len(r.get("order_date", "").split("-")) == 3
        and r["customer_id"].startswith("CUST-")
    }
    valid_initial_ids = sorted(initial_map.keys())

    for u_idx in range(1, num_updates + 1):
        target_order_id = valid_initial_ids[(u_idx * 5) % len(valid_initial_ids)]
        base_row = initial_map[target_order_id]

        # Determine updated status
        status = "PROCESSING" if u_idx % 3 == 0 else ("SHIPPED" if u_idx % 3 == 1 else "DELIVERED")
        # Late-arriving date: updated 5 days after base order_date
        base_day = int(base_row["order_date"].split("-")[2])
        update_day = min(base_day + 5, 28)
        updated_at = f"2026-01-{update_day:02d}T14:30:00Z"

        record = {
            "order_id": target_order_id,
            "customer_id": base_row["customer_id"],
            "product_id": base_row["product_id"],
            "quantity": base_row["quantity"],
            "order_amount": base_row["order_amount"],
            "order_status": status,
            "order_date": base_row["order_date"],
            "updated_at": updated_at,
        }

        # Specific edge cases for updates
        if u_idx == 1:
            # Domain edge case 1: order cancelled after being shipped
            record["order_status"] = "CANCELLED"
            record["updated_at"] = "2026-01-25T16:00:00Z"
        elif u_idx == 2:
            # Domain edge case 2: quantity zero with nonzero amount (e.g. warranty / fee)
            record["quantity"] = 0
            record["order_amount"] = 35.00
            record["order_status"] = "SHIPPED"
            record["updated_at"] = "2026-01-22T10:15:00Z"
        elif u_idx == 3:
            # Out-of-order update: updated_at is OLDER than the base order's updated_at
            # Base order was e.g. at 2026-01-02T12:00:00Z; this one claims 2025-12-31
            record["order_status"] = "PENDING"
            record["updated_at"] = "2025-12-31T23:59:59Z"
        elif u_idx == 4:
            # Duplicate order_id within the update file itself
            # We will emit another record right after with a newer updated_at
            record["order_status"] = "PROCESSING"
            record["updated_at"] = "2026-01-20T09:00:00Z"
        elif u_idx == 5:
            # Complement to u_idx == 4: same order_id, later updated_at
            record["order_id"] = update_orders[3]["order_id"]
            record["order_status"] = "DELIVERED"
            record["updated_at"] = "2026-01-20T18:00:00Z"

        update_orders.append(record)

    # Write orders_updates.csv
    update_file = out_path / "orders_updates.csv"
    with open(update_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "order_amount",
                "order_status",
                "order_date",
                "updated_at",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(update_orders)

    # Generate unexpected schema change file
    # Extra column and renamed column
    schema_change_rows: list[dict[str, Any]] = []
    for s_idx in range(1, num_schema_change + 1):
        s_id = f"ORD-SC-{s_idx:04d}"
        schema_change_rows.append(
            {
                "order_number": s_id,  # Renamed from order_id
                "customer_id": customers[0]["customer_id"],
                "product_id": products[0]["product_id"],
                "quantity": 1,
                "order_amount": 49.99,
                "order_status": "PENDING",
                "order_date": "2026-01-05",
                "updated_at": "2026-01-05T12:00:00Z",
                "unexpected_extra_column": "promo_code_xyz",
            }
        )

    schema_file = out_path / "orders_schema_change.csv"
    with open(schema_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "order_number",
                "customer_id",
                "product_id",
                "quantity",
                "order_amount",
                "order_status",
                "order_date",
                "updated_at",
                "unexpected_extra_column",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(schema_change_rows)

    # Generate malformed lines file
    malformed_file = out_path / "orders_malformed.csv"
    with open(malformed_file, "w", newline="\n", encoding="utf-8") as f:
        f.write(
            "order_id,customer_id,product_id,quantity,order_amount,order_status,order_date,updated_at\n"
        )
        f.write(
            'ORD-MAL-0001,CUST-00001,PROD-00001,1,25.00,"UNCLOSED_QUOTE,2026-01-02,2026-01-02T10:00:00Z\n'
        )
        f.write("CORRUPT_LINE_WITHOUT_COMMAS_AND_FIELDS_HERE_ABCDEFG\n")
        f.write(
            "ORD-MAL-0003,CUST-00001,PROD-00001,NOT_A_NUMBER,25.00,PENDING,2026-01-02,2026-01-02T10:00:00Z\n"
        )
        f.write("ORD-MAL-0004,,,,,,\n")
        f.write(
            "ORD-MAL-0005,CUST-00001,PROD-00001,2,50.00,PENDING,2026-01-02,INVALID_TIMESTAMP_STRING\n"
        )
        for m_idx in range(6, num_malformed + 1):
            if m_idx % 4 == 0:
                f.write(f"CORRUPT_LINE_{m_idx}_CORRUPT_PAYLOAD\n")
            elif m_idx % 4 == 1:
                f.write(f"ORD-MAL-{m_idx:05d},,,,,,\n")
            elif m_idx % 4 == 2:
                f.write(
                    f'ORD-MAL-{m_idx:05d},CUST-00001,PROD-00001,1,10.00,"UNCLOSED_QUOTE,2026-01-02,2026-01-02T10:00:00Z\n'
                )
            else:
                f.write(
                    f"ORD-MAL-{m_idx:05d},CUST-00001,PROD-00001,BAD_QUANTITY,10.00,PENDING,2026-01-02,BAD_TIME\n"
                )

    return {
        "customers": len(customers),
        "products": len(products),
        "initial_orders": len(initial_orders),
        "updates": len(update_orders),
        "schema_change_rows": len(schema_change_rows),
        "malformed_lines": num_malformed,
        "total_order_rows": len(initial_orders)
        + len(update_orders)
        + len(schema_change_rows)
        + num_malformed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic E-Commerce Dataset Generator")
    parser.add_argument(
        "--preset",
        type=str,
        choices=["sample", "large"],
        default=None,
        help="Dataset preset: 'sample' (200 rows in data/sample) or 'large' (100,000 rows in data/large)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="Total target order rows (e.g. 200, 100000, 500000; overrides preset default)",
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="Seed for reproducible generation (default: 42)"
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output directory (default: data/sample for sample, data/large for large)",
    )
    args = parser.parse_args()

    # Determine rows and output directory from preset or explicit arguments
    if args.preset == "large":
        target_rows = args.rows if args.rows is not None else 100000
        target_out = args.out if args.out is not None else "data/large"
    elif args.preset == "sample":
        target_rows = args.rows if args.rows is not None else 200
        target_out = args.out if args.out is not None else "data/sample"
    else:
        target_rows = args.rows if args.rows is not None else 200
        target_out = args.out if args.out is not None else "data/sample"

    counts = generate_dataset(total_rows=target_rows, seed=args.seed, out_dir=target_out)
    preset_label = args.preset or ("sample" if target_rows <= 300 else "large")
    print(f"Generation complete for preset '{preset_label}' ({target_rows} rows -> {target_out}):")
    for k, v in counts.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
