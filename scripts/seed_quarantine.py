import argparse
import os
import sys
from random import randint

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.database import SessionLocal
from app.models import QuarantineData


def build_record(index: int):
    bucket = index % 10
    name = f"User {index}"

    if bucket < 6:
        email = f"user{index}@mail.com"
    elif bucket < 9:
        email = f"user{index}"
    else:
        email = ""

    return {
        "name": name,
        "email": email,
        "error": "Seeded record",
        "match_status": "new",
    }


def seed_quarantine(total: int, batch_size: int, truncate_first: bool):
    db = SessionLocal()
    try:
        if truncate_first:
            db.query(QuarantineData).delete()
            db.commit()

        inserted = 0
        while inserted < total:
            size = min(batch_size, total - inserted)
            rows = [build_record(inserted + i + randint(0, 3)) for i in range(size)]
            db.bulk_insert_mappings(QuarantineData, rows)
            db.commit()
            inserted += size
            print(f"Inserted {inserted}/{total} rows")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed quarantine_data with synthetic rows.")
    parser.add_argument("--total", type=int, default=100000, help="Total rows to insert.")
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Rows inserted per batch."
    )
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help="Delete existing quarantine_data rows before seeding.",
    )
    args = parser.parse_args()

    seed_quarantine(
        total=args.total, batch_size=args.batch_size, truncate_first=args.truncate_first
    )
