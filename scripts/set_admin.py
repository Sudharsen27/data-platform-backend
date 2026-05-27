#!/usr/bin/env python3
"""
Set the mini-mdm-platform admin from backend/.env or CLI.

Examples:
  python scripts/set_admin.py --email admin@company.com --password "YourSecurePass1"
  # Or edit ADMIN_EMAILS + ADMIN_BOOTSTRAP_PASSWORD in .env, then:
  python scripts/set_admin.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1]
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from dotenv import load_dotenv

load_dotenv(_BACKEND / ".env")

from app.database import SessionLocal
from app.main import seed_data
from app.models import User
from app.utils.security import hash_password


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create or update mini-mdm admin user")
    parser.add_argument("--email", help="Admin email (overrides ADMIN_EMAILS in .env)")
    parser.add_argument("--password", help="Admin password (overrides ADMIN_BOOTSTRAP_PASSWORD)")
    parser.add_argument(
        "--remove-email",
        action="append",
        default=[],
        help="Delete a user by email (can repeat). Use to drop an old admin.",
    )
    parser.add_argument(
        "--sync-only",
        action="store_true",
        help="Only run seed_data(); do not delete users",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    email = (args.email or os.getenv("ADMIN_EMAILS", "").split(",")[0] or "").strip().lower()
    password = (args.password or os.getenv("ADMIN_BOOTSTRAP_PASSWORD") or "").strip()

    db = SessionLocal()
    try:
        for old in args.remove_email:
            old_norm = old.strip().lower()
            if not old_norm:
                continue
            user = db.query(User).filter(User.email == old_norm).first()
            if user:
                db.delete(user)
                print(f"Removed user: {old_norm}")

        if args.remove_email:
            db.commit()

        if args.sync_only and not email:
            print("Done (remove/sync-only).")
            return 0

        if not args.sync_only and email and password:
            os.environ["ADMIN_EMAILS"] = email
            os.environ["ADMIN_BOOTSTRAP_PASSWORD"] = password
            os.environ["ADMIN_BOOTSTRAP_SYNC_PASSWORD"] = "true"

        if not email:
            print("Provide --email or set ADMIN_EMAILS in backend/.env", file=sys.stderr)
            return 1
        if not password:
            print("Provide --password or set ADMIN_BOOTSTRAP_PASSWORD in backend/.env", file=sys.stderr)
            return 1
        if len(password) < 8:
            print("Password must be at least 8 characters", file=sys.stderr)
            return 1

        seed_data(db)

        user = db.query(User).filter(User.email == email).first()
        if not user:
            print(f"Failed to create admin for {email}", file=sys.stderr)
            return 1

        print("Admin ready:")
        print(f"  email: {user.email}")
        print(f"  role:  {user.role}")
        print("\nSign in at http://localhost:3000/login")
        print("Use mini-mdm backend on port 8000 (not MDQM).")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
