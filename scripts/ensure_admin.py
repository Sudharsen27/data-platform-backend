#!/usr/bin/env python3
"""Create or sync the admin user from backend/.env (ADMIN_EMAILS + ADMIN_BOOTSTRAP_PASSWORD)."""

from __future__ import annotations

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


def main() -> int:
    admin_emails = [
        p.strip().lower()
        for p in os.getenv("ADMIN_EMAILS", "").split(",")
        if p.strip()
    ]
    password = (os.getenv("ADMIN_BOOTSTRAP_PASSWORD") or "").strip()
    if not admin_emails:
        print("Set ADMIN_EMAILS in backend/.env", file=sys.stderr)
        return 1
    if not password:
        print("Set ADMIN_BOOTSTRAP_PASSWORD in backend/.env", file=sys.stderr)
        return 1

    db = SessionLocal()
    try:
        seed_data(db)
    finally:
        db.close()

    print("Admin account ready for mini-mdm-platform:")
    for email in admin_emails:
        print(f"  email:    {email}")
    print(f"  password: (value of ADMIN_BOOTSTRAP_PASSWORD in .env)")
    print(
        "\nAfter you can sign in, set ADMIN_BOOTSTRAP_SYNC_PASSWORD=false in .env."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
