"""Wait until PostgreSQL accepts connections (used by Docker entrypoint)."""

from __future__ import annotations

import os
import sys
import time

from sqlalchemy import create_engine, text

from app.db_url import get_database_host_for_logging, get_database_url

MAX_ATTEMPTS = int(os.getenv("DB_WAIT_MAX_ATTEMPTS", "30"))
SLEEP_SECONDS = float(os.getenv("DB_WAIT_SLEEP_SECONDS", "2"))


def main() -> int:
    try:
        database_url = get_database_url()
    except Exception as exc:
        print(f"DATABASE_URL configuration error: {exc}", file=sys.stderr)
        return 1

    print(f"Waiting for PostgreSQL at host: {get_database_host_for_logging()}")

    engine = create_engine(database_url, pool_pre_ping=True)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"Database ready (attempt {attempt}/{MAX_ATTEMPTS})")
            return 0
        except Exception as exc:
            print(
                f"Waiting for database ({attempt}/{MAX_ATTEMPTS}): {exc}",
                file=sys.stderr,
            )
            time.sleep(SLEEP_SECONDS)

    print("Database did not become ready in time", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
