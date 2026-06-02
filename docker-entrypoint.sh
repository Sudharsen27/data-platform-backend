#!/bin/sh
set -eu

echo "Waiting for PostgreSQL..."
python -c "from app.db_url import get_database_host_for_logging; print('DATABASE_URL host:', get_database_host_for_logging())"
python scripts/docker_wait_db.py

echo "Running Alembic migrations..."
alembic upgrade head

echo "Starting FastAPI (uvicorn app.main:app)..."
exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"