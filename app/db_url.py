"""
Resolve and normalize DATABASE_URL from the environment.

Priority:
  1. DATABASE_URL already set (Docker Compose, Render, CI, shell)
  2. backend/.env via python-dotenv (local development only)

Never falls back to localhost — missing DATABASE_URL raises a clear error.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlparse

# Hostnames that only work on the developer machine — never valid inside a container.
_FORBIDDEN_CONTAINER_DB_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


class DatabaseConfigError(RuntimeError):
    """DATABASE_URL is missing or invalid."""


def is_container_runtime() -> bool:
    """True when running inside Docker (Compose, Render Docker, etc.)."""
    if os.getenv("RUNNING_IN_DOCKER", "").strip().lower() in ("1", "true", "yes"):
        return True
    return Path("/.dockerenv").is_file()


def _running_in_docker() -> bool:
    return is_container_runtime()


def _load_local_dotenv() -> None:
    """Load backend/.env for local runs; never override platform-injected env vars."""
    if _running_in_docker():
        return
    if os.getenv("SKIP_DOTENV", "").strip().lower() in ("1", "true", "yes"):
        return
    if os.getenv("DATABASE_URL", "").strip():
        return

    from dotenv import load_dotenv

    backend_root = Path(__file__).resolve().parents[1]
    env_path = backend_root / ".env"
    if env_path.is_file():
        load_dotenv(env_path, override=False)


def normalize_database_url(url: str) -> str:
    """
    Ensure SQLAlchemy can use the DSN with psycopg2.

    Accepts Render/Heroku-style schemes:
      postgres://...
      postgresql://...
      postgresql+psycopg2://... (unchanged)
    """
    normalized = url.strip()
    if not normalized:
        return normalized

    if normalized.startswith("postgres://"):
        normalized = "postgresql+psycopg2://" + normalized[len("postgres://") :]
    elif normalized.startswith("postgresql://") and "+psycopg2" not in normalized.split("://", 1)[0]:
        normalized = "postgresql+psycopg2://" + normalized[len("postgresql://") :]

    return normalized


def get_database_url() -> str:
    _load_local_dotenv()

    raw = os.getenv("DATABASE_URL", "").strip()
    if not raw:
        raise DatabaseConfigError(
            "DATABASE_URL is not set. "
            "Local: copy backend/.env.example to backend/.env. "
            "Docker: set POSTGRES_* in mini-mdm-platform/.env (Compose builds DATABASE_URL). "
            "Render: link the Postgres instance so DATABASE_URL is injected."
        )

    url = normalize_database_url(raw)
    _assert_valid_runtime_host(url)
    return url


def _assert_valid_runtime_host(url: str) -> None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").strip().lower()

    if not is_container_runtime():
        return

    if host in _FORBIDDEN_CONTAINER_DB_HOSTS:
        raise DatabaseConfigError(
            f"DATABASE_URL uses host '{host}', which is invalid inside Docker. "
            "Containers must use the Compose service name 'postgres' "
            "(e.g. postgresql+psycopg2://postgres:PASSWORD@postgres:5432/mdm_platform). "
            "Remove env_file: backend/.env from compose, rebuild the image, and set "
            "POSTGRES_PASSWORD in mini-mdm-platform/.env — Compose builds DATABASE_URL."
        )


def get_database_host_for_logging() -> str:
    """Safe host label for startup logs (no credentials)."""
    try:
        parsed = urlparse(get_database_url())
        return parsed.hostname or "(unknown host)"
    except DatabaseConfigError:
        return "(DATABASE_URL not set)"
