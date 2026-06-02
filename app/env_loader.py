"""Load backend/.env only for local (non-container) development."""

from __future__ import annotations

from pathlib import Path

from app.db_url import is_container_runtime


def load_backend_dotenv() -> None:
    """Load backend/.env when running on the host; never in Docker."""
    if is_container_runtime():
        return
    if __import__("os").getenv("SKIP_DOTENV", "").strip().lower() in ("1", "true", "yes"):
        return

    from dotenv import load_dotenv

    backend_root = Path(__file__).resolve().parents[1]
    env_path = backend_root / ".env"
    if env_path.is_file():
        load_dotenv(env_path, override=False)
