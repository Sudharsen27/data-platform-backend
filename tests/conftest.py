"""Pytest setup: SQLite file DB + skip Postgres seed so tests run without Docker."""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

_tmp = tempfile.NamedTemporaryFile(suffix=".pytest_mdm.db", delete=False)
_tmp.close()
TEST_DB_PATH = Path(_tmp.name)

os.environ["DATABASE_URL"] = f"sqlite:///{TEST_DB_PATH.as_posix()}"
os.environ["SKIP_STARTUP_SEED"] = "1"
os.environ["JWT_SECRET_KEY"] = "pytest-jwt-secret-key-32chars-min!!"
os.environ.setdefault("ADMIN_EMAILS", "admin@example.com")
os.environ.setdefault("AI_ENABLED", "true")
os.environ.setdefault("AI_PROVIDER", "heuristic")
for _var in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"):
    os.environ.pop(_var, None)

from fastapi.testclient import TestClient
import pytest

from app.main import app


def pytest_sessionfinish(session, exitstatus):
    try:
        TEST_DB_PATH.unlink(missing_ok=True)
    except OSError:
        pass


@pytest.fixture(scope="session")
def client() -> TestClient:
    with TestClient(app) as c:
        yield c
