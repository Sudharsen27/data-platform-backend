import os

import pytest

from app.db_url import normalize_database_url


@pytest.mark.parametrize(
    "raw,expected_prefix",
    [
        ("postgresql+psycopg2://u:p@db.example.com:5432/app", "postgresql+psycopg2://"),
        ("postgresql://u:p@db.example.com:5432/app", "postgresql+psycopg2://"),
        ("postgres://u:p@db.example.com:5432/app", "postgresql+psycopg2://"),
    ],
)
def test_normalize_database_url_render_and_sqlalchemy_schemes(raw, expected_prefix):
    out = normalize_database_url(raw)
    assert out.startswith(expected_prefix)
    assert "db.example.com" in out
    assert "+psycopg2" in out.split("://", 1)[0]


def test_get_database_url_requires_env(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("SKIP_DOTENV", "1")

    from app import db_url

    with pytest.raises(db_url.DatabaseConfigError):
        db_url.get_database_url()


def test_localhost_rejected_inside_container(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/app")
    monkeypatch.setenv("RUNNING_IN_DOCKER", "1")
    monkeypatch.setenv("SKIP_DOTENV", "1")

    from app import db_url

    with pytest.raises(db_url.DatabaseConfigError, match="invalid inside Docker"):
        db_url.get_database_url()
