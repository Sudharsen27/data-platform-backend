import os

from app.db.snowflake import is_snowflake_enabled


def test_snowflake_disabled_when_flag_false(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ENABLED", "false")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pass")
    assert is_snowflake_enabled() is False


def test_snowflake_disabled_when_credentials_missing(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_ENABLED", raising=False)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "")
    monkeypatch.setenv("SNOWFLAKE_USER", "")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "")
    assert is_snowflake_enabled() is False
