import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

_TRUE = frozenset({"1", "true", "yes", "on"})


def is_snowflake_enabled() -> bool:
    """False when SNOWFLAKE_ENABLED=false or required credentials are missing."""
    if os.getenv("SNOWFLAKE_ENABLED", "true").strip().lower() not in _TRUE:
        return False
    return all(
        os.getenv(key, "").strip()
        for key in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
    )


def get_snowflake_connection():
    if not is_snowflake_enabled():
        raise RuntimeError(
            "Snowflake is disabled. Set SNOWFLAKE_ENABLED=true and credentials, "
            "or set SNOWFLAKE_ENABLED=false after your trial ends."
        )
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
        user=os.getenv("SNOWFLAKE_USER", ""),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        database=os.getenv("SNOWFLAKE_DATABASE", ""),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        role=os.getenv("SNOWFLAKE_ROLE", None),
    )
