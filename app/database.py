import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/mdm_platform",
)

# Fail fast when Postgres is down instead of hanging the API for minutes.
_connect_args = {}
if DATABASE_URL.startswith("postgresql"):
    _connect_args["connect_timeout"] = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args=_connect_args or {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
