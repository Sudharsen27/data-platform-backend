"""JWT creation and verification (HS256)."""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import jwt

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mdm-secret-key-change-this")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_REFRESH_TOKEN_EXPIRE_DAYS", "7"))


def create_access_token(
    subject: str,
    *,
    role: str,
    is_active: bool,
    full_name: str | None = None,
    expires_delta: timedelta | None = None,
) -> str:
    """Encode a JWT with ``sub``, ``role``, ``active`` (account enabled), optional ``name``, ``exp``."""
    if expires_delta is None:
        expires_delta = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "active": bool(is_active),
        "type": "access",
        "exp": int(expire.timestamp()),
    }
    if full_name:
        to_encode["name"] = full_name
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def verify_token(token: str) -> dict[str, Any]:
    """
    Decode and validate a JWT. Returns claims dict.
    Raises ``jose.JWTError`` if invalid or expired.
    """
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])


def create_refresh_token(subject: str, *, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode: dict[str, Any] = {
        "sub": subject,
        "role": role,
        "type": "refresh",
        "exp": int(expire.timestamp()),
    }
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


__all__ = [
    "ACCESS_TOKEN_EXPIRE_MINUTES",
    "ALGORITHM",
    "SECRET_KEY",
    "create_access_token",
    "create_refresh_token",
    "verify_token",
]
