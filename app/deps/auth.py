from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import User
from app.utils.jwt import verify_token

bearer_scheme = HTTPBearer()

ROLE_PERMISSIONS = {
    "admin": {
        "dashboard:read",
        "lineage:read",
        "catalog:read",
        "catalog:write",
        "rules:read",
        "rules:write",
        "pipeline:run",
        "users:manage",
        "audit:read",
        "stewardship:manage",
    },
    "user": {
        "dashboard:read",
        "lineage:read",
        "catalog:read",
        "rules:read",
        "audit:read",
        "stewardship:manage",
    },
}


def get_bearer_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> str:
    return credentials.credentials


def get_token_payload(token: str = Depends(get_bearer_token)) -> dict:
    try:
        return verify_token(token)
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


def get_current_user(
    db: Session = Depends(get_db),
    payload: dict = Depends(get_token_payload),
) -> User:
    email = (payload.get("sub") or "").strip().lower()
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated",
        )
    return user


def require_admin(user: User = Depends(get_current_user)) -> User:
    if (user.role or "").lower() != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return user


def require_permission(permission: str):
    def checker(user: User = Depends(get_current_user)) -> User:
        role = (user.role or "user").lower()
        allowed = ROLE_PERMISSIONS.get(role, set())
        if permission not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission required: {permission}",
            )
        return user

    return checker
