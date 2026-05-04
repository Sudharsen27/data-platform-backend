import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import User
from app.utils.jwt import create_access_token
from app.utils.security import hash_password, verify_password

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

router = APIRouter(prefix="/auth", tags=["auth"])


def _admin_email_set() -> set[str]:
    raw = os.getenv("ADMIN_EMAILS", "")
    return {part.strip().lower() for part in raw.split(",") if part.strip()}


class LoginRequest(BaseModel):
    email: str
    password: str


class RegisterRequest(BaseModel):
    full_name: str
    email: str
    company_name: str | None = None
    password: str


def _user_public(user: User) -> dict:
    return {
        "id": user.id,
        "full_name": user.full_name,
        "email": user.email,
        "company_name": user.company_name,
        "role": user.role,
        "is_active": user.is_active,
        "created_at": user.created_at.isoformat(),
    }


@router.post("/login")
def login_user(payload: LoginRequest, db: Session = Depends(get_db)):
    email = payload.email.strip().lower()
    if not email or not payload.password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and password are required",
        )

    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email",
        )

    if not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Wrong password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated",
        )

    # Keep DB in sync with ADMIN_EMAILS (no server restart needed)
    admin_emails = _admin_email_set()
    if email in admin_emails and (user.role or "").lower() != "admin":
        user.role = "admin"
        db.add(user)
        db.commit()
        db.refresh(user)

    access_token = create_access_token(
        subject=user.email,
        role=user.role,
        is_active=user.is_active,
        full_name=user.full_name,
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user": _user_public(user),
    }


@router.post("/register")
def register_user(payload: RegisterRequest, db: Session = Depends(get_db)):
    full_name = payload.full_name.strip()
    email = payload.email.strip().lower()
    company_name = payload.company_name.strip() if payload.company_name else None
    admin_emails = _admin_email_set()
    role = "admin" if email in admin_emails else "user"

    if not full_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Full name is required",
        )
    if "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Please provide a valid email",
        )
    if not payload.password or len(payload.password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 8 characters",
        )

    existing_user = db.query(User).filter(User.email == email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already exists",
        )

    user = User(
        full_name=full_name,
        email=email,
        company_name=company_name,
        password_hash=hash_password(payload.password),
        role=role,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "message": "User registered successfully",
        "user": _user_public(user),
    }
