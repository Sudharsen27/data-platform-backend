from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_admin
from app.models import User
from app.services.audit_log import write_audit_log

router = APIRouter(prefix="/users", tags=["users"])

ALLOWED_ROLES = frozenset({"admin", "user"})


class UserListItem(BaseModel):
    id: int
    full_name: str
    email: str
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class UserRoleBody(BaseModel):
    role: str = Field(..., description="admin or user")

    def normalized_role(self) -> str:
        r = (self.role or "").strip().lower()
        if r not in ALLOWED_ROLES:
            raise ValueError("role must be admin or user")
        return r


class UserStatusBody(BaseModel):
    is_active: bool


def _count_active_admins(db: Session) -> int:
    return (
        db.query(func.count(User.id))
        .filter(func.lower(User.role) == "admin", User.is_active.is_(True))
        .scalar()
        or 0
    )


def _is_only_active_admin(db: Session, user: User) -> bool:
    if (user.role or "").lower() != "admin" or not user.is_active:
        return False
    return _count_active_admins(db) <= 1


@router.get("", response_model=list[UserListItem])
def list_users(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    users = db.query(User).order_by(User.id.asc()).all()
    return users


@router.put("/{user_id}/role", response_model=UserListItem)
def update_user_role(
    user_id: int,
    body: UserRoleBody,
    db: Session = Depends(get_db),
    actor: User = Depends(require_admin),
):
    try:
        new_role = body.normalized_role()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    old_role = (user.role or "").lower()
    if old_role == "admin" and new_role == "user" and _is_only_active_admin(db, user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot remove the last active administrator",
        )

    user.role = new_role
    write_audit_log(
        db,
        user_id=actor.email,
        action="role_change",
        entity=f"user:{user.id}",
        old_value=old_role,
        new_value=new_role,
    )
    db.commit()
    db.refresh(user)
    return user


@router.put("/{user_id}/status", response_model=UserListItem)
def update_user_status(
    user_id: int,
    body: UserStatusBody,
    db: Session = Depends(get_db),
    actor: User = Depends(require_admin),
):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if actor.id == user.id and not body.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="You cannot deactivate your own account",
        )

    if not body.is_active and _is_only_active_admin(db, user):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot deactivate the last active administrator",
        )

    prev_active = user.is_active
    user.is_active = body.is_active
    write_audit_log(
        db,
        user_id=actor.email,
        action="status_change",
        entity=f"user:{user.id}",
        old_value="active" if prev_active else "inactive",
        new_value="active" if user.is_active else "inactive",
    )
    db.commit()
    db.refresh(user)
    return user
