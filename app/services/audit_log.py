"""Append-only activity audit trail."""

from datetime import datetime

from sqlalchemy.orm import Session

from app.models import AuditLog


def write_audit_log(
    db: Session,
    *,
    user_id: str,
    action: str,
    entity: str,
    old_value: str = "",
    new_value: str = "",
) -> None:
    """Queue an audit row; caller must ``commit``."""
    db.add(
        AuditLog(
            user_id=user_id or "unknown",
            action=(action or "unknown").strip().lower(),
            entity=(entity or "").strip(),
            old_value=old_value or "",
            new_value=new_value or "",
            timestamp=datetime.utcnow(),
        )
    )
