from csv import writer
from io import StringIO
from typing import List

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user
from app.models import AuditLog, User
from app.schemas import AuditLogOut

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("", response_model=List[AuditLogOut])
def list_audit_logs(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
    action: str | None = Query(default=None, description="Filter by action"),
    user_filter: str | None = Query(default=None, alias="user", description="Filter by user email"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    q = db.query(AuditLog)
    if action and action.strip():
        q = q.filter(AuditLog.action == action.strip().lower())
    if user_filter and user_filter.strip():
        needle = user_filter.strip().lower()
        q = q.filter(AuditLog.user_id.ilike(f"%{needle}%"))
    return q.order_by(AuditLog.timestamp.desc()).limit(limit).all()


@router.get("/export")
def export_audit_logs_csv(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
    action: str | None = Query(default=None, description="Filter by action"),
    user_filter: str | None = Query(default=None, alias="user", description="Filter by user email"),
    limit: int = Query(default=300, ge=1, le=5000),
):
    q = db.query(AuditLog)
    if action and action.strip():
        q = q.filter(AuditLog.action == action.strip().lower())
    if user_filter and user_filter.strip():
        needle = user_filter.strip().lower()
        q = q.filter(AuditLog.user_id.ilike(f"%{needle}%"))

    rows = q.order_by(AuditLog.timestamp.desc()).limit(limit).all()

    buffer = StringIO()
    csv_writer = writer(buffer)
    csv_writer.writerow(["id", "user_id", "action", "entity", "old_value", "new_value", "timestamp"])
    for row in rows:
        csv_writer.writerow(
            [
                row.id,
                row.user_id,
                row.action,
                row.entity,
                row.old_value,
                row.new_value,
                row.timestamp.isoformat() if row.timestamp else "",
            ]
        )

    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=audit_logs.csv"},
    )
