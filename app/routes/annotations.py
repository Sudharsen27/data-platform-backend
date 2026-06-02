from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import AnnotationHistory, RecordAnnotation, User
from app.schemas import (
    AnnotationCreateIn,
    AnnotationHistoryOut,
    AnnotationListOut,
    AnnotationOut,
    AnnotationUpdateIn,
)
from app.services.audit_log import write_audit_log

router = APIRouter(prefix="/annotations", tags=["annotations"])

_ALLOWED_ANNOTATION_STATUSES = frozenset(
    {"approved", "rejected", "needs_review", "duplicate"}
)


def _normalize_annotation_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value not in _ALLOWED_ANNOTATION_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=(
                "status must be one of: "
                + ", ".join(sorted(_ALLOWED_ANNOTATION_STATUSES))
            ),
        )
    return value


@router.post("", response_model=AnnotationOut)
def create_annotation(
    payload: AnnotationCreateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    status = _normalize_annotation_status(payload.status)
    row = RecordAnnotation(
        record_id=payload.record_id,
        comment=(payload.comment or "").strip(),
        status=status,
        created_by=actor.email,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()

    db.add(
        AnnotationHistory(
            annotation_id=row.id,
            action="create",
            old_value="",
            new_value=f"status={status}; comment={row.comment}",
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="annotation_create",
        entity=f"stewardship_queue:{payload.record_id}",
        old_value="",
        new_value=f"annotation_id={row.id}; status={status}",
    )
    db.commit()
    db.refresh(row)
    return row


@router.get("", response_model=AnnotationListOut)
def list_annotations(
    record_id: int = Query(..., ge=1),
    status: str = Query("all"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("stewardship:manage")),
):
    query = db.query(RecordAnnotation).filter(RecordAnnotation.record_id == record_id)
    raw = (status or "all").strip().lower()
    if raw != "all":
        raw = _normalize_annotation_status(raw)
        query = query.filter(RecordAnnotation.status == raw)

    total = query.count()
    items = (
        query.order_by(RecordAnnotation.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {"items": items, "total": total, "offset": offset, "limit": limit}


@router.get("/{annotation_id}", response_model=AnnotationOut)
def get_annotation(
    annotation_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("stewardship:manage")),
):
    row = (
        db.query(RecordAnnotation)
        .filter(RecordAnnotation.id == annotation_id)
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Annotation not found")
    return row


@router.put("/{annotation_id}", response_model=AnnotationOut)
def update_annotation(
    annotation_id: int,
    payload: AnnotationUpdateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = (
        db.query(RecordAnnotation)
        .filter(RecordAnnotation.id == annotation_id)
        .first()
    )
    if not row:
        raise HTTPException(status_code=404, detail="Annotation not found")

    prev_comment = row.comment
    prev_status = row.status
    next_status = _normalize_annotation_status(payload.status)
    next_comment = (payload.comment or "").strip()

    row.status = next_status
    row.comment = next_comment
    row.updated_at = datetime.utcnow()
    db.add(
        AnnotationHistory(
            annotation_id=row.id,
            action="update",
            old_value=f"status={prev_status}; comment={prev_comment}",
            new_value=f"status={next_status}; comment={next_comment}",
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action=(
            "annotation_status_change"
            if prev_status != next_status
            else "annotation_update"
        ),
        entity=f"annotation:{annotation_id}",
        old_value=f"status={prev_status}; comment={prev_comment}",
        new_value=f"status={next_status}; comment={next_comment}",
    )
    db.commit()
    db.refresh(row)
    return row


@router.get("/{annotation_id}/history", response_model=list[AnnotationHistoryOut])
def get_annotation_history(
    annotation_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("stewardship:manage")),
):
    return (
        db.query(AnnotationHistory)
        .filter(AnnotationHistory.annotation_id == annotation_id)
        .order_by(AnnotationHistory.acted_at.desc())
        .all()
    )
