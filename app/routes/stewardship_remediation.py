import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import (
    AICopilotActionLog,
    StewardshipQueue,
    StewardshipRemediation,
    StewardshipRemediationHistory,
    User,
)
from app.schemas import (
    StewardshipExplainIn,
    StewardshipExplainOut,
    StewardshipRemediationAssignIn,
    StewardshipRemediationEntryOut,
    StewardshipRemediationHistoryOut,
    StewardshipRemediationOut,
    StewardshipRemediateIn,
)
from app.services.audit_log import write_audit_log
from app.services.remediation_service import (
    explain_stewardship_failure,
    generate_remediation,
    get_latest_remediation,
    persist_remediation,
    serialize_remediation,
)

router = APIRouter(prefix="/stewardship", tags=["stewardship"])


def _log_remediation(
    db: Session,
    *,
    actor: User,
    action_key: str,
    entity: str,
    summary: str,
    payload: dict,
) -> None:
    db.add(
        AICopilotActionLog(
            action_key=action_key,
            user_id=actor.email,
            status="success",
            summary=summary[:500],
            payload=json.dumps(payload, ensure_ascii=True),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action=f"ai_{action_key}",
        entity=entity,
        old_value="",
        new_value=summary[:500],
    )


def _entry_out(row: StewardshipRemediation) -> StewardshipRemediationEntryOut:
    return StewardshipRemediationEntryOut(**serialize_remediation(row))


@router.post("/explain", response_model=StewardshipExplainOut)
def explain_failure(
    body: StewardshipExplainIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    result = explain_stewardship_failure(db, body.stewardship_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stewardship record not found")

    summary = f"Explained failure for stewardship #{body.stewardship_id}"
    _log_remediation(
        db,
        actor=actor,
        action_key="stewardship_explain",
        entity=f"stewardship_queue:{body.stewardship_id}",
        summary=summary,
        payload=result,
    )
    db.commit()
    return result


@router.post("/remediate", response_model=StewardshipRemediationEntryOut)
def remediate_failure(
    body: StewardshipRemediateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == body.stewardship_id).first()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stewardship record not found")

    result = generate_remediation(db, record)
    row = persist_remediation(
        db,
        stewardship_id=body.stewardship_id,
        payload=result,
        actor_email=actor.email,
        source_engine=result.get("source_engine", "heuristics"),
    )
    result = {**result, **serialize_remediation(row)}
    summary = (
        f"Remediation for stewardship #{body.stewardship_id}: "
        f"{result.get('root_cause', '')[:120]}"
    )
    _log_remediation(
        db,
        actor=actor,
        action_key="stewardship_remediate",
        entity=f"stewardship_queue:{body.stewardship_id}",
        summary=summary,
        payload={
            "stewardship_id": body.stewardship_id,
            "root_cause": result.get("root_cause"),
            "risk_score": result.get("risk_score"),
            "source_engine": result.get("source_engine"),
        },
    )
    db.commit()
    return result


@router.get("/remediation/{stewardship_id}", response_model=StewardshipRemediationEntryOut)
def get_remediation(
    stewardship_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = get_latest_remediation(db, stewardship_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")
    return _entry_out(row)


@router.post("/remediation/{remediation_id}/accept", response_model=StewardshipRemediationEntryOut)
def accept_remediation(
    remediation_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(StewardshipRemediation).filter(StewardshipRemediation.id == remediation_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")

    old_snapshot = serialize_remediation(row)
    row.status = "accepted"
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()
    db.add(
        StewardshipRemediationHistory(
            remediation_id=row.id,
            action="accept",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_remediation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_remediation_accept",
        entity=f"stewardship_remediation:{row.id}",
        old_value=old_snapshot.get("status", ""),
        new_value="accepted",
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.post("/remediation/{remediation_id}/reject", response_model=StewardshipRemediationEntryOut)
def reject_remediation(
    remediation_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(StewardshipRemediation).filter(StewardshipRemediation.id == remediation_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")

    old_snapshot = serialize_remediation(row)
    row.status = "rejected"
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()
    db.add(
        StewardshipRemediationHistory(
            remediation_id=row.id,
            action="reject",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_remediation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_remediation_reject",
        entity=f"stewardship_remediation:{row.id}",
        old_value=old_snapshot.get("status", ""),
        new_value="rejected",
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.post("/remediation/{remediation_id}/resolve", response_model=StewardshipRemediationEntryOut)
def resolve_remediation(
    remediation_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(StewardshipRemediation).filter(StewardshipRemediation.id == remediation_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")

    old_snapshot = serialize_remediation(row)
    row.status = "resolved"
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()
    db.add(
        StewardshipRemediationHistory(
            remediation_id=row.id,
            action="resolve",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_remediation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_remediation_resolve",
        entity=f"stewardship_remediation:{row.id}",
        old_value=old_snapshot.get("status", ""),
        new_value="resolved",
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.post("/remediation/{remediation_id}/assign", response_model=StewardshipRemediationEntryOut)
def assign_steward(
    remediation_id: int,
    body: StewardshipRemediationAssignIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(StewardshipRemediation).filter(StewardshipRemediation.id == remediation_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")

    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == row.stewardship_id).first()
    if not record:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Stewardship record not found")

    owner = (body.owner_email or "").strip().lower()
    if not owner:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="owner_email is required")

    old_snapshot = serialize_remediation(row)
    row.assigned_owner = owner
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()
    record.owner_email = owner

    db.add(
        StewardshipRemediationHistory(
            remediation_id=row.id,
            action="assign",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_remediation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_remediation_assign",
        entity=f"stewardship_queue:{row.stewardship_id}",
        old_value=old_snapshot.get("assigned_owner", ""),
        new_value=owner,
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.get("/remediation/{remediation_id}/history", response_model=list[StewardshipRemediationHistoryOut])
def remediation_history(
    remediation_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(StewardshipRemediation).filter(StewardshipRemediation.id == remediation_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Remediation not found")
    rows = (
        db.query(StewardshipRemediationHistory)
        .filter(StewardshipRemediationHistory.remediation_id == remediation_id)
        .order_by(StewardshipRemediationHistory.acted_at.desc())
        .limit(50)
        .all()
    )
    return rows
