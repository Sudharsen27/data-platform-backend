from difflib import SequenceMatcher

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import DuplicateReview, MasterData, QuarantineData, StewardshipQueue, User
from app.services.audit_log import write_audit_log
from app.schemas import (
    DuplicateCandidatesOut,
    DuplicateCandidateOut,
    DuplicateMergeRequest,
    DuplicateRejectRequest,
    MasterDataCompareOut,
    MasterDataOut,
    MasterDataPageOut,
    QuarantineOut,
    StewardshipOut,
)

router = APIRouter(prefix="/master-data", tags=["master-data"])


def _norm_name(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_email(value: str) -> str:
    return (value or "").strip().lower()


def _pair_key(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def _score_duplicate(left: MasterData, right: MasterData) -> tuple[float, str]:
    left_email = _norm_email(left.email)
    right_email = _norm_email(right.email)
    left_name = _norm_name(left.name)
    right_name = _norm_name(right.name)

    if left_email and right_email and left_email == right_email:
        return 0.99, "Exact email match"

    if left_name and right_name and left_name == right_name:
        return 0.92, "Exact name match"

    if left_name and right_name:
        ratio = SequenceMatcher(None, left_name, right_name).ratio()
        if ratio >= 0.9:
            return float(round(0.75 + ((ratio - 0.9) * 1.5), 2)), "Very similar names"
        if ratio >= 0.82 and left_email and right_email:
            left_domain = left_email.split("@")[-1] if "@" in left_email else ""
            right_domain = right_email.split("@")[-1] if "@" in right_email else ""
            if left_domain and left_domain == right_domain:
                return 0.72, "Similar name and same email domain"
    return 0.0, ""


@router.get("", response_model=MasterDataPageOut)
def list_master_data(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    q: str = Query("", description="Search name or email"),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("dashboard:read")),
):
    query = db.query(MasterData)
    term = q.strip()
    if term:
        like = f"%{term}%"
        query = query.filter(
            or_(MasterData.name.ilike(like), MasterData.email.ilike(like))
        )
    total = query.count()
    items = (
        query.order_by(MasterData.id.desc()).offset(offset).limit(limit).all()
    )
    return {
        "items": items,
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/duplicates", response_model=DuplicateCandidatesOut)
def list_duplicate_candidates(
    min_confidence: float = Query(0.7, ge=0.5, le=1.0),
    limit: int = Query(40, ge=1, le=200),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("dashboard:read")),
):
    rows = (
        db.query(MasterData)
        .order_by(MasterData.id.desc())
        .limit(300)
        .all()
    )

    reviewed_pairs = {
        _pair_key(r.left_id, r.right_id)
        for r in db.query(DuplicateReview).all()
    }

    candidates: list[DuplicateCandidateOut] = []
    for i in range(len(rows)):
        left = rows[i]
        for j in range(i + 1, len(rows)):
            right = rows[j]
            pair = _pair_key(left.id, right.id)
            if pair in reviewed_pairs:
                continue
            confidence, reason = _score_duplicate(left, right)
            if confidence < min_confidence:
                continue
            candidates.append(
                DuplicateCandidateOut(
                    left_id=left.id,
                    right_id=right.id,
                    left_name=left.name,
                    right_name=right.name,
                    left_email=left.email,
                    right_email=right.email,
                    confidence=confidence,
                    reason=reason,
                )
            )

    candidates.sort(key=lambda c: c.confidence, reverse=True)
    items = candidates[:limit]
    return {"items": items, "total": len(candidates)}


@router.post("/duplicates/merge")
def merge_duplicate_candidate(
    body: DuplicateMergeRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    left = db.query(MasterData).filter(MasterData.id == body.left_id).first()
    right = db.query(MasterData).filter(MasterData.id == body.right_id).first()
    if not left or not right:
        raise HTTPException(status_code=404, detail="Duplicate candidate row not found")

    survivor_id = body.survivor_id or body.left_id
    if survivor_id not in (left.id, right.id):
        raise HTTPException(status_code=400, detail="survivor_id must match left_id or right_id")

    survivor = left if survivor_id == left.id else right
    loser = right if survivor is left else left
    confidence, _ = _score_duplicate(left, right)

    if (not survivor.name or len((survivor.name or "").strip()) < len((loser.name or "").strip())) and loser.name:
        survivor.name = loser.name
    if not (survivor.email or "").strip() and (loser.email or "").strip():
        survivor.email = loser.email

    db.add(
        DuplicateReview(
            left_id=min(left.id, right.id),
            right_id=max(left.id, right.id),
            status="merged",
            reviewed_by=actor.email,
            note=f"survivor={survivor.id}",
            confidence=confidence,
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="duplicate_merge",
        entity=f"master:{left.id}-{right.id}",
        old_value=f"{left.id},{right.id}",
        new_value=f"survivor={survivor.id}",
    )
    db.delete(loser)
    db.add(survivor)
    db.commit()
    return {"message": f"Merged record #{loser.id} into #{survivor.id}", "survivor_id": survivor.id}


@router.post("/duplicates/reject")
def reject_duplicate_candidate(
    body: DuplicateRejectRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    left_id, right_id = _pair_key(body.left_id, body.right_id)
    left = db.query(MasterData).filter(MasterData.id == left_id).first()
    right = db.query(MasterData).filter(MasterData.id == right_id).first()
    if not left or not right:
        raise HTTPException(status_code=404, detail="Duplicate candidate row not found")
    confidence, _ = _score_duplicate(left, right)
    db.add(
        DuplicateReview(
            left_id=left_id,
            right_id=right_id,
            status="dismissed",
            reviewed_by=actor.email,
            note=(body.note or "").strip(),
            confidence=confidence,
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="duplicate_dismiss",
        entity=f"master:{left_id}-{right_id}",
        old_value="",
        new_value=(body.note or "").strip(),
    )
    db.commit()
    return {"message": "Candidate dismissed"}


@router.get("/compare/{source_queue_id}", response_model=MasterDataCompareOut)
def compare_source_to_golden(
    source_queue_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("dashboard:read")),
):
    stewardship = (
        db.query(StewardshipQueue)
        .filter(StewardshipQueue.id == source_queue_id)
        .first()
    )
    if not stewardship:
        raise HTTPException(status_code=404, detail="Stewardship record not found")

    quarantine = (
        db.query(QuarantineData)
        .filter(QuarantineData.id == source_queue_id)
        .first()
    )
    golden = (
        db.query(MasterData)
        .filter(MasterData.source_queue_id == source_queue_id)
        .order_by(MasterData.id.desc())
        .first()
    )
    if not golden and (stewardship.email or "").strip():
        golden = (
            db.query(MasterData)
            .filter(func.lower(MasterData.email) == stewardship.email.strip().lower())
            .order_by(MasterData.id.desc())
            .first()
        )

    return {
        "source_queue_id": source_queue_id,
        "stewardship": StewardshipOut.model_validate(stewardship),
        "quarantine": QuarantineOut.model_validate(quarantine) if quarantine else None,
        "golden": MasterDataOut.model_validate(golden) if golden else None,
        "is_published": golden is not None,
    }


@router.get("/{master_id}", response_model=MasterDataOut)
def get_master_record(
    master_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("dashboard:read")),
):
    row = db.query(MasterData).filter(MasterData.id == master_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Golden record not found")
    return row
