from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import MasterData, QuarantineData, StewardshipQueue, User
from app.schemas import (
    MasterDataCompareOut,
    MasterDataOut,
    MasterDataPageOut,
    QuarantineOut,
    StewardshipOut,
)

router = APIRouter(prefix="/master-data", tags=["master-data"])


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
