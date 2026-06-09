from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import User
from app.schemas import GovernanceDatasetScoreOut, GovernancePlatformScoreOut
from app.services.governance_score_service import (
    compute_platform_score,
    get_dataset_governance_score,
)

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/score", response_model=GovernancePlatformScoreOut)
def get_platform_governance_score(
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("dashboard:read")),
):
    """Platform-wide governance health score and KPI rollup."""
    return compute_platform_score(db)


@router.get("/score/{dataset_id}", response_model=GovernanceDatasetScoreOut)
def get_dataset_score(
    dataset_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("catalog:read")),
):
    """Governance health score for a single catalog dataset."""
    result = get_dataset_governance_score(db, dataset_id)
    if not result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Catalog asset {dataset_id} not found",
        )
    return result
