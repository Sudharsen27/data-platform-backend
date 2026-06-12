from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import User
from app.schemas import GovernanceDashboardOut, GovernanceDatasetScoreOut, GovernancePlatformScoreOut
from app.services.audit_log import write_audit_log
from app.services.governance_score_service import (
    build_governance_dashboard,
    compute_platform_score,
    get_dataset_governance_score,
)

router = APIRouter(prefix="/governance", tags=["governance"])


@router.get("/score", response_model=GovernancePlatformScoreOut)
def get_platform_governance_score(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("dashboard:read")),
):
    """Platform-wide governance health score and KPI rollup."""
    result = compute_platform_score(db)
    write_audit_log(
        db,
        user_id=actor.email,
        action="governance_score_view",
        entity="platform",
        old_value="",
        new_value=str(result.get("overall_score", 0)),
    )
    db.commit()
    return result


@router.get("/dashboard", response_model=GovernanceDashboardOut)
def get_governance_dashboard(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("dashboard:read")),
):
    """Executive governance dashboard with scores, gaps, trends, and recommendations."""
    result = build_governance_dashboard(db)
    write_audit_log(
        db,
        user_id=actor.email,
        action="governance_dashboard_view",
        entity="platform",
        old_value="",
        new_value=str(result.get("overall_score", 0)),
    )
    db.commit()
    return result


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
