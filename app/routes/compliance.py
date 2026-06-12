from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import User
from app.schemas import ComplianceDashboardOut, ComplianceScoreOut
from app.services.audit_log import write_audit_log
from app.services.compliance_privacy_service import (
    build_compliance_dashboard,
    compute_compliance_score,
)

router = APIRouter(prefix="/compliance", tags=["compliance"])


@router.get("/score", response_model=ComplianceScoreOut)
def get_compliance_score(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("dashboard:read")),
):
    """Platform compliance & privacy score summary."""
    result = compute_compliance_score(db)
    write_audit_log(
        db,
        user_id=actor.email,
        action="compliance_score_view",
        entity="platform",
        old_value="",
        new_value=str(result.get("compliance_score", 0)),
    )
    db.commit()
    return result


@router.get("/dashboard", response_model=ComplianceDashboardOut)
def get_compliance_dashboard(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("dashboard:read")),
):
    """Compliance & privacy dashboard with KPIs, risks, and recommendations."""
    result = build_compliance_dashboard(db)
    write_audit_log(
        db,
        user_id=actor.email,
        action="compliance_dashboard_view",
        entity="platform",
        old_value="",
        new_value=str(result.get("compliance_score", 0)),
    )
    db.commit()
    return result
