import json

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import AICopilotActionLog, CatalogAsset, User
from app.schemas import (
    ClassificationAnalyzeDatasetIn,
    ClassificationAnalyzeFieldIn,
    ClassificationDatasetOut,
    ClassificationFieldOut,
)
from app.services.audit_log import write_audit_log
from app.services.classification_ai_service import (
    enhance_dataset_classification,
    enhance_field_classification,
)
from app.services.data_classification_service import analyze_dataset, classify_field_name

router = APIRouter(prefix="/classification", tags=["classification"])


def _log_classification(
    db: Session,
    *,
    actor: User,
    entity: str,
    classification_type: str,
    summary: str,
    payload: dict,
) -> None:
    db.add(
        AICopilotActionLog(
            action_key="classification_analyze",
            user_id=actor.email,
            status="success",
            summary=summary[:500],
            payload=json.dumps(payload, ensure_ascii=True),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_classification_analyze",
        entity=entity,
        old_value=classification_type,
        new_value=summary[:500],
    )


@router.post("/analyze-field", response_model=ClassificationFieldOut)
def analyze_field_classification(
    body: ClassificationAnalyzeFieldIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    field_name = (body.field_name or "").strip()
    if not field_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="field_name is required")

    dataset_name = body.dataset_name
    description = body.description
    tags = body.tags
    if body.dataset_id is not None:
        asset = db.query(CatalogAsset).filter(CatalogAsset.id == body.dataset_id).first()
        if asset:
            dataset_name = asset.name
            description = asset.description
            tags = asset.tags

    try:
        result = classify_field_name(
            field_name,
            dataset_name=dataset_name,
            description=description,
            tags=tags,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    enhanced = enhance_field_classification(result)
    summary = f"Field {field_name} classified as {enhanced['classification']} ({enhanced['confidence']}%)"
    _log_classification(
        db,
        actor=actor,
        entity=f"field:{field_name}",
        classification_type=enhanced["classification"],
        summary=summary,
        payload={
            "field_name": field_name,
            "classification": enhanced["classification"],
            "confidence": enhanced["confidence"],
            "dataset_id": body.dataset_id,
        },
    )
    db.commit()
    return enhanced


@router.post("/analyze-dataset", response_model=ClassificationDatasetOut)
def analyze_dataset_classification(
    body: ClassificationAnalyzeDatasetIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    result = analyze_dataset(db, body.dataset_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    enhanced = enhance_dataset_classification(result)
    summary = enhanced.get("summary") or f"Dataset classified as {enhanced['dataset_classification']}"
    _log_classification(
        db,
        actor=actor,
        entity=f"catalog_asset:{body.dataset_id}",
        classification_type=enhanced["dataset_classification"],
        summary=summary,
        payload={
            "dataset_id": body.dataset_id,
            "risk_score": enhanced["risk_score"],
            "pii_count": enhanced["pii_count"],
            "sensitive_count": enhanced["sensitive_count"],
        },
    )
    db.commit()
    return enhanced
