import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import (
    AICopilotActionLog,
    CatalogAsset,
    Rule,
    RuleRecommendation,
    RuleRecommendationHistory,
    User,
)
from app.schemas import (
    RuleRecommendDatasetOut,
    RuleRecommendationHistoryOut,
    RuleRecommendationOut,
    RuleRecommendationUpdateIn,
    RuleRecommendFieldIn,
    RuleRecommendFieldOut,
    RuleRecommendIn,
    RuleRecommendationsPageOut,
)
from app.services.audit_log import write_audit_log
from app.services.rule_recommendation_service import (
    persist_recommendations,
    recommend_rules_for_dataset,
    recommend_rules_for_field,
    serialize_recommendation,
)

router = APIRouter(prefix="/rules", tags=["rules"])


def _log_rule_recommendation(
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


def _rec_out(row: RuleRecommendation) -> RuleRecommendationOut:
    return RuleRecommendationOut(**serialize_recommendation(row))


@router.post("/recommend", response_model=RuleRecommendDatasetOut)
def recommend_dataset_rules(
    body: RuleRecommendIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:read")),
):
    result = recommend_rules_for_dataset(db, body.dataset_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    saved_rows = persist_recommendations(
        db,
        catalog_asset_id=body.dataset_id,
        recommendations=result.get("recommended_rules") or [],
        actor_email=actor.email,
        source_engine=result.get("source_engine", "heuristics"),
    )
    saved_by_key = {
        (r.field_name.lower(), r.rule_text.lower()): serialize_recommendation(r) for r in saved_rows
    }
    enriched_rules = []
    for item in result.get("recommended_rules") or []:
        key = ((item.get("field_name") or "").lower(), (item.get("rule_text") or "").lower())
        enriched_rules.append({**item, **saved_by_key.get(key, {})})
    result["recommended_rules"] = enriched_rules
    summary = (
        f"Generated {len(result.get('recommended_rules') or [])} rule recommendations "
        f"for {result.get('dataset_name', body.dataset_id)}"
    )
    _log_rule_recommendation(
        db,
        actor=actor,
        action_key="rule_recommend",
        entity=f"catalog_asset:{body.dataset_id}",
        summary=summary,
        payload={
            "dataset_id": body.dataset_id,
            "rule_count": len(result.get("recommended_rules") or []),
            "source_engine": result.get("source_engine"),
            "risk_analysis": result.get("risk_analysis"),
        },
    )
    db.commit()
    return result


@router.post("/recommend-field", response_model=RuleRecommendFieldOut)
def recommend_field_rules(
    body: RuleRecommendFieldIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:read")),
):
    try:
        result = recommend_rules_for_field(
            db,
            field_name=body.field_name,
            dataset_id=body.dataset_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if body.dataset_id is not None:
        saved_rows = persist_recommendations(
            db,
            catalog_asset_id=body.dataset_id,
            recommendations=result.get("rules") or [],
            actor_email=actor.email,
            source_engine=result.get("source_engine", "heuristics"),
        )
        saved_by_key = {
            (r.field_name.lower(), r.rule_text.lower()): serialize_recommendation(r)
            for r in saved_rows
        }
        enriched = []
        for item in result.get("rules") or []:
            key = ((item.get("field_name") or "").lower(), (item.get("rule_text") or "").lower())
            enriched.append({**item, **saved_by_key.get(key, {})})
        result["rules"] = enriched

    summary = f"Generated {len(result.get('rules') or [])} rules for field {body.field_name}"
    _log_rule_recommendation(
        db,
        actor=actor,
        action_key="rule_recommend_field",
        entity=f"field:{body.field_name}",
        summary=summary,
        payload={
            "field_name": body.field_name,
            "dataset_id": body.dataset_id,
            "rule_count": len(result.get("rules") or []),
            "source_engine": result.get("source_engine"),
        },
    )
    db.commit()
    return result


@router.get("/recommendations", response_model=RuleRecommendationsPageOut)
def list_recommendations(
    dataset_id: int | None = Query(default=None),
    field_name: str = Query(default=""),
    status: str = Query(default=""),
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:read")),
):
    q = db.query(RuleRecommendation).order_by(RuleRecommendation.updated_at.desc())
    if dataset_id is not None:
        q = q.filter(RuleRecommendation.catalog_asset_id == dataset_id)
    if field_name.strip():
        q = q.filter(RuleRecommendation.field_name == field_name.strip().lower())
    if status.strip():
        q = q.filter(RuleRecommendation.status == status.strip().lower())
    rows = q.limit(100).all()
    return RuleRecommendationsPageOut(items=[_rec_out(r) for r in rows], total=len(rows))


@router.post("/recommendations/{rec_id}/approve", response_model=RuleRecommendationOut)
def approve_recommendation(
    rec_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(RuleRecommendation).filter(RuleRecommendation.id == rec_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recommendation not found")
    if row.status == "approved":
        return _rec_out(row)

    old_snapshot = serialize_recommendation(row)
    rule = Rule(
        field=row.field_name or "record",
        rule=row.rule_text,
        status="active",
        created_by=actor.email,
        updated_at=datetime.utcnow(),
    )
    db.add(rule)
    db.flush()

    row.status = "approved"
    row.approved_rule_id = rule.id
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()

    db.add(
        RuleRecommendationHistory(
            recommendation_id=row.id,
            action="approve",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_recommendation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="rule_recommendation_approve",
        entity=f"rule_recommendation:{row.id}",
        old_value=old_snapshot.get("status", ""),
        new_value=f"approved; rule_id={rule.id}",
    )
    db.commit()
    db.refresh(row)
    return _rec_out(row)


@router.post("/recommendations/{rec_id}/reject", response_model=RuleRecommendationOut)
def reject_recommendation(
    rec_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(RuleRecommendation).filter(RuleRecommendation.id == rec_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recommendation not found")

    old_snapshot = serialize_recommendation(row)
    row.status = "rejected"
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()

    db.add(
        RuleRecommendationHistory(
            recommendation_id=row.id,
            action="reject",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_recommendation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="rule_recommendation_reject",
        entity=f"rule_recommendation:{row.id}",
        old_value=old_snapshot.get("status", ""),
        new_value="rejected",
    )
    db.commit()
    db.refresh(row)
    return _rec_out(row)


@router.put("/recommendations/{rec_id}", response_model=RuleRecommendationOut)
def update_recommendation(
    rec_id: int,
    body: RuleRecommendationUpdateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(RuleRecommendation).filter(RuleRecommendation.id == rec_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recommendation not found")

    old_snapshot = serialize_recommendation(row)
    if body.rule_text is not None:
        row.rule_text = body.rule_text.strip()
    if body.rule_type is not None:
        row.rule_type = body.rule_type.strip().lower()
    if body.confidence is not None:
        row.confidence = max(0, min(100, body.confidence))
    if body.business_reason is not None:
        row.business_reason = body.business_reason.strip()
    if body.governance_importance is not None:
        row.governance_importance = body.governance_importance.strip()
    if body.compliance_impact is not None:
        row.compliance_impact = body.compliance_impact.strip()
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()

    db.add(
        RuleRecommendationHistory(
            recommendation_id=row.id,
            action="edit",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_recommendation(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="rule_recommendation_edit",
        entity=f"rule_recommendation:{row.id}",
        old_value=old_snapshot.get("rule_text", ""),
        new_value=row.rule_text[:500],
    )
    db.commit()
    db.refresh(row)
    return _rec_out(row)


@router.get("/recommendations/{rec_id}/history", response_model=list[RuleRecommendationHistoryOut])
def recommendation_history(
    rec_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:read")),
):
    row = db.query(RuleRecommendation).filter(RuleRecommendation.id == rec_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Recommendation not found")
    rows = (
        db.query(RuleRecommendationHistory)
        .filter(RuleRecommendationHistory.recommendation_id == rec_id)
        .order_by(RuleRecommendationHistory.acted_at.desc())
        .limit(50)
        .all()
    )
    return rows
