import json

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import AICopilotActionLog, CatalogAsset, LineageEdge, LineageNode, User
from app.schemas import (
    LineageEdgeOut,
    LineageGraphOut,
    LineageImpactAnalyzeIn,
    LineageImpactAnalyzeOut,
    LineageImpactOut,
    LineageImpactScoreOut,
    LineageNodeOut,
)
from app.services.audit_log import write_audit_log
from app.services.lineage_explanation_service import explain_lineage_impact
from app.services.lineage_impact import analyze_field_impact, analyze_node_impact
from app.services.lineage_impact_service import (
    analyze_asset_impact_by_id,
    analyze_comprehensive_by_field,
    analyze_comprehensive_by_node,
    resolve_impact_from_question,
)

router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/nodes", response_model=list[LineageNodeOut])
def get_lineage_nodes(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return db.query(LineageNode).order_by(LineageNode.id.asc()).all()


@router.get("/edges", response_model=list[LineageEdgeOut])
def get_lineage_edges(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()


@router.get("/graph", response_model=LineageGraphOut)
def get_lineage_graph(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    nodes = db.query(LineageNode).order_by(LineageNode.id.asc()).all()
    edges = db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()
    return {"nodes": nodes, "edges": edges}


@router.get("/impact", response_model=LineageImpactOut)
def get_lineage_impact(
    node_key: str = Query("", max_length=200),
    field: str = Query("", max_length=120),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("lineage:read")),
):
    node_key = node_key.strip()
    field = field.strip()
    if not node_key and not field:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide node_key or field for impact analysis.",
        )
    if node_key and field:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide only one of node_key or field.",
        )

    if node_key:
        result = analyze_node_impact(db, node_key)
        if result is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No lineage node with key '{node_key}'.",
            )
        return result

    result = analyze_field_impact(db, field)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="field is required for field-scoped impact.",
        )
    return result


def _log_lineage_impact_analysis(
    db: Session,
    *,
    actor: User,
    analysis_type: str,
    entity: str,
    summary: str,
    payload: dict,
) -> None:
    db.add(
        AICopilotActionLog(
            action_key="lineage_impact_analyze",
            user_id=actor.email,
            status="success",
            summary=summary[:500],
            payload=json.dumps(payload, ensure_ascii=True),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_lineage_impact_analyze",
        entity=entity,
        old_value=analysis_type,
        new_value=summary[:500],
    )


@router.get("/impact/{asset_id}", response_model=LineageImpactScoreOut)
def get_asset_lineage_impact(
    asset_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("lineage:read")),
):
    result = analyze_asset_impact_by_id(db, asset_id)
    if result is None:
        asset = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
        if not asset:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Asset has no lineage_node_key; link it to a lineage node first.",
        )

    summary = (
        f"Impact score {result['impact_score']} for {result['source_asset']}: "
        f"{result['downstream_count']} downstream, {result['upstream_count']} upstream."
    )
    _log_lineage_impact_analysis(
        db,
        actor=actor,
        analysis_type="asset",
        entity=f"catalog_asset:{asset_id}",
        summary=summary,
        payload={
            "asset_id": asset_id,
            "impact_score": result["impact_score"],
            "downstream_count": result["downstream_count"],
        },
    )
    db.commit()
    return result


@router.post("/impact/analyze", response_model=LineageImpactAnalyzeOut)
def analyze_lineage_impact_question(
    body: LineageImpactAnalyzeIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("lineage:read")),
):
    question = (body.question or "").strip()
    if not question and not body.asset_id and not body.node_key.strip() and not body.field.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Provide question, asset_id, node_key, or field.",
        )

    impact = None
    if body.asset_id is not None:
        impact = analyze_asset_impact_by_id(db, body.asset_id)
    elif body.node_key.strip():
        impact = analyze_comprehensive_by_node(db, body.node_key.strip())
    elif body.field.strip():
        impact = analyze_comprehensive_by_field(db, body.field.strip())
    elif question:
        impact = resolve_impact_from_question(db, question)

    if impact is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Could not resolve lineage impact for the given input.",
        )

    explanation = explain_lineage_impact(
        question=question or impact.get("summary") or "Analyze lineage impact",
        impact=impact,
    )

    entity = f"lineage:{impact.get('anchor_node_key') or impact.get('field') or 'unknown'}"
    _log_lineage_impact_analysis(
        db,
        actor=actor,
        analysis_type="question",
        entity=entity,
        summary=explanation["analysis"][:500],
        payload={
            "question": question[:500],
            "impact_score": impact.get("impact_score"),
            "source_engine": explanation.get("source_engine"),
        },
    )
    db.commit()

    return {
        "analysis": explanation["analysis"],
        "impacts": explanation["impacts"],
        "impact_score": impact.get("impact_score", 0),
        "downstream_count": impact.get("downstream_count", 0),
        "upstream_count": impact.get("upstream_count", 0),
        "source_engine": explanation.get("source_engine", "heuristics"),
        "impact_detail": impact,
    }
