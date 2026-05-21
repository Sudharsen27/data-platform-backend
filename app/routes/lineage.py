from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import LineageEdge, LineageNode, User
from app.schemas import LineageEdgeOut, LineageGraphOut, LineageImpactOut, LineageNodeOut
from app.services.lineage_impact import analyze_field_impact, analyze_node_impact

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
