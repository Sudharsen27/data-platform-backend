from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user
from app.models import LineageEdge, LineageNode, User
from app.schemas import LineageEdgeOut, LineageGraphOut, LineageNodeOut

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
