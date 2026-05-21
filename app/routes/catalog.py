from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import CatalogAsset, LineageNode, User
from app.schemas import CatalogAssetCreate, CatalogAssetOut, LineageImpactOut
from app.services.lineage_impact import analyze_node_impact

router = APIRouter(prefix="/catalog", tags=["catalog"])


@router.get("/assets", response_model=list[CatalogAssetOut])
def list_catalog_assets(
    q: str = Query("", max_length=200),
    domain: str = Query("", max_length=120),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("catalog:read")),
):
    query = db.query(CatalogAsset).order_by(CatalogAsset.name.asc())
    if domain.strip():
        query = query.filter(CatalogAsset.domain.ilike(f"%{domain.strip()}%"))
    if q.strip():
        term = f"%{q.strip()}%"
        query = query.filter(
            or_(
                CatalogAsset.name.ilike(term),
                CatalogAsset.asset_key.ilike(term),
                CatalogAsset.description.ilike(term),
                CatalogAsset.tags.ilike(term),
                CatalogAsset.owner_email.ilike(term),
            )
        )
    return query.limit(200).all()


@router.get("/assets/{asset_id}", response_model=CatalogAssetOut)
def get_catalog_asset(
    asset_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("catalog:read")),
):
    row = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    return row


@router.get("/assets/{asset_id}/lineage-impact", response_model=LineageImpactOut)
def get_catalog_asset_lineage_impact(
    asset_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("catalog:read")),
):
    row = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")
    lineage_key = (row.lineage_node_key or "").strip()
    if not lineage_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Asset has no lineage_node_key; link it to a lineage node first.",
        )
    result = analyze_node_impact(db, lineage_key)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No lineage node with key '{lineage_key}'.",
        )
    return result


@router.post("/assets", response_model=CatalogAssetOut)
def create_catalog_asset(
    payload: CatalogAssetCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("catalog:write")),
):
    asset_key = payload.asset_key.strip()
    if not asset_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="asset_key is required")
    existing = db.query(CatalogAsset).filter(CatalogAsset.asset_key == asset_key.lower()).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An asset with this key already exists",
        )
    lineage_key = (payload.lineage_node_key or "").strip()
    if lineage_key:
        node = db.query(LineageNode).filter(LineageNode.key == lineage_key).first()
        if not node:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No lineage node with key '{lineage_key}'. Link to an existing node key.",
            )

    sla_hours = payload.sla_hours if payload.sla_hours and payload.sla_hours > 0 else 24
    row = CatalogAsset(
        asset_key=asset_key.lower(),
        name=payload.name.strip(),
        asset_type=(payload.asset_type or "table").strip(),
        domain=(payload.domain or "").strip(),
        owner_email=(payload.owner_email or "").strip().lower(),
        description=(payload.description or "").strip(),
        tags=(payload.tags or "").strip(),
        pii_tier=(payload.pii_tier or "internal").strip().lower(),
        lineage_node_key=lineage_key,
        schema_fields=(payload.schema_fields or "").strip(),
        sla_hours=sla_hours,
        contract_version=(payload.contract_version or "1.0").strip(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row
