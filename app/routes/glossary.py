import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import AICopilotActionLog, CatalogAsset, GlossaryEntry, GlossaryHistory, User
from app.schemas import (
    GlossaryDatasetOut,
    GlossaryEntriesPageOut,
    GlossaryEntryOut,
    GlossaryFieldOut,
    GlossaryGenerateDatasetIn,
    GlossaryGenerateFieldIn,
    GlossaryHistoryOut,
    GlossarySaveIn,
    GlossaryUpdateIn,
)
from app.services.audit_log import write_audit_log
from app.services.glossary_generator_service import (
    generate_dataset_glossary,
    generate_field_glossary,
    get_saved_glossary_entries,
    serialize_glossary_entry,
)

router = APIRouter(prefix="/glossary", tags=["glossary"])

_ALLOWED_STATUSES = frozenset({"draft", "approved"})


def _log_glossary_action(
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


def _entry_out(row: GlossaryEntry) -> GlossaryEntryOut:
    data = serialize_glossary_entry(row)
    return GlossaryEntryOut(**data)


@router.post("/generate", response_model=GlossaryFieldOut)
def generate_field_definition(
    body: GlossaryGenerateFieldIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    field_name = (body.field_name or "").strip()
    if not field_name:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="field_name is required")

    dataset_name = body.dataset_name
    description = body.description
    tags = body.tags
    domain = ""
    pii_tier = ""
    classification = ""
    if body.dataset_id is not None:
        asset = db.query(CatalogAsset).filter(CatalogAsset.id == body.dataset_id).first()
        if asset:
            dataset_name = asset.name
            description = asset.description
            tags = asset.tags
            domain = asset.domain
            pii_tier = asset.pii_tier
            from app.services.data_classification_service import classify_field_name

            classification = classify_field_name(
                field_name,
                dataset_name=asset.name,
                description=asset.description,
                tags=asset.tags,
            ).get("classification", "")

    result = generate_field_glossary(
        field_name=field_name,
        dataset_name=dataset_name,
        field_type=body.field_type,
        description=description,
        tags=tags,
        classification=classification,
        domain=domain,
        pii_tier=pii_tier,
    )
    summary = f"Generated glossary for field {field_name}: {result.get('title', '')}"
    _log_glossary_action(
        db,
        actor=actor,
        action_key="glossary_generate",
        entity=f"field:{field_name}",
        summary=summary,
        payload={
            "field_name": field_name,
            "dataset_id": body.dataset_id,
            "dataset_name": dataset_name,
            "title": result.get("title"),
            "definition": result.get("definition"),
            "source_engine": result.get("source_engine"),
        },
    )
    db.commit()
    return result


@router.post("/generate-dataset", response_model=GlossaryDatasetOut)
def generate_dataset_definition(
    body: GlossaryGenerateDatasetIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    result = generate_dataset_glossary(db, body.dataset_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    summary = f"Generated dataset glossary for {result.get('dataset_title', body.dataset_id)}"
    _log_glossary_action(
        db,
        actor=actor,
        action_key="glossary_generate_dataset",
        entity=f"catalog_asset:{body.dataset_id}",
        summary=summary,
        payload={
            "dataset_id": body.dataset_id,
            "dataset_title": result.get("dataset_title"),
            "field_count": len(result.get("field_glossaries") or []),
            "source_engine": result.get("source_engine"),
        },
    )
    db.commit()
    return result


@router.get("/entries", response_model=GlossaryEntriesPageOut)
def list_glossary_entries(
    dataset_id: int | None = Query(default=None),
    field_name: str = Query(default=""),
    status: str = Query(default=""),
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    rows = get_saved_glossary_entries(
        db,
        catalog_asset_id=dataset_id,
        field_name=field_name,
        status=status,
    )
    items = [_entry_out(row) for row in rows]
    return GlossaryEntriesPageOut(items=items, total=len(items))


@router.post("/save", response_model=GlossaryEntryOut)
def save_glossary_entry(
    body: GlossarySaveIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    status_value = (body.status or "approved").strip().lower()
    if status_value not in _ALLOWED_STATUSES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="status must be draft or approved",
        )

    asset = db.query(CatalogAsset).filter(CatalogAsset.id == body.catalog_asset_id).first()
    if not asset:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Catalog asset not found")

    field_key = (body.field_name or "").strip().lower()
    existing = (
        db.query(GlossaryEntry)
        .filter(
            GlossaryEntry.catalog_asset_id == body.catalog_asset_id,
            GlossaryEntry.field_name == field_key,
        )
        .first()
    )
    examples_json = json.dumps(body.examples or [], ensure_ascii=True)
    now = datetime.utcnow()

    if existing:
        old_snapshot = serialize_glossary_entry(existing)
        existing.title = body.title.strip()
        existing.definition = body.definition.strip()
        existing.usage = (body.usage or "").strip()
        existing.governance_notes = (body.governance_notes or "").strip()
        existing.examples = examples_json
        existing.status = status_value
        existing.updated_by = actor.email
        existing.updated_at = now
        row = existing
        action = "update"
    else:
        row = GlossaryEntry(
            catalog_asset_id=body.catalog_asset_id,
            field_name=field_key,
            title=body.title.strip(),
            definition=body.definition.strip(),
            usage=(body.usage or "").strip(),
            governance_notes=(body.governance_notes or "").strip(),
            examples=examples_json,
            status=status_value,
            source_engine="user_saved",
            created_by=actor.email,
            updated_by=actor.email,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        db.flush()
        old_snapshot = {}
        action = "create"

    db.add(
        GlossaryHistory(
            glossary_entry_id=row.id,
            action=action,
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_glossary_entry(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=now,
        )
    )
    entity_label = f"field:{field_key}" if field_key else f"dataset:{asset.asset_key}"
    write_audit_log(
        db,
        user_id=actor.email,
        action="glossary_save",
        entity=f"catalog_asset:{body.catalog_asset_id}:{entity_label}",
        old_value=old_snapshot.get("definition", ""),
        new_value=row.definition[:500],
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.put("/entries/{entry_id}", response_model=GlossaryEntryOut)
def update_glossary_entry(
    entry_id: int,
    body: GlossaryUpdateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(GlossaryEntry).filter(GlossaryEntry.id == entry_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Glossary entry not found")

    old_snapshot = serialize_glossary_entry(row)
    if body.title is not None:
        row.title = body.title.strip()
    if body.definition is not None:
        row.definition = body.definition.strip()
    if body.usage is not None:
        row.usage = body.usage.strip()
    if body.governance_notes is not None:
        row.governance_notes = body.governance_notes.strip()
    if body.examples is not None:
        row.examples = json.dumps(body.examples, ensure_ascii=True)
    if body.status is not None:
        status_value = body.status.strip().lower()
        if status_value not in _ALLOWED_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="status must be draft or approved",
            )
        row.status = status_value
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()

    db.add(
        GlossaryHistory(
            glossary_entry_id=row.id,
            action="edit",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_glossary_entry(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="glossary_edit",
        entity=f"glossary_entry:{row.id}",
        old_value=old_snapshot.get("definition", ""),
        new_value=row.definition[:500],
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.get("/entries/{entry_id}/history", response_model=list[GlossaryHistoryOut])
def glossary_entry_history(
    entry_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    row = db.query(GlossaryEntry).filter(GlossaryEntry.id == entry_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Glossary entry not found")
    rows = (
        db.query(GlossaryHistory)
        .filter(GlossaryHistory.glossary_entry_id == entry_id)
        .order_by(GlossaryHistory.acted_at.desc())
        .limit(50)
        .all()
    )
    return rows
