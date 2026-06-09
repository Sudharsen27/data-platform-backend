import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import PlainTextResponse, Response
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import require_permission
from app.models import (
    AICopilotActionLog,
    CatalogAsset,
    DatasetDocumentation,
    DatasetDocumentationHistory,
    User,
)
from app.schemas import (
    DocumentationEntryOut,
    DocumentationExportOut,
    DocumentationGenerateIn,
    DocumentationHistoryOut,
    DocumentationOut,
    DocumentationSaveIn,
    DocumentationUpdateIn,
)
from app.services.audit_log import write_audit_log
from app.services.dataset_documentation_service import (
    documentation_to_markdown,
    documentation_to_text,
    generate_dataset_documentation,
    get_saved_documentation,
    serialize_documentation_row,
)

router = APIRouter(prefix="/documentation", tags=["documentation"])

_ALLOWED_STATUSES = frozenset({"draft", "approved"})


def _log_documentation_action(
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


def _content_from_doc(doc: dict) -> str:
    return json.dumps(
        {
            k: doc.get(k)
            for k in (
                "summary",
                "business_description",
                "purpose",
                "key_fields",
                "owner_recommendation",
                "governance_notes",
                "classification_summary",
                "quality_expectations",
                "usage_guidelines",
                "compliance_considerations",
                "dataset_key",
            )
        },
        ensure_ascii=True,
    )


def _entry_out(row: DatasetDocumentation) -> DocumentationEntryOut:
    data = serialize_documentation_row(row)
    return DocumentationEntryOut(**data)


@router.post("/generate", response_model=DocumentationOut)
def generate_documentation(
    body: DocumentationGenerateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    result = generate_dataset_documentation(db, body.dataset_id)
    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    summary = f"Generated documentation for {result.get('title', body.dataset_id)}"
    _log_documentation_action(
        db,
        actor=actor,
        action_key="documentation_generate",
        entity=f"catalog_asset:{body.dataset_id}",
        summary=summary,
        payload={
            "dataset_id": body.dataset_id,
            "title": result.get("title"),
            "summary": result.get("summary", "")[:200],
            "source_engine": result.get("source_engine"),
        },
    )
    db.commit()
    return result


@router.get("/dataset/{dataset_id}", response_model=DocumentationEntryOut)
def get_dataset_documentation(
    dataset_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    row = get_saved_documentation(db, dataset_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Documentation not found")
    return _entry_out(row)


@router.post("/save", response_model=DocumentationEntryOut)
def save_documentation(
    body: DocumentationSaveIn,
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

    doc_payload = body.model_dump()
    doc_payload["dataset_key"] = asset.asset_key
    content_json = _content_from_doc(doc_payload)
    now = datetime.utcnow()

    existing = get_saved_documentation(db, body.catalog_asset_id)
    if existing:
        old_snapshot = serialize_documentation_row(existing)
        existing.title = body.title.strip()
        existing.content = content_json
        existing.status = status_value
        existing.updated_by = actor.email
        existing.updated_at = now
        row = existing
        action = "update"
    else:
        row = DatasetDocumentation(
            catalog_asset_id=body.catalog_asset_id,
            title=body.title.strip(),
            content=content_json,
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
        DatasetDocumentationHistory(
            documentation_id=row.id,
            action=action,
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_documentation_row(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=now,
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="documentation_save",
        entity=f"catalog_asset:{body.catalog_asset_id}",
        old_value=old_snapshot.get("summary", ""),
        new_value=body.summary[:500],
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.put("/entries/{entry_id}", response_model=DocumentationEntryOut)
def update_documentation(
    entry_id: int,
    body: DocumentationUpdateIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    row = db.query(DatasetDocumentation).filter(DatasetDocumentation.id == entry_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Documentation not found")

    old_snapshot = serialize_documentation_row(row)
    merged = {**old_snapshot, **body.model_dump(exclude_unset=True)}
    if body.status is not None:
        status_value = body.status.strip().lower()
        if status_value not in _ALLOWED_STATUSES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="status must be draft or approved",
            )
        row.status = status_value
    row.title = (merged.get("title") or row.title).strip()
    row.content = _content_from_doc(merged)
    row.updated_by = actor.email
    row.updated_at = datetime.utcnow()

    db.add(
        DatasetDocumentationHistory(
            documentation_id=row.id,
            action="edit",
            old_value=json.dumps(old_snapshot, ensure_ascii=True, default=str),
            new_value=json.dumps(serialize_documentation_row(row), ensure_ascii=True, default=str),
            acted_by=actor.email,
            acted_at=datetime.utcnow(),
        )
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="documentation_edit",
        entity=f"documentation:{row.id}",
        old_value=old_snapshot.get("summary", ""),
        new_value=merged.get("summary", "")[:500],
    )
    db.commit()
    db.refresh(row)
    return _entry_out(row)


@router.get("/entries/{entry_id}/history", response_model=list[DocumentationHistoryOut])
def documentation_history(
    entry_id: int,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    row = db.query(DatasetDocumentation).filter(DatasetDocumentation.id == entry_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Documentation not found")
    rows = (
        db.query(DatasetDocumentationHistory)
        .filter(DatasetDocumentationHistory.documentation_id == entry_id)
        .order_by(DatasetDocumentationHistory.acted_at.desc())
        .limit(50)
        .all()
    )
    return rows


@router.get("/export/{dataset_id}")
def export_documentation(
    dataset_id: int,
    format: str = Query(default="markdown", alias="format"),
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    row = get_saved_documentation(db, dataset_id)
    doc = serialize_documentation_row(row) if row else generate_dataset_documentation(db, dataset_id)
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    fmt = (format or "markdown").strip().lower()
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == dataset_id).first()
    slug = (asset.asset_key if asset else f"dataset-{dataset_id}").replace(".", "_")

    if fmt == "text":
        content = documentation_to_text(doc)
        return PlainTextResponse(
            content,
            headers={"Content-Disposition": f'attachment; filename="{slug}_documentation.txt"'},
        )
    if fmt == "pdf":
        html = f"<html><body><pre>{documentation_to_text(doc)}</pre></body></html>"
        return Response(
            content=html.encode("utf-8"),
            media_type="text/html",
            headers={"Content-Disposition": f'inline; filename="{slug}_documentation.html"'},
        )
    content = documentation_to_markdown(doc)
    return PlainTextResponse(
        content,
        headers={"Content-Disposition": f'attachment; filename="{slug}_documentation.md"'},
    )


@router.post("/export", response_model=DocumentationExportOut)
def export_documentation_json(
    body: DocumentationGenerateIn,
    format: str = Query(default="markdown", alias="format"),
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    row = get_saved_documentation(db, body.dataset_id)
    doc = serialize_documentation_row(row) if row else generate_dataset_documentation(db, body.dataset_id)
    if not doc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found")

    asset = db.query(CatalogAsset).filter(CatalogAsset.id == body.dataset_id).first()
    slug = (asset.asset_key if asset else f"dataset-{body.dataset_id}").replace(".", "_")
    fmt = (format or "markdown").strip().lower()

    if fmt == "text":
        return DocumentationExportOut(
            format="text",
            filename=f"{slug}_documentation.txt",
            content=documentation_to_text(doc),
            content_type="text/plain",
        )
    if fmt == "pdf":
        return DocumentationExportOut(
            format="pdf",
            filename=f"{slug}_documentation.html",
            content=f"<html><body><pre>{documentation_to_text(doc)}</pre></body></html>",
            content_type="text/html",
        )
    return DocumentationExportOut(
        format="markdown",
        filename=f"{slug}_documentation.md",
        content=documentation_to_markdown(doc),
        content_type="text/markdown",
    )
