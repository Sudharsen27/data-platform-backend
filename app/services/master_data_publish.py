"""Publish approved stewardship rows to golden master data (with email survivorship)."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import MasterData, StewardshipQueue


def publish_stewardship_to_master(
    db: Session, record: StewardshipQueue
) -> tuple[MasterData, str, bool]:
    """
    Upsert golden record by email when duplicate exists; otherwise insert.
    Returns (master_row, message, merged_into_existing).
    """
    email_key = (record.email or "").strip().lower()
    existing = None
    if email_key:
        existing = (
            db.query(MasterData)
            .filter(func.lower(MasterData.email) == email_key)
            .order_by(MasterData.id.asc())
            .first()
        )

    if existing:
        existing.name = record.name
        existing.email = record.email
        existing.source_queue_id = record.id
        existing.created_at = datetime.utcnow()
        db.flush()
        return (
            existing,
            f"Merged into golden record #{existing.id} (survivorship by email).",
            True,
        )

    row = MasterData(
        source_queue_id=record.id,
        name=record.name,
        email=record.email,
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return (row, f"Published as new golden record #{row.id}.", False)
