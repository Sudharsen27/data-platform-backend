import csv
import io
import re
from datetime import datetime, timezone
from threading import Thread

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session

from app.database import SessionLocal, get_db
from app.deps.auth import require_admin
from app.models import IngestionJob, MasterData, User
from app.schemas import IngestionJobOut
from app.services.audit_log import write_audit_log

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


def _norm_email(country: str, item_type: str, order_id: int) -> str:
    slug_country = re.sub(r"[^a-z0-9]+", "-", (country or "").strip().lower()).strip("-") or "unknown"
    slug_item = re.sub(r"[^a-z0-9]+", "-", (item_type or "").strip().lower()).strip("-") or "item"
    return f"{slug_country}.{slug_item}.{order_id % 50000}@sales.local"


def _run_ingestion_job(job_id: int, csv_content: bytes, actor_email: str) -> None:
    db = SessionLocal()
    try:
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if not job:
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        db.add(job)
        db.commit()

        batch: list[MasterData] = []
        total = 0
        inserted = 0
        text_stream = io.TextIOWrapper(
            io.BytesIO(csv_content),
            encoding="utf-8-sig",
            newline="",
        )
        try:
            reader = csv.DictReader(text_stream)
            for row in reader:
                total += 1
                country = (row.get("Country") or "").strip()
                item_type = (row.get("Item Type") or "").strip()
                order_id_raw = (row.get("Order ID") or "").strip()
                if not order_id_raw.isdigit():
                    continue
                order_id = int(order_id_raw)
                name = f"{country} {item_type}".strip() or "Unknown Record"
                email = _norm_email(country, item_type, order_id)
                batch.append(
                    MasterData(
                        source_queue_id=order_id,
                        name=name,
                        email=email,
                    )
                )
                if len(batch) >= 10000:
                    db.bulk_save_objects(batch)
                    db.commit()
                    inserted += len(batch)
                    batch.clear()
                if total % 5000 == 0:
                    job.total_rows = total
                    job.processed_rows = total
                    job.inserted_rows = inserted
                    db.add(job)
                    db.commit()
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                inserted += len(batch)
        finally:
            text_stream.close()

        job.total_rows = total
        job.processed_rows = total
        job.inserted_rows = inserted
        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc)
        db.add(job)
        write_audit_log(
            db,
            user_id=actor_email,
            action="ingestion_complete",
            entity=f"ingestion:{job.id}",
            old_value="",
            new_value=f"inserted={inserted}",
        )
        db.commit()
    except Exception as exc:  # pragma: no cover
        job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
        if job:
            job.status = "failed"
            job.error_message = str(exc)[:500]
            job.completed_at = datetime.now(timezone.utc)
            db.add(job)
            db.commit()
    finally:
        db.close()


@router.post("/upload", response_model=IngestionJobOut)
async def upload_csv_and_start_job(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    actor: User = Depends(require_admin),
):
    filename = file.filename or "upload.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    job = IngestionJob(
        filename=filename,
        status="queued",
        created_by=actor.email,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    write_audit_log(
        db,
        user_id=actor.email,
        action="ingestion_start",
        entity=f"ingestion:{job.id}",
        old_value="",
        new_value=filename,
    )
    db.commit()

    worker = Thread(
        target=_run_ingestion_job,
        args=(job.id, content, actor.email),
        daemon=True,
    )
    worker.start()
    return job


@router.get("/jobs/latest", response_model=IngestionJobOut | None)
def get_latest_ingestion_job(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    return db.query(IngestionJob).order_by(IngestionJob.id.desc()).first()


@router.get("/jobs", response_model=list[IngestionJobOut])
def list_ingestion_jobs(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    return (
        db.query(IngestionJob)
        .order_by(IngestionJob.id.desc())
        .limit(limit)
        .all()
    )


@router.get("/jobs/{job_id}", response_model=IngestionJobOut)
def get_ingestion_job(
    job_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    job = db.query(IngestionJob).filter(IngestionJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
