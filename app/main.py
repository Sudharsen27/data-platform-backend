import os
from csv import writer
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session
from jose import jwt

from app.database import Base, SessionLocal, engine, get_db
from app.models import AuditLog, PipelineRun, QuarantineData, Rule, SyncJob
from app.schemas import (
    AuditLogOut,
    PipelineRunOut,
    QuarantinePageOut,
    QuarantineOut,
    QuarantineUpdate,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SchedulerToggleRequest,
    SyncJobOut,
)
from app.services.snowflake_analytics import get_quarantine_analytics
from app.db.snowflake import get_snowflake_connection
from app.services.pipeline import get_pipeline_state, run_pipeline
from app.services.sync_jobs import run_scheduled_sync_job, run_sync_job
from app.services.sync_scheduler import (
    configure_sync_schedule,
    disable_sync_schedule,
    get_scheduler_state,
)

app = FastAPI()

frontend_origin = os.getenv("FRONTEND_URL", "").strip()
allowed_origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]
if frontend_origin:
    allowed_origins.append(frontend_origin)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mdm-secret-key-change-this")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60
HARDCODED_EMAIL = "admin@mdm.com"
HARDCODED_PASSWORD = "admin123"


class LoginRequest(BaseModel):
    email: str
    password: str


def get_user_id_from_request(request: Request):
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return "unknown"

    token = auth_header.replace("Bearer ", "").strip()
    if not token:
        return "unknown"

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub", "unknown")
    except Exception:
        return "unknown"


def seed_data(db: Session):
    db.execute(
        text(
            "ALTER TABLE rules ADD COLUMN IF NOT EXISTS created_by VARCHAR DEFAULT 'system'"
        )
    )
    db.execute(
        text(
            "ALTER TABLE rules ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        )
    )
    db.execute(
        text(
            "ALTER TABLE quarantine_data ADD COLUMN IF NOT EXISTS match_status VARCHAR DEFAULT 'new'"
        )
    )
    db.commit()

    quarantine_count = db.execute(text("SELECT COUNT(*) FROM quarantine_data")).scalar()
    if quarantine_count == 0:
        db.add_all(
            [
                QuarantineData(name="John", email="", error="Email missing"),
                QuarantineData(name="Alice", email="alice@mail.com", error=""),
                QuarantineData(
                    name="Mark",
                    email="markmail.com",
                    error="Invalid email format",
                ),
            ]
        )

    rules_count = db.execute(text("SELECT COUNT(*) FROM rules")).scalar()
    if rules_count == 0:
        db.add_all(
            [
                Rule(field="email", rule="Email cannot be null", status="active"),
                Rule(field="name", rule="Name must be at least 2 chars", status="active"),
                Rule(field="phone", rule="Phone format must be valid", status="inactive"),
            ]
        )

    db.commit()


@app.on_event("startup")
def on_startup():
    db = SessionLocal()
    try:
        seed_data(db)
    finally:
        db.close()

    scheduler_interval = int(os.getenv("SYNC_INTERVAL_MINUTES", "10"))
    if os.getenv("SYNC_SCHEDULER_ENABLED", "false").lower() == "true":
        configure_sync_schedule(
            lambda: run_scheduled_sync_job(SessionLocal),
            interval_minutes=scheduler_interval,
        )


@app.get("/")
def home():
    return {"message": "Backend running 🚀"}


@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    database_status = "ok"
    snowflake_status = "skipped"

    try:
        db.execute(text("SELECT 1"))
    except Exception:
        database_status = "failed"

    snowflake_required = [
        os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        os.getenv("SNOWFLAKE_USER", "").strip(),
        os.getenv("SNOWFLAKE_PASSWORD", "").strip(),
    ]
    if all(snowflake_required):
        try:
            connection = get_snowflake_connection()
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()
                connection.close()
            snowflake_status = "ok"
        except Exception:
            snowflake_status = "failed"

    overall_status = (
        "ok" if database_status == "ok" and snowflake_status in {"ok", "skipped"} else "degraded"
    )

    return {
        "status": overall_status,
        "api": "ok",
        "database": database_status,
        "snowflake": snowflake_status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/auth/login")
def login(payload: LoginRequest):
    if payload.email != HARDCODED_EMAIL or payload.password != HARDCODED_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    token_payload = {"sub": payload.email, "exp": expire}
    access_token = jwt.encode(token_payload, SECRET_KEY, algorithm=ALGORITHM)

    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/dashboard")
def dashboard(db: Session = Depends(get_db)):
    latest_job = db.query(SyncJob).order_by(SyncJob.id.desc()).first()
    scheduler_state = get_scheduler_state()
    analytics = get_quarantine_analytics()
    failed_records = analytics["failed_records"]
    success_records = analytics["success_records"]

    return {
        "success_rate": analytics["success_rate"],
        "failed_records": failed_records,
        "active_jobs": 1 if scheduler_state["enabled"] else 0,
        "last_sync_job": {
            "status": latest_job.status,
            "start_time": latest_job.start_time,
            "end_time": latest_job.end_time,
            "quarantine_rows_synced": latest_job.quarantine_rows_synced,
            "rules_synced": latest_job.rules_synced,
        }
        if latest_job
        else None,
        "success_vs_failed": [
            {"name": "Success", "value": success_records},
            {"name": "Failed", "value": failed_records},
        ],
        "records_trend": [
            {"day": "Mon", "records": 280},
            {"day": "Tue", "records": 350},
            {"day": "Wed", "records": 300},
            {"day": "Thu", "records": 420},
            {"day": "Fri", "records": 480},
            {"day": "Sat", "records": 360},
            {"day": "Sun", "records": 210},
        ],
        "error_distribution": [
            {"type": item["error"], "count": item["count"]}
            for item in analytics["error_distribution"]
        ],
    }


@app.get("/quarantine", response_model=List[QuarantineOut])
def get_quarantine(db: Session = Depends(get_db)):
    return db.query(QuarantineData).order_by(QuarantineData.id.asc()).all()


@app.get("/quarantine/paged", response_model=QuarantinePageOut)
def get_quarantine_paged(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    total = db.query(QuarantineData).count()
    items = (
        db.query(QuarantineData)
        .order_by(QuarantineData.id.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {
        "items": items,
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@app.post("/quarantine/update")
def update_quarantine(
    payload: QuarantineUpdate,
    request: Request,
    db: Session = Depends(get_db),
):
    record = db.query(QuarantineData).filter(QuarantineData.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    user_id = get_user_id_from_request(request)
    changed_fields = []

    if record.name != payload.name:
        changed_fields.append(("name", record.name, payload.name))
    if record.email != payload.email:
        changed_fields.append(("email", record.email, payload.email))
    if record.error != payload.error:
        changed_fields.append(("error", record.error, payload.error))

    record.name = payload.name
    record.email = payload.email
    record.error = payload.error

    for field_name, old_value, new_value in changed_fields:
        db.add(
            AuditLog(
                user_id=user_id,
                field_changed=field_name,
                old_value=str(old_value or ""),
                new_value=str(new_value or ""),
                timestamp=datetime.utcnow(),
            )
        )

    db.commit()
    db.refresh(record)

    return {"message": "Record updated successfully", "record": record}


@app.get("/rules", response_model=List[RuleOut])
def get_rules(db: Session = Depends(get_db)):
    return db.query(Rule).order_by(Rule.id.asc()).all()


@app.post("/rules")
@app.post("/rules/add")
def add_rule(payload: RuleCreate, db: Session = Depends(get_db)):
    new_rule = Rule(
        field=payload.field,
        rule=payload.rule,
        status=payload.status,
        created_by=payload.created_by,
        updated_at=datetime.utcnow(),
    )
    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)
    return {"message": "Rule added successfully", "rule": new_rule}


@app.post("/rules/update")
def update_rule(payload: RuleUpdate, db: Session = Depends(get_db)):
    rule_item = db.query(Rule).filter(Rule.id == payload.id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule_item.field = payload.field
    rule_item.rule = payload.rule
    rule_item.status = payload.status
    rule_item.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(rule_item)
    return {"message": "Rule updated successfully", "rule": rule_item}


@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule_item = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    db.delete(rule_item)
    db.commit()
    return {"message": "Rule deleted successfully"}


@app.post("/sync/snowflake")
def trigger_snowflake_sync(db: Session = Depends(get_db)):
    try:
        return run_sync_job(db, triggered_by="manual")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Snowflake sync failed: {error}")


@app.get("/sync/jobs", response_model=List[SyncJobOut])
def get_sync_jobs(db: Session = Depends(get_db)):
    return db.query(SyncJob).order_by(SyncJob.id.desc()).limit(20).all()


@app.post("/sync/jobs/{job_id}/retry")
def retry_sync_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(SyncJob).filter(SyncJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")

    try:
        return run_sync_job(db, triggered_by=f"retry:{job_id}")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Snowflake retry failed: {error}")


@app.post("/sync/scheduler")
def toggle_sync_scheduler(payload: SchedulerToggleRequest):
    if payload.enabled:
        configure_sync_schedule(
            lambda: run_scheduled_sync_job(SessionLocal),
            interval_minutes=payload.interval_minutes,
        )
    else:
        disable_sync_schedule()

    return get_scheduler_state()


@app.get("/sync/scheduler")
def get_sync_scheduler():
    return get_scheduler_state()


@app.get("/analytics/snowflake")
def snowflake_analytics():
    try:
        return get_quarantine_analytics()
    except Exception as error:
        raise HTTPException(
            status_code=500, detail=f"Snowflake analytics query failed: {error}"
        )


@app.get("/export/quarantine.csv")
def export_quarantine_csv(db: Session = Depends(get_db)):
    rows = db.query(QuarantineData).order_by(QuarantineData.id.asc()).all()
    buffer = StringIO()
    csv_writer = writer(buffer)
    csv_writer.writerow(["id", "name", "email", "error"])
    for row in rows:
        csv_writer.writerow([row.id, row.name, row.email, row.error])

    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quarantine_records.csv"},
    )


@app.get("/export/analytics.csv")
def export_analytics_csv():
    analytics = get_quarantine_analytics()
    buffer = StringIO()
    csv_writer = writer(buffer)
    csv_writer.writerow(["metric", "value"])
    csv_writer.writerow(["total_records", analytics["total_records"]])
    csv_writer.writerow(["success_records", analytics["success_records"]])
    csv_writer.writerow(["failed_records", analytics["failed_records"]])
    csv_writer.writerow(["success_rate", analytics["success_rate"]])

    for error_item in analytics["error_distribution"]:
        csv_writer.writerow([f"error:{error_item['error']}", error_item["count"]])

    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=analytics_summary.csv"},
    )


@app.get("/audit/logs", response_model=List[AuditLogOut])
def get_audit_logs(db: Session = Depends(get_db)):
    return db.query(AuditLog).order_by(AuditLog.timestamp.desc()).limit(100).all()


@app.post("/pipeline/run")
def trigger_pipeline_run(db: Session = Depends(get_db)):
    state = get_pipeline_state()
    if state["status"] == "running":
        raise HTTPException(status_code=409, detail="Pipeline is already running")

    try:
        return run_pipeline(db)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error))
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {error}")


@app.get("/pipeline/status")
def get_pipeline_status():
    return get_pipeline_state()


@app.get("/pipeline/runs", response_model=List[PipelineRunOut])
def get_pipeline_runs(db: Session = Depends(get_db)):
    return db.query(PipelineRun).order_by(PipelineRun.id.desc()).limit(100).all()