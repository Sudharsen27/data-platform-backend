import os
from csv import writer
from datetime import datetime, timezone
from io import StringIO
from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.orm import Session
from jose import JWTError

from app.database import Base, SessionLocal, engine, get_db
from app.models import (
    MasterData,
    PipelineRun,
    QuarantineData,
    Rule,
    StewardshipQueue,
    SyncJob,
    User,
)
from app.schemas import (
    PipelineRunOut,
    QuarantinePageOut,
    QuarantineOut,
    QuarantineUpdate,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SchedulerToggleRequest,
    StewardshipActionRequest,
    StewardshipOut,
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
from app.routes.auth import router as auth_router
from app.routes.audit import router as audit_router
from app.routes.users import router as users_router
from app.deps.auth import get_current_user, require_admin
from app.services.audit_log import write_audit_log
from app.utils.jwt import verify_token
from app.utils.security import hash_password

app = FastAPI()
app.include_router(auth_router)
app.include_router(audit_router)
app.include_router(users_router)

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


def get_user_id_from_request(request: Request):
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return "unknown"

    token = auth_header.replace("Bearer ", "").strip()
    if not token:
        return "unknown"

    try:
        payload = verify_token(token)
        return payload.get("sub") or "unknown"
    except JWTError:
        return "unknown"


def seed_data(db: Session):
    db.execute(
        text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(32) NOT NULL DEFAULT 'user'"
        )
    )
    db.commit()

    admin_emails = [
        part.strip().lower()
        for part in os.getenv("ADMIN_EMAILS", "").split(",")
        if part.strip()
    ]

    # Optional bootstrap user creation for empty prod DBs.
    # Set ADMIN_BOOTSTRAP_PASSWORD to enable automatic admin account creation.
    bootstrap_password = os.getenv("ADMIN_BOOTSTRAP_PASSWORD", "").strip()
    for admin_email in admin_emails:
        existing_admin_user = (
            db.query(User).filter(User.email.ilike(admin_email)).first()
        )
        if existing_admin_user:
            continue
        if not bootstrap_password:
            continue

        db.add(
            User(
                full_name="Platform Admin",
                email=admin_email,
                company_name="",
                password_hash=hash_password(bootstrap_password),
                role="admin",
                is_active=True,
            )
        )
    db.commit()

    for admin_email in admin_emails:
        db.execute(
            text("UPDATE users SET role = 'admin' WHERE LOWER(email) = :email"),
            {"email": admin_email},
        )
    db.commit()

    db.execute(
        text(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT true"
        )
    )
    db.commit()

    db.execute(
        text(
            "ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS action VARCHAR(64) DEFAULT 'unknown'"
        )
    )
    db.execute(
        text(
            "ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS entity VARCHAR(128) DEFAULT ''"
        )
    )
    db.commit()
    fc = db.execute(
        text(
            "SELECT 1 FROM information_schema.columns WHERE table_name = 'audit_logs' AND column_name = 'field_changed'"
        )
    ).fetchone()
    if fc:
        db.execute(
            text(
                "UPDATE audit_logs SET action = 'update', entity = 'quarantine:' || COALESCE(field_changed, '') WHERE COALESCE(field_changed, '') <> '' OR action = 'unknown'"
            )
        )
        db.commit()
        db.execute(text("ALTER TABLE audit_logs DROP COLUMN IF EXISTS field_changed"))
        db.commit()

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
    db.execute(
        text(
            "CREATE TABLE IF NOT EXISTS stewardship_queue (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR DEFAULT '', issue VARCHAR DEFAULT '', status VARCHAR DEFAULT 'pending')"
        )
    )
    db.execute(
        text(
            "CREATE TABLE IF NOT EXISTS master_data (id SERIAL PRIMARY KEY, source_queue_id INTEGER NOT NULL, name VARCHAR NOT NULL, email VARCHAR DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
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


@app.get("/quarantine/export")
def export_quarantine_table_csv(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    rows = db.query(QuarantineData).order_by(QuarantineData.id.asc()).all()
    buffer = StringIO()
    csv_writer = writer(buffer)
    csv_writer.writerow(["id", "name", "email", "error", "match_status"])
    for row in rows:
        csv_writer.writerow([row.id, row.name, row.email, row.error, row.match_status])

    buffer.seek(0)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=quarantine_records.csv"},
    )


@app.get("/stewardship", response_model=List[StewardshipOut])
def get_stewardship_records(db: Session = Depends(get_db)):
    return db.query(StewardshipQueue).order_by(StewardshipQueue.id.asc()).all()


@app.post("/stewardship/approve")
def approve_stewardship_record(
    payload: StewardshipActionRequest,
    db: Session = Depends(get_db),
):
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Stewardship record not found")

    if record.status == "approved":
        return {"message": "Record is already approved", "record": record}

    db.add(
        MasterData(
            source_queue_id=record.id,
            name=record.name,
            email=record.email,
            created_at=datetime.utcnow(),
        )
    )
    record.status = "approved"
    db.commit()
    db.refresh(record)
    return {"message": "Record approved and moved to master data", "record": record}


@app.post("/stewardship/reject")
def reject_stewardship_record(
    payload: StewardshipActionRequest,
    db: Session = Depends(get_db),
):
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Stewardship record not found")

    record.status = "rejected"
    db.commit()
    db.refresh(record)
    return {"message": "Record rejected", "record": record}


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
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    record = db.query(QuarantineData).filter(QuarantineData.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    user_id = current_user.email
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
        write_audit_log(
            db,
            user_id=user_id,
            action="update",
            entity=f"quarantine:{payload.id}/{field_name}",
            old_value=str(old_value or ""),
            new_value=str(new_value or ""),
        )

    db.commit()
    db.refresh(record)

    return {"message": "Record updated successfully", "record": record}


@app.get("/rules", response_model=List[RuleOut])
def get_rules(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return db.query(Rule).order_by(Rule.id.asc()).all()


@app.post("/rules")
@app.post("/rules/add")
def add_rule(
    payload: RuleCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    new_rule = Rule(
        field=payload.field,
        rule=payload.rule,
        status=payload.status,
        created_by=payload.created_by or current_user.email,
        updated_at=datetime.utcnow(),
    )
    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)
    write_audit_log(
        db,
        user_id=current_user.email,
        action="create",
        entity=f"rule:{new_rule.id}",
        old_value="",
        new_value=f"field={new_rule.field}; status={new_rule.status}; text={new_rule.rule[:200]}",
    )
    db.commit()
    return {"message": "Rule added successfully", "rule": new_rule}


@app.post("/rules/update")
def update_rule(
    payload: RuleUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rule_item = db.query(Rule).filter(Rule.id == payload.id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    old_value = (
        f"field={rule_item.field}; status={rule_item.status}; text={rule_item.rule[:200]}"
    )
    rule_item.field = payload.field
    rule_item.rule = payload.rule
    rule_item.status = payload.status
    rule_item.updated_at = datetime.utcnow()
    new_value = f"field={rule_item.field}; status={rule_item.status}; text={rule_item.rule[:200]}"
    write_audit_log(
        db,
        user_id=current_user.email,
        action="update",
        entity=f"rule:{payload.id}",
        old_value=old_value,
        new_value=new_value,
    )
    db.commit()
    db.refresh(rule_item)
    return {"message": "Rule updated successfully", "rule": rule_item}


@app.delete("/rules/{rule_id}")
def delete_rule(
    rule_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    rule_item = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    old_value = (
        f"field={rule_item.field}; status={rule_item.status}; text={rule_item.rule[:200]}"
    )
    write_audit_log(
        db,
        user_id=current_user.email,
        action="delete",
        entity=f"rule:{rule_id}",
        old_value=old_value,
        new_value="",
    )
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


@app.post("/pipeline/run")
def trigger_pipeline_run(
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    state = get_pipeline_state()
    if state["status"] == "running":
        raise HTTPException(status_code=409, detail="Pipeline is already running")

    try:
        result = run_pipeline(db)
        write_audit_log(
            db,
            user_id=current_user.email,
            action="pipeline_run",
            entity="pipeline",
            old_value="",
            new_value=str(result)[:2000],
        )
        db.commit()
        return result
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