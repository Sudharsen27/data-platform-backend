import os
from time import perf_counter
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
    CatalogAsset,
    MasterData,
    LineageEdge,
    LineageNode,
    PipelineRun,
    QuarantineData,
    Rule,
    StewardshipQueue,
    SyncJob,
    User,
)
from app.schemas import (
    DashboardOverviewOut,
    PipelineRunOut,
    QuarantineBulkImport,
    QuarantinePageOut,
    QuarantineOut,
    QuarantineUpdate,
    RuleCreate,
    RuleOut,
    RuleUpdate,
    SchedulerJobConfigRequest,
    SchedulerOverviewOut,
    SchedulerToggleRequest,
    StewardshipActionRequest,
    StewardshipBulkActionRequest,
    StewardshipBulkOutcome,
    StewardshipOut,
    StewardshipPageOut,
    SyncJobOut,
)
from app.services.ai_insights import build_ai_insights
from app.services.dashboard_metrics import (
    build_audit_activity_feed,
    build_compliance_status,
    build_dashboard_alerts,
    build_dashboard_trends,
    build_kpi_cards,
    build_kpi_summary,
    build_sla_status,
    resolve_quarantine_analytics,
)
from app.services.snowflake_analytics import get_quarantine_analytics
from app.db.snowflake import get_snowflake_connection
from app.services.pipeline import get_pipeline_state, run_pipeline
from app.services.sync_jobs import run_sync_job
from app.services.job_scheduler import (
    any_scheduler_enabled,
    configure_job,
    configure_sync_schedule,
    count_active_jobs,
    disable_job,
    disable_sync_schedule,
    get_all_job_states,
    get_scheduler_state,
)
from app.services.scheduled_jobs import (
    run_scheduled_pipeline_job,
    run_scheduled_sync_job,
)
from app.routes.auth import router as auth_router
from app.routes.audit import router as audit_router
from app.routes.users import router as users_router
from app.routes.lineage import router as lineage_router
from app.routes.ai import router as ai_router
from app.routes.catalog import router as catalog_router
from app.routes.classification import router as classification_router
from app.routes.documentation import router as documentation_router
from app.routes.glossary import router as glossary_router
from app.routes.master_data import router as master_data_router
from app.routes.rule_recommendations import router as rule_recommendations_router
from app.routes.stewardship_remediation import router as stewardship_remediation_router
from app.routes.rules_engine import router as rules_engine_router
from app.routes.ingestion import router as ingestion_router
from app.routes.annotations import router as annotations_router
from app.routes.governance import router as governance_router
from app.services.master_data_publish import publish_stewardship_to_master
from app.deps.auth import get_current_user, require_admin, require_permission
from app.services.audit_log import write_audit_log
from app.utils.jwt import verify_token
from app.utils.security import hash_password

app = FastAPI(
    title="MDM Data Governance Platform API",
    description="mini-mdm-platform backend — verify this title in /docs or /openapi.json when debugging port 8000.",
    version="0.1.0",
)
app.include_router(auth_router)
app.include_router(audit_router)
app.include_router(users_router)
app.include_router(lineage_router)
app.include_router(ai_router)
app.include_router(catalog_router)
app.include_router(classification_router)
app.include_router(glossary_router)
app.include_router(documentation_router)
app.include_router(master_data_router)
app.include_router(rules_engine_router)
app.include_router(rule_recommendations_router)
app.include_router(stewardship_remediation_router)
app.include_router(ingestion_router)
app.include_router(annotations_router)
app.include_router(governance_router)

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


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    start = perf_counter()
    response = await call_next(request)
    duration_ms = round((perf_counter() - start) * 1000, 2)
    response.headers["X-Response-Time-Ms"] = str(duration_ms)
    return response

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
    # If the admin email already exists (e.g. registered earlier), creation is skipped
    # and the stored password is unchanged unless ADMIN_BOOTSTRAP_SYNC_PASSWORD is set.
    bootstrap_password = os.getenv("ADMIN_BOOTSTRAP_PASSWORD", "").strip()
    admin_full_name = (os.getenv("ADMIN_FULL_NAME", "Platform Admin") or "Platform Admin").strip()
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
                full_name=admin_full_name,
                email=admin_email,
                company_name="",
                password_hash=hash_password(bootstrap_password),
                role="admin",
                is_active=True,
            )
        )
    db.commit()

    sync_bootstrap = os.getenv("ADMIN_BOOTSTRAP_SYNC_PASSWORD", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if sync_bootstrap and bootstrap_password and admin_emails:
        for admin_email in admin_emails:
            user = db.query(User).filter(User.email.ilike(admin_email)).first()
            if user:
                user.password_hash = hash_password(bootstrap_password)
                db.add(user)
        db.commit()

    sync_admin_name = os.getenv("ADMIN_BOOTSTRAP_SYNC_NAME", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    if admin_full_name and admin_emails:
        for admin_email in admin_emails:
            user = db.query(User).filter(User.email.ilike(admin_email)).first()
            if not user:
                continue
            if user.full_name == admin_full_name:
                continue
            if sync_admin_name or user.full_name == "Platform Admin":
                user.full_name = admin_full_name
                db.add(user)
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
            "CREATE TABLE IF NOT EXISTS stewardship_queue (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR DEFAULT '', issue VARCHAR DEFAULT '', status VARCHAR DEFAULT 'pending', owner_email VARCHAR DEFAULT '')"
        )
    )
    db.execute(
        text(
            "ALTER TABLE stewardship_queue ADD COLUMN IF NOT EXISTS owner_email VARCHAR DEFAULT ''"
        )
    )
    db.execute(
        text(
            "CREATE TABLE IF NOT EXISTS master_data (id SERIAL PRIMARY KEY, source_queue_id INTEGER NOT NULL, name VARCHAR NOT NULL, email VARCHAR DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
    )
    db.execute(
        text(
            "CREATE TABLE IF NOT EXISTS lineage_nodes (id SERIAL PRIMARY KEY, key VARCHAR NOT NULL UNIQUE, label VARCHAR NOT NULL, node_type VARCHAR NOT NULL DEFAULT 'dataset', system VARCHAR NOT NULL DEFAULT '', layer VARCHAR NOT NULL DEFAULT '')"
        )
    )
    db.execute(
        text(
            "CREATE TABLE IF NOT EXISTS lineage_edges (id SERIAL PRIMARY KEY, source_key VARCHAR NOT NULL, target_key VARCHAR NOT NULL, transformation VARCHAR NOT NULL DEFAULT '', criticality VARCHAR NOT NULL DEFAULT 'medium')"
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

    lineage_nodes_count = db.execute(text("SELECT COUNT(*) FROM lineage_nodes")).scalar()
    if lineage_nodes_count == 0:
        db.add_all(
            [
                LineageNode(
                    key="crm.customers_raw",
                    label="CRM Customers Raw",
                    node_type="table",
                    system="CRM",
                    layer="source",
                ),
                LineageNode(
                    key="staging.customers_clean",
                    label="Customers Clean",
                    node_type="table",
                    system="Postgres",
                    layer="staging",
                ),
                LineageNode(
                    key="mdm.customer_master",
                    label="Customer Master",
                    node_type="table",
                    system="MDM",
                    layer="golden",
                ),
                LineageNode(
                    key="bi.customer_360",
                    label="Customer 360 Mart",
                    node_type="table",
                    system="Analytics",
                    layer="consumption",
                ),
            ]
        )

    lineage_edges_count = db.execute(text("SELECT COUNT(*) FROM lineage_edges")).scalar()
    if lineage_edges_count == 0:
        db.add_all(
            [
                LineageEdge(
                    source_key="crm.customers_raw",
                    target_key="staging.customers_clean",
                    transformation="standardize_email, trim_name",
                    criticality="high",
                ),
                LineageEdge(
                    source_key="staging.customers_clean",
                    target_key="mdm.customer_master",
                    transformation="match_merge_survivorship",
                    criticality="high",
                ),
                LineageEdge(
                    source_key="mdm.customer_master",
                    target_key="bi.customer_360",
                    transformation="aggregate_profile_metrics",
                    criticality="medium",
                ),
            ]
        )

    db.execute(
        text(
            "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS lineage_node_key VARCHAR NOT NULL DEFAULT ''"
        )
    )
    db.execute(
        text(
            "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS schema_fields VARCHAR NOT NULL DEFAULT ''"
        )
    )
    db.execute(
        text(
            "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS sla_hours INTEGER NOT NULL DEFAULT 24"
        )
    )
    db.execute(
        text(
            "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS contract_version VARCHAR NOT NULL DEFAULT '1.0'"
        )
    )
    db.commit()
    db.execute(
        text(
            """
            UPDATE catalog_assets AS ca
            SET lineage_node_key = ca.asset_key
            FROM lineage_nodes AS ln
            WHERE (ca.lineage_node_key IS NULL OR ca.lineage_node_key = '')
              AND ln.key = ca.asset_key
            """
        )
    )
    db.commit()

    catalog_count = db.execute(text("SELECT COUNT(*) FROM catalog_assets")).scalar()
    if catalog_count == 0:
        db.add_all(
            [
                CatalogAsset(
                    asset_key="crm.customers_raw",
                    name="CRM Customers Raw",
                    asset_type="table",
                    domain="Customer",
                    owner_email="steward-customer@example.com",
                    description="Raw customer feed from CRM before MDM processing.",
                    tags="crm,source,pii",
                    pii_tier="confidential",
                    lineage_node_key="crm.customers_raw",
                    schema_fields="customer_id,name,email,phone,created_at",
                    sla_hours=4,
                    contract_version="1.2",
                ),
                CatalogAsset(
                    asset_key="staging.customers_clean",
                    name="Customers Clean",
                    asset_type="table",
                    domain="Customer",
                    owner_email="steward-customer@example.com",
                    description="Standardized customer attributes in staging.",
                    tags="staging,mdm",
                    pii_tier="confidential",
                    lineage_node_key="staging.customers_clean",
                    schema_fields="customer_id,name,email,phone,match_status",
                    sla_hours=8,
                    contract_version="1.1",
                ),
                CatalogAsset(
                    asset_key="mdm.customer_master",
                    name="Customer Master",
                    asset_type="table",
                    domain="Customer",
                    owner_email="governance-admin@example.com",
                    description="Golden customer master used across the enterprise.",
                    tags="golden,mdm,authoritative",
                    pii_tier="restricted",
                    lineage_node_key="mdm.customer_master",
                    schema_fields="golden_id,name,email,source_system,last_updated",
                    sla_hours=24,
                    contract_version="2.0",
                ),
                CatalogAsset(
                    asset_key="bi.customer_360",
                    name="Customer 360 Mart",
                    asset_type="view",
                    domain="Analytics",
                    owner_email="analytics-owner@example.com",
                    description="Downstream mart for analytics and reporting.",
                    tags="bi,consumption",
                    pii_tier="confidential",
                    lineage_node_key="bi.customer_360",
                    schema_fields="customer_key,lifetime_value,segment,email_domain",
                    sla_hours=48,
                    contract_version="1.0",
                ),
            ]
        )

    db.commit()


def _ensure_runtime_schema(db: Session) -> None:
    """DDL that must run even when SKIP_STARTUP_SEED is set (e.g. new columns on existing DBs)."""
    bind = db.get_bind()
    if bind.dialect.name == "sqlite":
        columns = db.execute(text("PRAGMA table_info(stewardship_queue)")).fetchall()
        names = {row[1] for row in columns}
        if "owner_email" not in names:
            db.execute(
                text(
                    "ALTER TABLE stewardship_queue ADD COLUMN owner_email VARCHAR DEFAULT ''"
                )
            )
        cat_cols = db.execute(text("PRAGMA table_info(catalog_assets)")).fetchall()
        cat_names = {row[1] for row in cat_cols}
        if "schema_fields" not in cat_names:
            db.execute(
                text("ALTER TABLE catalog_assets ADD COLUMN schema_fields VARCHAR DEFAULT ''")
            )
        if "sla_hours" not in cat_names:
            db.execute(
                text("ALTER TABLE catalog_assets ADD COLUMN sla_hours INTEGER DEFAULT 24")
            )
        if "contract_version" not in cat_names:
            db.execute(
                text(
                    "ALTER TABLE catalog_assets ADD COLUMN contract_version VARCHAR DEFAULT '1.0'"
                )
            )
    else:
        db.execute(
            text(
                "ALTER TABLE stewardship_queue ADD COLUMN IF NOT EXISTS owner_email VARCHAR DEFAULT ''"
            )
        )
        db.execute(
            text(
                "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS schema_fields VARCHAR NOT NULL DEFAULT ''"
            )
        )
        db.execute(
            text(
                "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS sla_hours INTEGER NOT NULL DEFAULT 24"
            )
        )
        db.execute(
            text(
                "ALTER TABLE catalog_assets ADD COLUMN IF NOT EXISTS contract_version VARCHAR NOT NULL DEFAULT '1.0'"
            )
        )
    db.commit()


@app.on_event("startup")
def on_startup():
    db = SessionLocal()
    try:
        _ensure_runtime_schema(db)
    finally:
        db.close()

    if os.getenv("SKIP_STARTUP_SEED", "").strip().lower() in ("1", "true", "yes"):
        return
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

    pipeline_cron = os.getenv("PIPELINE_SCHEDULER_CRON", "").strip()
    if os.getenv("PIPELINE_SCHEDULER_ENABLED", "false").lower() == "true":
        if pipeline_cron:
            configure_job(
                "pipeline",
                lambda: run_scheduled_pipeline_job(SessionLocal),
                enabled=True,
                trigger_type="cron",
                cron_expression=pipeline_cron,
            )
        else:
            pipeline_interval = int(os.getenv("PIPELINE_INTERVAL_MINUTES", "60"))
            configure_job(
                "pipeline",
                lambda: run_scheduled_pipeline_job(SessionLocal),
                enabled=True,
                trigger_type="interval",
                interval_minutes=pipeline_interval,
            )


@app.get("/")
def home():
    return {"message": "Backend running 🚀"}


@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    database_status = "ok"
    snowflake_status = "skipped"
    database_latency_ms = None
    snowflake_latency_ms = None

    try:
        db_start = perf_counter()
        db.execute(text("SELECT 1"))
        database_latency_ms = round((perf_counter() - db_start) * 1000, 2)
    except Exception:
        database_status = "failed"

    from app.db.snowflake import is_snowflake_enabled

    if is_snowflake_enabled():
        try:
            sf_start = perf_counter()
            connection = get_snowflake_connection()
            try:
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()
                connection.close()
            snowflake_status = "ok"
            snowflake_latency_ms = round((perf_counter() - sf_start) * 1000, 2)
        except Exception:
            snowflake_status = "failed"

    overall_status = (
        "ok" if database_status == "ok" and snowflake_status in {"ok", "skipped"} else "degraded"
    )

    return {
        "status": overall_status,
        "api": "ok",
        "database": database_status,
        "database_latency_ms": database_latency_ms,
        "snowflake": snowflake_status,
        "snowflake_latency_ms": snowflake_latency_ms,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/dashboard")
def dashboard(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    latest_job = db.query(SyncJob).order_by(SyncJob.id.desc()).first()
    analytics = get_quarantine_analytics()
    failed_records = analytics["failed_records"]
    success_records = analytics["success_records"]

    return {
        "success_rate": analytics["success_rate"],
        "failed_records": failed_records,
        "active_jobs": count_active_jobs(),
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


@app.get("/dashboard/overview", response_model=DashboardOverviewOut)
def dashboard_overview(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    latest_job = db.query(SyncJob).order_by(SyncJob.id.desc()).first()
    scheduler_enabled = any_scheduler_enabled()
    analytics = resolve_quarantine_analytics(db)
    lineage_nodes = db.query(LineageNode).order_by(LineageNode.id.asc()).all()
    lineage_edges = db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()
    stewardship_items = (
        db.query(StewardshipQueue).order_by(StewardshipQueue.id.desc()).limit(8).all()
    )
    recent_jobs = db.query(SyncJob).order_by(SyncJob.id.desc()).limit(8).all()
    pipeline_status = get_pipeline_state()

    return {
        "kpi_summary": build_kpi_summary(analytics, scheduler_enabled),
        "kpi_cards": build_kpi_cards(db, analytics, scheduler_enabled),
        "alerts": build_dashboard_alerts(db, analytics),
        "compliance": build_compliance_status(db, analytics),
        "trends": build_dashboard_trends(db, analytics),
        "audit_activity": build_audit_activity_feed(db),
        "sla": build_sla_status(db, analytics, pipeline_status, scheduler_enabled),
        "last_sync_job": {
            "status": latest_job.status,
            "start_time": latest_job.start_time,
            "end_time": latest_job.end_time,
            "quarantine_rows_synced": latest_job.quarantine_rows_synced,
            "rules_synced": latest_job.rules_synced,
        }
        if latest_job
        else None,
        "pipeline_status": pipeline_status,
        "recent_jobs": recent_jobs,
        "lineage": {
            "nodes": lineage_nodes,
            "edges": lineage_edges,
        },
        "stewardship": stewardship_items,
        "ai_insights": build_ai_insights(db, analytics),
    }


@app.get("/quarantine", response_model=List[QuarantineOut])
def get_quarantine(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
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


_ALLOWED_STEWARDSHIP_STATUS = frozenset({"all", "pending", "approved", "rejected"})
_STEWARDSHIP_BULK_MAX_IDS = 500
_STEWARDSHIP_EXPORT_MAX_ROWS = 100_000


def _stewardship_status_param(status: str) -> str:
    raw = (status or "pending").strip().lower()
    if raw not in _ALLOWED_STEWARDSHIP_STATUS:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of: {', '.join(sorted(_ALLOWED_STEWARDSHIP_STATUS))}",
        )
    return raw


def _filtered_stewardship_query(db: Session, raw_status: str):
    q = db.query(StewardshipQueue)
    if raw_status != "all":
        q = q.filter(StewardshipQueue.status == raw_status)
    return q


def _normalize_bulk_ids(ids: list[int]) -> list[int]:
    if not ids:
        raise HTTPException(status_code=400, detail="ids must not be empty")
    if len(ids) > _STEWARDSHIP_BULK_MAX_IDS:
        raise HTTPException(
            status_code=400,
            detail=f"At most {_STEWARDSHIP_BULK_MAX_IDS} ids per bulk request",
        )
    seen = set()
    out = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


@app.get("/stewardship", response_model=StewardshipPageOut)
def get_stewardship_records(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    status: str = Query(
        "pending",
        description="Filter: all, pending, approved, rejected (default pending)",
    ),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("stewardship:manage")),
):
    raw = _stewardship_status_param(status)
    total = _filtered_stewardship_query(db, raw).count()
    items = (
        _filtered_stewardship_query(db, raw)
        .order_by(StewardshipQueue.id.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    pending_total = (
        db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()
    )
    return {
        "items": items,
        "total": total,
        "offset": offset,
        "limit": limit,
        "pending_total": pending_total,
    }


@app.get("/stewardship/export")
def export_stewardship_csv(
    status: str = Query(
        "pending",
        description="Same as list filter: all, pending, approved, rejected",
    ),
    max_rows: int = Query(
        50_000,
        ge=1,
        le=_STEWARDSHIP_EXPORT_MAX_ROWS,
        description="Cap export size (max 100000)",
    ),
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    raw = _stewardship_status_param(status)
    rows = (
        _filtered_stewardship_query(db, raw)
        .order_by(StewardshipQueue.id.asc())
        .limit(max_rows)
        .all()
    )
    buffer = StringIO()
    csv_writer = writer(buffer)
    csv_writer.writerow(["id", "name", "email", "issue", "status", "owner_email"])
    for row in rows:
        csv_writer.writerow(
            [row.id, row.name, row.email, row.issue, row.status, row.owner_email or ""]
        )
    buffer.seek(0)
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_export_csv",
        entity="stewardship_queue",
        old_value=f"filter={raw}",
        new_value=f"rows={len(rows)} max_rows={max_rows}",
    )
    db.commit()
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": 'attachment; filename="stewardship_queue_export.csv"',
        },
    )


@app.post("/stewardship/approve")
def approve_stewardship_record(
    payload: StewardshipActionRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Stewardship record not found")

    if record.status == "approved":
        return {"message": "Record is already approved", "record": record}

    prev = record.status
    master_row, publish_msg, _merged = publish_stewardship_to_master(db, record)
    record.status = "approved"
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_approve",
        entity=f"stewardship_queue:{payload.id}",
        old_value=prev,
        new_value=f"approved; {publish_msg}",
    )
    db.commit()
    db.refresh(record)
    return {
        "message": publish_msg,
        "master_id": master_row.id,
        "record": record,
    }


@app.post("/stewardship/reject")
def reject_stewardship_record(
    payload: StewardshipActionRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Stewardship record not found")

    prev = record.status
    record.status = "rejected"
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_reject",
        entity=f"stewardship_queue:{payload.id}",
        old_value=prev,
        new_value="rejected",
    )
    db.commit()
    db.refresh(record)
    return {"message": "Record rejected", "record": record}


@app.post("/stewardship/bulk-approve", response_model=StewardshipBulkOutcome)
def bulk_approve_stewardship(
    payload: StewardshipBulkActionRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    ids = _normalize_bulk_ids(payload.ids)
    success_count = 0
    skipped_not_pending = 0
    missing_count = 0
    for sid in ids:
        record = db.query(StewardshipQueue).filter(StewardshipQueue.id == sid).first()
        if not record:
            missing_count += 1
            continue
        if record.status != "pending":
            skipped_not_pending += 1
            continue
        publish_stewardship_to_master(db, record)
        record.status = "approved"
        success_count += 1
    summary = f"bulk_approve success={success_count} skipped={skipped_not_pending} missing={missing_count} id_sample={ids[:20]}"
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_bulk_approve",
        entity="stewardship_queue",
        old_value=f"n_ids={len(ids)}",
        new_value=summary[:2000],
    )
    db.commit()
    return {
        "success_count": success_count,
        "skipped_not_pending": skipped_not_pending,
        "missing_count": missing_count,
    }


@app.post("/stewardship/bulk-reject", response_model=StewardshipBulkOutcome)
def bulk_reject_stewardship(
    payload: StewardshipBulkActionRequest,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    ids = _normalize_bulk_ids(payload.ids)
    success_count = 0
    skipped_not_pending = 0
    missing_count = 0
    for sid in ids:
        record = db.query(StewardshipQueue).filter(StewardshipQueue.id == sid).first()
        if not record:
            missing_count += 1
            continue
        if record.status != "pending":
            skipped_not_pending += 1
            continue
        record.status = "rejected"
        success_count += 1
    summary = f"bulk_reject success={success_count} skipped={skipped_not_pending} missing={missing_count} id_sample={ids[:20]}"
    write_audit_log(
        db,
        user_id=actor.email,
        action="stewardship_bulk_reject",
        entity="stewardship_queue",
        old_value=f"n_ids={len(ids)}",
        new_value=summary[:2000],
    )
    db.commit()
    return {
        "success_count": success_count,
        "skipped_not_pending": skipped_not_pending,
        "missing_count": missing_count,
    }


@app.get("/quarantine/paged", response_model=QuarantinePageOut)
def get_quarantine_paged(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=1000),
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
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


_MAX_QUARANTINE_IMPORT_ROWS = 500


@app.post("/quarantine/import")
def import_quarantine_rows(
    payload: QuarantineBulkImport,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_admin),
):
    """Bulk-load quarantine rows (e.g. from CSV processing or demos). Audited."""
    n = len(payload.rows)
    if n == 0:
        raise HTTPException(status_code=400, detail="No rows provided")
    if n > _MAX_QUARANTINE_IMPORT_ROWS:
        raise HTTPException(
            status_code=400,
            detail=f"Too many rows; maximum is {_MAX_QUARANTINE_IMPORT_ROWS}",
        )

    for row in payload.rows:
        db.add(
            QuarantineData(
                name=row.name.strip(),
                email=(row.email or "").strip(),
                error=(row.error or "").strip(),
                match_status="new",
            )
        )

    write_audit_log(
        db,
        user_id=current_user.email,
        action="quarantine_bulk_import",
        entity="quarantine_data",
        old_value="",
        new_value=f"imported_count={n}",
    )
    db.commit()
    return {"message": f"Imported {n} row(s)", "count": n}


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
def trigger_snowflake_sync(
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    from app.db.snowflake import is_snowflake_enabled

    if not is_snowflake_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "Snowflake sync is disabled. Set SNOWFLAKE_ENABLED=true with valid credentials, "
                "or use Postgres-only mode (SNOWFLAKE_ENABLED=false) after your trial ends."
            ),
        )
    try:
        return run_sync_job(db, triggered_by="manual")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Snowflake sync failed: {error}")


@app.get("/sync/jobs", response_model=List[SyncJobOut])
def get_sync_jobs(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return db.query(SyncJob).order_by(SyncJob.id.desc()).limit(20).all()


@app.post("/sync/jobs/{job_id}/retry")
def retry_sync_job(
    job_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_admin),
):
    from app.db.snowflake import is_snowflake_enabled

    job = db.query(SyncJob).filter(SyncJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")

    if not is_snowflake_enabled():
        raise HTTPException(
            status_code=503,
            detail=(
                "Snowflake sync is disabled. Set SNOWFLAKE_ENABLED=true with valid credentials, "
                "or use Postgres-only mode (SNOWFLAKE_ENABLED=false) after your trial ends."
            ),
        )

    try:
        return run_sync_job(db, triggered_by=f"retry:{job_id}")
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Snowflake retry failed: {error}")


@app.post("/sync/scheduler")
def toggle_sync_scheduler(
    payload: SchedulerToggleRequest,
    _: User = Depends(require_admin),
):
    if payload.enabled:
        configure_sync_schedule(
            lambda: run_scheduled_sync_job(SessionLocal),
            interval_minutes=payload.interval_minutes,
        )
    else:
        disable_sync_schedule()

    return get_scheduler_state()


@app.get("/sync/scheduler")
def get_sync_scheduler(_: User = Depends(get_current_user)):
    return get_scheduler_state()


def _apply_scheduler_config(payload: SchedulerJobConfigRequest):
    job_type = payload.job_type
    if payload.enabled:
        try:
            configure_job(
                job_type,
                _scheduler_callback_for(job_type),
                enabled=True,
                trigger_type=payload.trigger_type,
                interval_minutes=payload.interval_minutes,
                cron_expression=payload.cron_expression,
            )
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error))
    else:
        disable_job(job_type)
    return get_all_job_states()


def _scheduler_callback_for(job_type: str):
    if job_type == "pipeline":
        return lambda: run_scheduled_pipeline_job(SessionLocal)
    return lambda: run_scheduled_sync_job(SessionLocal)


@app.get("/scheduler", response_model=SchedulerOverviewOut)
def get_job_scheduler(_: User = Depends(require_admin)):
    return {"jobs": get_all_job_states()}


@app.put("/scheduler", response_model=SchedulerOverviewOut)
def configure_job_scheduler(
    payload: SchedulerJobConfigRequest,
    _: User = Depends(require_admin),
):
    jobs = _apply_scheduler_config(payload)
    return {"jobs": jobs}


@app.post("/scheduler", response_model=SchedulerOverviewOut)
def configure_job_scheduler_post(
    payload: SchedulerJobConfigRequest,
    _: User = Depends(require_admin),
):
    jobs = _apply_scheduler_config(payload)
    return {"jobs": jobs}


@app.get("/analytics/snowflake")
def snowflake_analytics(_: User = Depends(get_current_user)):
    try:
        return get_quarantine_analytics()
    except Exception as error:
        raise HTTPException(
            status_code=500, detail=f"Snowflake analytics query failed: {error}"
        )


@app.get("/export/quarantine.csv")
def export_quarantine_csv(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
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
def export_analytics_csv(_: User = Depends(get_current_user)):
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
def get_pipeline_status(_: User = Depends(get_current_user)):
    return get_pipeline_state()


@app.get("/pipeline/runs", response_model=List[PipelineRunOut])
def get_pipeline_runs(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return db.query(PipelineRun).order_by(PipelineRun.id.desc()).limit(100).all()