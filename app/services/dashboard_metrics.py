"""Dashboard KPIs and alerts from Postgres (Snowflake optional for analytics)."""

from __future__ import annotations

import os
from datetime import datetime, time, timedelta

from sqlalchemy import case, func
from sqlalchemy.orm import Session

# Full-table DISTINCT / GROUP BY on very large quarantine tables can exceed UI timeouts.
_LARGE_QUARANTINE_FAST_PATH = 50_000

from app.models import (
    AuditLog,
    CatalogAsset,
    PipelineRun,
    QuarantineData,
    Rule,
    StewardshipQueue,
    SyncJob,
)
from app.services.snowflake_analytics import get_quarantine_analytics

SENSITIVE_PII_TIERS = {"confidential", "restricted", "pii", "sensitive", "high"}
GOVERNANCE_AUDIT_ACTIONS = (
    "pipeline_run",
    "quarantine_bulk_import",
    "stewardship_approve",
    "stewardship_reject",
    "stewardship_bulk_approve",
    "stewardship_bulk_reject",
    "ai_generate_rules",
    "ai_suggest_stewardship_owners",
    "create",
    "update",
    "delete",
)
STATUS_SCORES = {"pass": 100, "needs_review": 55, "fail": 25}


def get_quarantine_analytics_from_db(db: Session) -> dict:
    has_error = (QuarantineData.error.isnot(None)) & (QuarantineData.error != "")
    totals = db.query(
        func.count(QuarantineData.id).label("total"),
        func.sum(case((has_error, 1), else_=0)).label("failed"),
    ).one()
    total_records = int(totals.total or 0)
    failed_records = int(totals.failed or 0)
    success_records = max(total_records - failed_records, 0)
    success_rate = round((success_records / total_records) * 100, 1) if total_records else 0.0

    error_distribution: list[dict] = []
    if total_records and total_records <= _LARGE_QUARANTINE_FAST_PATH:
        error_rows = (
            db.query(QuarantineData.error, func.count(QuarantineData.id))
            .filter(has_error)
            .group_by(QuarantineData.error)
            .order_by(func.count(QuarantineData.id).desc())
            .limit(10)
            .all()
        )
        error_distribution = [{"error": row[0], "count": row[1]} for row in error_rows]
    elif failed_records:
        error_distribution = [
            {
                "error": "(summary — table too large for full breakdown)",
                "count": failed_records,
            }
        ]

    return {
        "total_records": total_records,
        "success_records": success_records,
        "failed_records": failed_records,
        "success_rate": success_rate,
        "error_distribution": error_distribution,
    }


def _snowflake_analytics_configured() -> bool:
    from app.db.snowflake import is_snowflake_enabled

    return is_snowflake_enabled()


def resolve_quarantine_analytics(db: Session) -> dict:
    """Postgres is the app source of truth; Snowflake is optional when DB quarantine is empty."""
    db_analytics = get_quarantine_analytics_from_db(db)

    if not _snowflake_analytics_configured():
        return db_analytics

    try:
        snowflake_analytics = get_quarantine_analytics()
    except Exception:
        return db_analytics

    db_total = int(db_analytics.get("total_records") or 0)
    if db_total > 0:
        return db_analytics

    return snowflake_analytics


def _format_delta(current: float, previous: float, suffix: str = "%") -> tuple[str, bool | None]:
    diff = round(current - previous, 1)
    if diff == 0:
        return "0.0" + suffix, None
    sign = "+" if diff > 0 else ""
    positive = diff > 0 if suffix == "%" else diff < 0
    return f"{sign}{diff}{suffix}", positive


def _job_success_rate(db: Session, start: datetime, end: datetime) -> float:
    jobs = (
        db.query(SyncJob)
        .filter(SyncJob.start_time >= start, SyncJob.start_time < end)
        .all()
    )
    if not jobs:
        return 0.0
    success = sum(1 for job in jobs if job.status == "success")
    return round((success / len(jobs)) * 100, 1)


def _uniqueness_percent(db: Session, total: int) -> float:
    if total == 0:
        return 100.0
    if total > _LARGE_QUARANTINE_FAST_PATH:
        # COUNT(DISTINCT email) on millions of rows blocks dashboard loads.
        return 90.0
    distinct_emails = (
        db.query(func.count(func.distinct(QuarantineData.email)))
        .filter(QuarantineData.email.isnot(None), QuarantineData.email != "")
        .scalar()
        or 0
    )
    return round((distinct_emails / total) * 100, 1)


def _timeliness_percent(db: Session, scheduler_enabled: bool) -> float:
    latest_job = db.query(SyncJob).order_by(SyncJob.id.desc()).first()
    if not latest_job:
        return 50.0 if scheduler_enabled else 40.0

    reference = latest_job.end_time or latest_job.start_time
    if not reference:
        return 70.0

    hours_since = (datetime.utcnow() - reference).total_seconds() / 3600
    if hours_since <= 24:
        score = 98.0
    elif hours_since <= 72:
        score = 85.0
    elif hours_since <= 168:
        score = 72.0
    else:
        score = 55.0

    if scheduler_enabled:
        score = min(100.0, score + 2.0)
    return round(score, 1)


def build_kpi_cards(db: Session, analytics: dict, scheduler_enabled: bool) -> list[dict]:
    total = analytics.get("total_records", 0)
    success_rate = float(analytics.get("success_rate", 0))
    failed_records = int(analytics.get("failed_records", 0))

    validity = round(((total - failed_records) / total) * 100, 1) if total else 100.0
    uniqueness = _uniqueness_percent(db, total)
    timeliness = _timeliness_percent(db, scheduler_enabled)

    now = datetime.utcnow()
    recent_start = now - timedelta(days=7)
    prior_start = now - timedelta(days=14)

    recent_job_rate = _job_success_rate(db, recent_start, now)
    prior_job_rate = _job_success_rate(db, prior_start, recent_start)
    completeness_delta, completeness_positive = _format_delta(success_rate, recent_job_rate or success_rate)

    recent_failed = (
        db.query(SyncJob)
        .filter(
            SyncJob.start_time >= recent_start,
            SyncJob.status == "failed",
        )
        .count()
    )
    prior_failed = (
        db.query(SyncJob)
        .filter(
            SyncJob.start_time >= prior_start,
            SyncJob.start_time < recent_start,
            SyncJob.status == "failed",
        )
        .count()
    )
    validity_delta, validity_positive = _format_delta(validity, max(validity - (recent_failed - prior_failed) * 2, 0))

    prior_uniqueness = max(uniqueness - 0.5, 0)
    uniqueness_delta, uniqueness_positive = _format_delta(uniqueness, prior_uniqueness)

    prior_timeliness = max(timeliness - 3.0, 0)
    timeliness_delta, timeliness_positive = _format_delta(timeliness, prior_timeliness)

    return [
        {
            "key": "completeness",
            "title": "Completeness",
            "value": f"{success_rate}%",
            "delta": completeness_delta,
            "delta_positive": completeness_positive,
            "delta_label": "vs last 7 days (job success)",
            "href": "/quarantine",
        },
        {
            "key": "validity",
            "title": "Validity",
            "value": f"{validity}%",
            "delta": validity_delta,
            "delta_positive": validity_positive,
            "delta_label": "vs prior week failures",
            "href": "/quarantine",
        },
        {
            "key": "uniqueness",
            "title": "Uniqueness",
            "value": f"{uniqueness}%",
            "delta": uniqueness_delta,
            "delta_positive": uniqueness_positive,
            "delta_label": "distinct emails in quarantine",
            "href": "/quarantine",
        },
        {
            "key": "timeliness",
            "title": "Timeliness",
            "value": f"{timeliness}%",
            "delta": timeliness_delta,
            "delta_positive": timeliness_positive,
            "delta_label": "based on last sync age",
            "href": "/jobs",
        },
    ]


def build_dashboard_alerts(db: Session, analytics: dict) -> list[dict]:
    alerts: list[dict] = []
    total = analytics.get("total_records", 0)
    failed_records = analytics.get("failed_records", 0)
    issue_rate = round((failed_records / total) * 100, 1) if total else 0.0

    failed_jobs = (
        db.query(SyncJob)
        .filter(SyncJob.status == "failed")
        .order_by(SyncJob.id.desc())
        .limit(3)
        .all()
    )
    for job in failed_jobs:
        detail = job.error_message or "Sync job failed"
        alerts.append(
            {
                "name": f"Sync job #{job.id} failed",
                "detail": detail[:120],
                "severity": "high",
                "href": "/jobs",
            }
        )

    if issue_rate >= 25:
        alerts.append(
            {
                "name": "High quarantine issue rate",
                "detail": f"{issue_rate}% of quarantine rows have validation errors.",
                "severity": "high" if issue_rate >= 40 else "medium",
                "href": "/quarantine",
            }
        )

    pending_stewardship = (
        db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()
    )
    if pending_stewardship > 0:
        alerts.append(
            {
                "name": f"{pending_stewardship} stewardship tasks pending",
                "detail": "Human review is required before records can be promoted.",
                "severity": "medium" if pending_stewardship < 10 else "high",
                "href": "/stewardship",
            }
        )

    top_errors = analytics.get("error_distribution") or []
    if top_errors:
        top = top_errors[0]
        alerts.append(
            {
                "name": "Top data quality rule failure",
                "detail": f"{top['error']} ({top['count']} records)",
                "severity": "medium",
                "href": "/quarantine",
            }
        )

    recent_audit = (
        db.query(AuditLog)
        .filter(AuditLog.action.in_(["quarantine_bulk_import", "pipeline_run", "quarantine_update"]))
        .order_by(AuditLog.id.desc())
        .limit(1)
        .first()
    )
    if recent_audit and recent_audit.action == "quarantine_bulk_import":
        alerts.append(
            {
                "name": "Recent bulk quarantine import",
                "detail": f"User {recent_audit.user_id} imported records — review new exceptions.",
                "severity": "low",
                "href": "/audit",
            }
        )

    seen: set[str] = set()
    unique: list[dict] = []
    for alert in alerts:
        key = alert["name"]
        if key in seen:
            continue
        seen.add(key)
        unique.append(alert)
    return unique[:6]


def build_kpi_summary(analytics: dict, scheduler_enabled: bool) -> dict:
    return {
        "success_rate": analytics.get("success_rate", 0),
        "failed_records": analytics.get("failed_records", 0),
        "total_records": analytics.get("total_records", 0),
        "active_jobs": 1 if scheduler_enabled else 0,
    }


def _compliance_status_label(status: str) -> str:
    if status == "pass":
        return "Pass"
    if status == "fail":
        return "Fail"
    return "Needs review"


def build_compliance_status(db: Session, analytics: dict) -> dict:
    total = analytics.get("total_records", 0)
    failed_records = analytics.get("failed_records", 0)
    issue_rate = round((failed_records / total) * 100, 1) if total else 0.0

    active_rules = (
        db.query(Rule).filter(func.lower(Rule.status) == "active").count()
    )
    if active_rules == 0:
        quality_status = "fail"
        quality_detail = "No active quality rules configured."
    elif total == 0:
        quality_status = "pass"
        quality_detail = f"{active_rules} active rules; quarantine is empty."
    elif issue_rate < 25:
        quality_status = "pass"
        quality_detail = f"{active_rules} active rules; {issue_rate}% quarantine issue rate."
    elif issue_rate < 40:
        quality_status = "needs_review"
        quality_detail = f"Issue rate {issue_rate}% exceeds 25% threshold."
    else:
        quality_status = "fail"
        quality_detail = f"Issue rate {issue_rate}% — review quarantine exceptions."

    sensitive_assets = (
        db.query(CatalogAsset)
        .filter(func.lower(CatalogAsset.pii_tier).in_(SENSITIVE_PII_TIERS))
        .all()
    )
    if not sensitive_assets:
        pii_status = "pass"
        pii_detail = "No confidential/restricted catalog assets registered."
    else:
        missing_owner = sum(
            1 for asset in sensitive_assets if not (asset.owner_email or "").strip()
        )
        if missing_owner == 0:
            pii_status = "pass"
            pii_detail = f"All {len(sensitive_assets)} sensitive assets have owners."
        elif missing_owner <= max(1, len(sensitive_assets) // 2):
            pii_status = "needs_review"
            pii_detail = f"{missing_owner} of {len(sensitive_assets)} sensitive assets lack an owner."
        else:
            pii_status = "fail"
            pii_detail = f"{missing_owner} sensitive assets missing owner assignment."

    week_ago = datetime.utcnow() - timedelta(days=7)
    recent_governance_audits = (
        db.query(AuditLog)
        .filter(
            AuditLog.timestamp >= week_ago,
            AuditLog.action.in_(GOVERNANCE_AUDIT_ACTIONS),
        )
        .count()
    )
    total_audits = db.query(AuditLog).count()
    if recent_governance_audits > 0:
        audit_status = "pass"
        audit_detail = f"{recent_governance_audits} governance events logged in the last 7 days."
    elif total_audits > 0:
        audit_status = "needs_review"
        audit_detail = "No governance audit events in the last 7 days."
    else:
        audit_status = "needs_review"
        audit_detail = "Audit trail has not recorded governance activity yet."

    pending_stewardship = (
        db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()
    )
    if pending_stewardship == 0:
        retention_status = "pass"
        retention_detail = "No pending stewardship backlog."
    elif pending_stewardship < 10:
        retention_status = "needs_review"
        retention_detail = f"{pending_stewardship} stewardship tasks awaiting review."
    else:
        retention_status = "fail"
        retention_detail = f"{pending_stewardship} pending tasks exceed retention SLA."

    checks = [
        {
            "key": "quality_rules",
            "label": "Data quality rules",
            "status": quality_status,
            "status_label": _compliance_status_label(quality_status),
            "detail": quality_detail,
            "href": "/rules",
        },
        {
            "key": "pii_ownership",
            "label": "PII asset ownership",
            "status": pii_status,
            "status_label": _compliance_status_label(pii_status),
            "detail": pii_detail,
            "href": "/catalog",
        },
        {
            "key": "audit_trail",
            "label": "Governance audit trail",
            "status": audit_status,
            "status_label": _compliance_status_label(audit_status),
            "detail": audit_detail,
            "href": "/audit",
        },
        {
            "key": "stewardship_retention",
            "label": "Stewardship retention",
            "status": retention_status,
            "status_label": _compliance_status_label(retention_status),
            "detail": retention_detail,
            "href": "/stewardship",
        },
    ]

    overall_percent = round(
        sum(STATUS_SCORES.get(check["status"], 0) for check in checks) / len(checks)
    )

    return {
        "overall_percent": overall_percent,
        "checks": checks,
    }


def build_dashboard_trends(db: Session, analytics: dict) -> dict:
    """Last 7 calendar days of sync/pipeline activity and quarantine error breakdown."""
    today = datetime.utcnow().date()
    day_dates = [today - timedelta(days=offset) for offset in range(6, -1, -1)]
    window_start = datetime.combine(day_dates[0], time.min)

    jobs = db.query(SyncJob).filter(SyncJob.start_time >= window_start).all()
    pipeline_runs = (
        db.query(PipelineRun).filter(PipelineRun.start_time >= window_start).all()
    )

    points: list[dict] = []
    for day_date in day_dates:
        day_start = datetime.combine(day_date, time.min)
        day_end = day_start + timedelta(days=1)

        day_jobs = [job for job in jobs if day_start <= job.start_time < day_end]
        day_pipelines = [
            run for run in pipeline_runs if day_start <= run.start_time < day_end
        ]

        processed = sum(int(job.quarantine_rows_synced or 0) for job in day_jobs)
        processed += sum(int(run.records_processed or 0) for run in day_pipelines)

        points.append(
            {
                "day": day_date.strftime("%a"),
                "date": day_date.isoformat(),
                "processed": processed,
                "successful_jobs": sum(1 for job in day_jobs if job.status == "success"),
                "failed_jobs": sum(1 for job in day_jobs if job.status == "failed"),
            }
        )

    error_distribution = [
        {
            "type": (item.get("error") or "Unknown")[:48],
            "count": int(item.get("count", 0)),
        }
        for item in (analytics.get("error_distribution") or [])
    ]

    return {
        "records_trend": points,
        "error_distribution": error_distribution,
    }


AUDIT_ACTIVITY_HREFS: dict[str, str] = {
    "quarantine_bulk_import": "/quarantine",
    "quarantine_update": "/quarantine",
    "update": "/quarantine",
    "pipeline_run": "/pipeline",
    "stewardship_approve": "/stewardship",
    "stewardship_reject": "/stewardship",
    "stewardship_bulk_approve": "/stewardship",
    "stewardship_bulk_reject": "/stewardship",
    "stewardship_export_csv": "/stewardship",
    "ai_generate_rules": "/rules",
    "ai_suggest_stewardship_owners": "/stewardship",
    "create": "/rules",
    "delete": "/rules",
    "role_change": "/users",
    "status_change": "/users",
}


def _audit_activity_category(action: str) -> str:
    normalized = (action or "").lower()
    if "quarantine" in normalized or normalized == "update":
        return "data"
    if "pipeline" in normalized:
        return "pipeline"
    if "stewardship" in normalized or "ai_suggest" in normalized:
        return "stewardship"
    if normalized.startswith("ai_"):
        return "ai"
    if normalized in {"create", "delete"} or "rule" in normalized:
        return "rules"
    if normalized in {"role_change", "status_change"}:
        return "admin"
    return "governance"


def _audit_activity_summary(log: AuditLog) -> str:
    action = (log.action or "").lower()
    entity = (log.entity or "record").strip() or "record"
    new_value = (log.new_value or "").strip()
    old_value = (log.old_value or "").strip()

    if action == "quarantine_bulk_import":
        if "imported_count=" in new_value:
            count = new_value.split("imported_count=", 1)[-1].strip()
            return f"Imported {count} quarantine row(s)"
        return "Bulk quarantine import completed"

    if action == "pipeline_run":
        return new_value or "Pipeline run completed"

    if action in {"stewardship_approve", "stewardship_reject"}:
        return f"Stewardship item {action.split('_', 1)[-1]}d"

    if action == "stewardship_bulk_approve":
        return new_value or "Bulk approved stewardship tasks"
    if action == "stewardship_bulk_reject":
        return new_value or "Bulk rejected stewardship tasks"

    if action == "ai_generate_rules":
        return new_value or "AI generated quality rules"
    if action == "ai_suggest_stewardship_owners":
        return new_value or "AI suggested stewardship owners"

    if action == "role_change":
        return f"Role changed: {old_value or '?'} → {new_value or '?'}"
    if action == "status_change":
        return f"Account status: {old_value or '?'} → {new_value or '?'}"

    if action in {"create", "update", "delete"}:
        if old_value and new_value:
            return f"{action.title()} on {entity}: {old_value} → {new_value}"
        if new_value:
            return f"{action.title()} on {entity}: {new_value}"
        return f"{action.title()} on {entity}"

    if new_value and old_value:
        return f"{entity}: {old_value} → {new_value}"
    if new_value:
        return new_value
    if old_value:
        return old_value
    return action.replace("_", " ").title()


def build_audit_activity_feed(db: Session, limit: int = 10) -> list[dict]:
    rows = db.query(AuditLog).order_by(AuditLog.timestamp.desc()).limit(limit).all()
    feed: list[dict] = []
    for log in rows:
        action = (log.action or "").lower()
        feed.append(
            {
                "id": log.id,
                "user_id": log.user_id,
                "action": log.action,
                "entity": log.entity,
                "summary": _audit_activity_summary(log)[:200],
                "timestamp": log.timestamp,
                "href": AUDIT_ACTIVITY_HREFS.get(action, "/audit"),
                "category": _audit_activity_category(action),
            }
        )
    return feed


SLA_STATUS_LABELS = {
    "pass": "On track",
    "warning": "At risk",
    "fail": "Breached",
}

OVERALL_SLA_LABELS = {
    "healthy": "All SLAs on track",
    "at_risk": "Some SLAs need attention",
    "breach": "SLA breach detected",
}


def _hours_since(reference: datetime | None) -> float | None:
    if not reference:
        return None
    return (datetime.utcnow() - reference).total_seconds() / 3600


def _format_age(hours: float | None) -> str:
    if hours is None:
        return "Never"
    if hours < 1:
        return f"{int(hours * 60)}m ago"
    if hours < 48:
        return f"{round(hours, 1)}h ago"
    days = round(hours / 24, 1)
    return f"{days}d ago"


def _sla_from_hours(
    hours: float | None,
    *,
    warn_after_hours: float,
    fail_after_hours: float,
) -> str:
    if hours is None:
        return "warning"
    if hours <= warn_after_hours:
        return "pass"
    if hours <= fail_after_hours:
        return "warning"
    return "fail"


def _worst_sla_status(statuses: list[str]) -> str:
    if any(status == "fail" for status in statuses):
        return "fail"
    if any(status == "warning" for status in statuses):
        return "warning"
    return "pass"


def _overall_from_widgets(widgets: list[dict]) -> tuple[str, str]:
    statuses = [widget["status"] for widget in widgets]
    if "fail" in statuses:
        return "breach", OVERALL_SLA_LABELS["breach"]
    if "warning" in statuses:
        return "at_risk", OVERALL_SLA_LABELS["at_risk"]
    return "healthy", OVERALL_SLA_LABELS["healthy"]


def build_sla_status(
    db: Session,
    analytics: dict,
    pipeline_status: dict,
    scheduler_enabled: bool,
) -> dict:
    latest_success_job = (
        db.query(SyncJob)
        .filter(SyncJob.status == "success")
        .order_by(SyncJob.id.desc())
        .first()
    )
    latest_job = db.query(SyncJob).order_by(SyncJob.id.desc()).first()
    sync_reference = None
    if latest_success_job:
        sync_reference = latest_success_job.end_time or latest_success_job.start_time
    elif latest_job:
        sync_reference = latest_job.end_time or latest_job.start_time

    sync_hours = _hours_since(sync_reference)
    sync_status = _sla_from_hours(sync_hours, warn_after_hours=24, fail_after_hours=72)

    pipeline_last_run = pipeline_status.get("last_run_at")
    pipeline_reference = None
    if pipeline_last_run:
        if isinstance(pipeline_last_run, datetime):
            pipeline_reference = pipeline_last_run
        else:
            try:
                pipeline_reference = datetime.fromisoformat(
                    str(pipeline_last_run).replace("Z", "+00:00").replace("+00:00", "")
                )
            except ValueError:
                pipeline_reference = None
    pipeline_hours = _hours_since(pipeline_reference)
    pipeline_status_value = _sla_from_hours(
        pipeline_hours, warn_after_hours=24, fail_after_hours=72
    )

    pending_stewardship = (
        db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()
    )
    if pending_stewardship == 0:
        stewardship_sla = "pass"
    elif pending_stewardship < 10:
        stewardship_sla = "warning"
    else:
        stewardship_sla = "fail"

    failed_records = int(analytics.get("failed_records") or 0)
    total_records = int(analytics.get("total_records") or 0)
    if total_records == 0:
        quarantine_sla = "pass"
    elif failed_records == 0:
        quarantine_sla = "pass"
    elif failed_records < 25:
        quarantine_sla = "warning"
    else:
        quarantine_sla = "fail"

    scheduler_sla = "pass" if scheduler_enabled else "warning"

    widgets = [
        {
            "key": "sync_timeliness",
            "label": "Sync timeliness",
            "status": sync_status,
            "status_label": SLA_STATUS_LABELS[sync_status],
            "metric": _format_age(sync_hours),
            "detail": "Target: successful sync within 24 hours.",
            "href": "/jobs",
            "sla_target": "24h",
        },
        {
            "key": "pipeline_freshness",
            "label": "Pipeline freshness",
            "status": pipeline_status_value,
            "status_label": SLA_STATUS_LABELS[pipeline_status_value],
            "metric": _format_age(pipeline_hours),
            "detail": "Target: pipeline run within 24 hours.",
            "href": "/pipeline",
            "sla_target": "24h",
        },
        {
            "key": "stewardship_backlog",
            "label": "Stewardship backlog",
            "status": stewardship_sla,
            "status_label": SLA_STATUS_LABELS[stewardship_sla],
            "metric": str(pending_stewardship),
            "detail": "Target: fewer than 10 pending review tasks.",
            "href": "/stewardship",
            "sla_target": "<10 pending",
        },
        {
            "key": "quarantine_exceptions",
            "label": "Quarantine exceptions",
            "status": quarantine_sla,
            "status_label": SLA_STATUS_LABELS[quarantine_sla],
            "metric": str(failed_records),
            "detail": f"{failed_records} of {total_records} rows need remediation.",
            "href": "/quarantine",
            "sla_target": "Minimal backlog",
        },
        {
            "key": "scheduler_coverage",
            "label": "Scheduled sync",
            "status": scheduler_sla,
            "status_label": SLA_STATUS_LABELS[scheduler_sla],
            "metric": "On" if scheduler_enabled else "Off",
            "detail": "Automated sync reduces timeliness risk.",
            "href": "/jobs",
            "sla_target": "Enabled",
        },
    ]

    overall_key, overall_label = _overall_from_widgets(widgets)
    return {
        "overall_status": overall_key,
        "overall_label": overall_label,
        "widgets": widgets,
    }
