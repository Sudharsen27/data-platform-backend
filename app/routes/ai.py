import json

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import AICopilotActionLog, QuarantineData, Rule, StewardshipQueue, SyncJob, User
from app.schemas import (
    AICopilotActionResponse,
    AICopilotInsightOut,
    AICopilotInsightsResponse,
)
from app.services.ai_insights import build_ai_insights
from app.services.audit_log import write_audit_log

router = APIRouter(prefix="/ai", tags=["ai"])


def _save_action_log(
    db: Session,
    *,
    action_key: str,
    user_id: str,
    status: str,
    summary: str,
    payload: dict | None = None,
) -> None:
    db.add(
        AICopilotActionLog(
            action_key=action_key,
            user_id=user_id,
            status=status,
            summary=summary,
            payload=json.dumps(payload or {}, ensure_ascii=True),
        )
    )


@router.get("/insights", response_model=AICopilotInsightsResponse)
def get_ai_insights(
    db: Session = Depends(get_db),
    _: User = Depends(get_current_user),
):
    return {"items": [AICopilotInsightOut(**item) for item in build_ai_insights(db)]}


@router.post("/actions/generate-rules", response_model=AICopilotActionResponse)
def generate_rules_from_profile(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:write")),
):
    suggestions = [
        ("email", "Email must contain @ and a valid domain suffix", 0.95),
        ("name", "Name should have at least 2 alphabetic characters", 0.88),
        ("email", "Email should be unique across source systems", 0.91),
    ]
    created_ids: list[str] = []
    details: list[str] = []

    for field_name, rule_text, confidence in suggestions:
        exists = (
            db.query(Rule)
            .filter(
                Rule.field == field_name,
                Rule.rule == rule_text,
            )
            .first()
        )
        if exists:
            details.append(f"{field_name}: already exists (confidence {confidence:.2f})")
            continue
        item = Rule(
            field=field_name,
            rule=rule_text,
            status="active",
            created_by="ai-copilot",
        )
        db.add(item)
        db.flush()
        created_ids.append(str(item.id))
        details.append(
            f"{field_name}: created Rule ID {item.id} (confidence {confidence:.2f})"
        )

    summary = (
        f"Generated {len(created_ids)} new quality rule(s)."
        if created_ids
        else "No new rules created. Suggested rules already exist."
    )
    _save_action_log(
        db,
        action_key="generate_rules",
        user_id=actor.email,
        status="success",
        summary=summary,
        payload={"created_rule_ids": created_ids, "details": details},
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_generate_rules",
        entity="rule",
        old_value="",
        new_value=summary,
    )
    db.commit()
    return {
        "action": "generate_rules",
        "summary": summary,
        "details": details,
    }


@router.post("/actions/suggest-stewardship-owners", response_model=AICopilotActionResponse)
def suggest_stewardship_owners(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    pending_items = (
        db.query(StewardshipQueue)
        .filter(StewardshipQueue.status == "pending")
        .order_by(StewardshipQueue.id.asc())
        .limit(10)
        .all()
    )
    candidate_load = {
        "Data Steward - Customer Domain": 2,
        "Data Steward - Product Domain": 1,
        "Data Governance Admin": 3,
    }
    details: list[str] = []
    for index, item in enumerate(pending_items):
        sorted_candidates = sorted(candidate_load.items(), key=lambda pair: pair[1])
        suggested_owner, current_load = sorted_candidates[0]
        candidate_load[suggested_owner] = current_load + 1
        confidence = max(0.55, 0.92 - (current_load * 0.08))
        details.append(
            f"TASK-{item.id}: {suggested_owner} (confidence {confidence:.2f}, issue: {item.issue or 'general review'})"
        )

    summary = (
        f"Prepared owner suggestions for {len(details)} stewardship task(s)."
        if details
        else "No pending stewardship tasks found."
    )
    _save_action_log(
        db,
        action_key="suggest_stewardship_owners",
        user_id=actor.email,
        status="success",
        summary=summary,
        payload={"suggestions": details},
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_suggest_stewardship_owners",
        entity="stewardship_queue",
        old_value="",
        new_value=summary,
    )
    db.commit()
    return {
        "action": "suggest_stewardship_owners",
        "summary": summary,
        "details": details,
    }


@router.get("/actions/failed-jobs-summary", response_model=AICopilotActionResponse)
def summarize_failed_jobs(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("dashboard:read")),
):
    failed_jobs = (
        db.query(SyncJob)
        .filter(SyncJob.status == "failed")
        .order_by(SyncJob.id.desc())
        .limit(5)
        .all()
    )
    grouped: dict[str, int] = {}
    details: list[str] = []
    for job in failed_jobs:
        raw = (job.error_message or "Unknown").strip()
        category = "unknown"
        low = raw.lower()
        if "timeout" in low:
            category = "timeout"
        elif "snowflake" in low:
            category = "snowflake"
        elif "network" in low:
            category = "network"
        elif "auth" in low or "token" in low:
            category = "auth"
        grouped[category] = grouped.get(category, 0) + 1
        details.append(f"Job #{job.id}: {raw[:180]} (group: {category})")
    summary = (
        f"Found {len(details)} recent failed sync job(s)."
        if details
        else "No failed sync jobs found in recent history."
    )
    _save_action_log(
        db,
        action_key="summarize_failed_jobs",
        user_id=actor.email,
        status="success",
        summary=summary,
        payload={"jobs": details, "groups": grouped},
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_summarize_failed_jobs",
        entity="sync_jobs",
        old_value="",
        new_value=summary,
    )
    db.commit()
    return {
        "action": "summarize_failed_jobs",
        "summary": summary,
        "details": details,
    }
