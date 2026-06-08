import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import AICopilotActionLog, QuarantineData, Rule, StewardshipQueue, SyncJob, User
from app.schemas import (
    AICopilotActionLogOut,
    AICopilotActionLogsPageOut,
    AICopilotActionResponse,
    AICopilotChatIn,
    AICopilotChatOut,
    AICopilotInsightOut,
    AICopilotInsightsResponse,
    AICopilotSourceOut,
    AIStatusOut,
    AISuggestStewardshipIn,
    ExplainQuarantineIn,
    ExplainQuarantineOut,
)
from app.services.ai_copilot import explain_quarantine, get_ai_status
from app.services.copilot_service import answer_governance_question
from app.services.ai_insights import build_ai_insights
from app.services.ai_rule_suggestions import build_rule_suggestions_from_quarantine
from app.services.ai_stewardship_assignments import assign_stewardship_owners
from app.services.audit_log import write_audit_log

router = APIRouter(prefix="/ai", tags=["ai"])

_KNOWN_ACTION_KEYS = frozenset(
    {
        "generate_rules",
        "suggest_stewardship_owners",
        "explain_quarantine",
        "summarize_failed_jobs",
        "copilot_chat",
        "lineage_impact_analyze",
        "classification_analyze",
    }
)


def _parse_action_payload(raw: str) -> dict:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _serialize_action_log(row: AICopilotActionLog) -> AICopilotActionLogOut:
    return AICopilotActionLogOut(
        id=row.id,
        action_key=row.action_key,
        user_id=row.user_id,
        status=row.status,
        summary=row.summary,
        payload=_parse_action_payload(row.payload),
        created_at=row.created_at,
    )


@router.get("/actions/logs", response_model=AICopilotActionLogsPageOut)
def list_ai_action_logs(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    action_key: str = Query("", description="Filter by action_key"),
    user_id: str = Query("", description="Filter by user email (partial match)"),
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("audit:read")),
):
    q = db.query(AICopilotActionLog)
    key = action_key.strip()
    if key:
        if key not in _KNOWN_ACTION_KEYS:
            raise HTTPException(
                status_code=400,
                detail=f"action_key must be one of: {', '.join(sorted(_KNOWN_ACTION_KEYS))}",
            )
        q = q.filter(AICopilotActionLog.action_key == key)
    user_filter = user_id.strip()
    if user_filter:
        q = q.filter(AICopilotActionLog.user_id.ilike(f"%{user_filter}%"))

    total = q.count()
    rows = (
        q.order_by(AICopilotActionLog.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return {
        "items": [_serialize_action_log(row) for row in rows],
        "total": total,
        "offset": offset,
        "limit": limit,
    }


@router.get("/status", response_model=AIStatusOut)
def read_ai_status(_: User = Depends(get_current_user)):
    return AIStatusOut(**get_ai_status())


@router.post("/copilot/chat", response_model=AICopilotChatOut)
def copilot_chat(
    body: AICopilotChatIn,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("catalog:read")),
):
    question = (body.question or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question is required")

    page_context = None
    if body.page_context is not None:
        page_context = body.page_context.model_dump(exclude_none=True)

    try:
        result = answer_governance_question(
            db,
            question=question,
            page_context=page_context,
        )
        status = "success"
        summary = f"Copilot answered: {question[:120]}"
        audit_new = (result.get("answer") or "")[:500]
    except ValueError as exc:
        _save_action_log(
            db,
            action_key="copilot_chat",
            user_id=actor.email,
            status="failure",
            summary=str(exc)[:200],
            payload={"question": question[:500], "error": str(exc)},
        )
        write_audit_log(
            db,
            user_id=actor.email,
            action="ai_copilot_chat",
            entity="copilot",
            old_value=question[:500],
            new_value=f"failure: {exc}",
        )
        db.commit()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        _save_action_log(
            db,
            action_key="copilot_chat",
            user_id=actor.email,
            status="failure",
            summary="Copilot chat failed",
            payload={"question": question[:500], "error": str(exc)[:300]},
        )
        write_audit_log(
            db,
            user_id=actor.email,
            action="ai_copilot_chat",
            entity="copilot",
            old_value=question[:500],
            new_value=f"failure: {str(exc)[:200]}",
        )
        db.commit()
        raise HTTPException(status_code=502, detail="AI copilot request failed") from exc

    sources = [
        AICopilotSourceOut(**src) for src in (result.get("sources") or [])
    ]
    _save_action_log(
        db,
        action_key="copilot_chat",
        user_id=actor.email,
        status=status,
        summary=summary,
        payload={
            "question": question[:500],
            "page_context": page_context or {},
            "source_engine": result.get("source_engine"),
            "context_summary": result.get("context_summary"),
            "source_count": len(sources),
        },
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_copilot_chat",
        entity="copilot",
        old_value=question[:500],
        new_value=audit_new,
    )
    db.commit()
    return AICopilotChatOut(answer=result["answer"], sources=sources)


@router.post("/actions/explain-quarantine", response_model=ExplainQuarantineOut)
def explain_quarantine_error(
    body: ExplainQuarantineIn,
    db: Session = Depends(get_db),
    actor: User = Depends(get_current_user),
):
    result = explain_quarantine(name=body.name, email=body.email, error=body.error)
    _save_action_log(
        db,
        action_key="explain_quarantine",
        user_id=actor.email,
        status="success",
        summary="Quarantine error explained",
        payload={"source": result["source"], "error_preview": (body.error or "")[:120]},
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_explain_quarantine",
        entity="quarantine",
        old_value=body.error or "",
        new_value=result["explanation"][:500],
    )
    db.commit()
    return ExplainQuarantineOut(**result)


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
    from app.services.dashboard_metrics import get_quarantine_analytics_from_db

    analytics = get_quarantine_analytics_from_db(db)
    return {
        "items": [
            AICopilotInsightOut(**item) for item in build_ai_insights(db, analytics)
        ]
    }


@router.post("/actions/generate-rules", response_model=AICopilotActionResponse)
def generate_rules_from_profile(
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("rules:write")),
):
    suggestions = build_rule_suggestions_from_quarantine(db)
    created_ids: list[str] = []
    details: list[str] = []

    for item in suggestions:
        field_name = item["field"]
        rule_text = item["rule"]
        confidence = float(item["confidence"])
        count = int(item.get("occurrence_count") or 0)
        source = (item.get("source_error") or "profile").strip()
        exists = (
            db.query(Rule)
            .filter(
                Rule.field == field_name,
                Rule.rule == rule_text,
            )
            .first()
        )
        if exists:
            if count > 0:
                details.append(
                    f"{field_name}: already exists (confidence {confidence:.2f}, "
                    f"pattern {count}× \"{source[:60]}\")"
                )
            else:
                details.append(f"{field_name}: already exists (confidence {confidence:.2f})")
            continue
        rule_row = Rule(
            field=field_name,
            rule=rule_text,
            status="active",
            created_by="ai-copilot",
        )
        db.add(rule_row)
        db.flush()
        created_ids.append(str(rule_row.id))
        if count > 0:
            details.append(
                f"{field_name}: created Rule ID {rule_row.id} (confidence {confidence:.2f}, "
                f"from {count}× \"{source[:60]}\")"
            )
        else:
            details.append(
                f"{field_name}: created Rule ID {rule_row.id} (confidence {confidence:.2f})"
            )

    analyzed = sum(1 for s in suggestions if int(s.get("occurrence_count") or 0) > 0)
    if created_ids:
        summary = (
            f"Generated {len(created_ids)} new quality rule(s) from quarantine patterns."
            if analyzed
            else f"Generated {len(created_ids)} new quality rule(s)."
        )
    else:
        summary = "No new rules created. Suggested rules already exist."
    _save_action_log(
        db,
        action_key="generate_rules",
        user_id=actor.email,
        status="success",
        summary=summary,
        payload={
            "created_rule_ids": created_ids,
            "details": details,
            "patterns_analyzed": analyzed,
        },
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
    body: AISuggestStewardshipIn | None = None,
    db: Session = Depends(get_db),
    actor: User = Depends(require_permission("stewardship:manage")),
):
    payload = body or AISuggestStewardshipIn()
    result = assign_stewardship_owners(
        db,
        task_ids=payload.ids if payload.ids else None,
        assign_all_pending=payload.assign_all_pending,
    )
    if result.get("error") == "no_target":
        raise HTTPException(status_code=400, detail=result["summary"])
    _save_action_log(
        db,
        action_key="suggest_stewardship_owners",
        user_id=actor.email,
        status="success",
        summary=result["summary"],
        payload={
            "assignments": result["assignments"],
            "applied_count": result["applied_count"],
            "skipped_count": result["skipped_count"],
            "ids": payload.ids,
            "assign_all_pending": payload.assign_all_pending,
        },
    )
    write_audit_log(
        db,
        user_id=actor.email,
        action="ai_suggest_stewardship_owners",
        entity="stewardship_queue",
        old_value="",
        new_value=result["summary"],
    )
    db.commit()
    return {
        "action": "suggest_stewardship_owners",
        "summary": result["summary"],
        "details": result["details"],
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
        elif (
            "snowflake" in low
            or "sql compilation" in low
            or "42601" in raw
            or "001795" in raw
            or "expressions in a list" in low
        ):
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
