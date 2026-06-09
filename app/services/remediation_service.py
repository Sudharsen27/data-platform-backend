"""AI stewardship remediation — explain failures and suggest fixes."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
from sqlalchemy.orm import Session

from app.models import CatalogAsset, QuarantineData, Rule, StewardshipQueue, StewardshipRemediation
from app.services.ai_copilot import ai_enabled, ai_provider, explain_quarantine_heuristic
from app.services.copilot_service import groq_api_key, groq_is_configured, groq_model, groq_timeout_seconds
from app.services.data_classification_service import classify_field_name

_SYSTEM_PROMPT = """You are an Enterprise Data Stewardship and Remediation Expert.

Explain record failures in steward-friendly language using only supplied metadata.
Focus on root cause, business impact, governance impact, and actionable remediation.

Respond with valid JSON only, no markdown fences:
{
  "failure_explanation": "...",
  "root_cause": "...",
  "suggested_fix": "...",
  "business_impact": "...",
  "governance_impact": "...",
  "risk_score": 75,
  "suggested_actions": ["...", "..."]
}"""


def _risk_level(score: int) -> str:
    if score >= 70:
        return "High Risk"
    if score >= 40:
        return "Medium Risk"
    return "Low Risk"


def _find_quarantine_match(db: Session, record: StewardshipQueue) -> QuarantineData | None:
    email = (record.email or "").strip().lower()
    name = (record.name or "").strip().lower()
    if email:
        row = (
            db.query(QuarantineData)
            .filter(QuarantineData.email.ilike(email))
            .order_by(QuarantineData.id.desc())
            .first()
        )
        if row:
            return row
    if name:
        return (
            db.query(QuarantineData)
            .filter(QuarantineData.name.ilike(name))
            .order_by(QuarantineData.id.desc())
            .first()
        )
    return None


def _infer_failed_fields(issue: str, email: str, name: str) -> list[str]:
    issue_low = (issue or "").lower()
    fields: list[str] = []
    if "email" in issue_low or ("@" not in (email or "") and email):
        fields.append("customer_email")
    if "email" in issue_low:
        fields.append("email")
    if "duplicate" in issue_low and "id" in issue_low:
        fields.append("customer_id")
    if "duplicate" in issue_low:
        fields.append("customer_id")
    if "phone" in issue_low:
        fields.append("phone_number")
    if "name" in issue_low:
        fields.append("customer_name")
    if "required" in issue_low or "missing" in issue_low:
        if not name:
            fields.append("customer_name")
        if not email:
            fields.append("customer_email")
    if not fields:
        fields.append("record")
    return list(dict.fromkeys(fields))


def _matching_rules(db: Session, issue: str, fields: list[str]) -> list[dict[str, Any]]:
    active = db.query(Rule).filter(Rule.status == "active").all()
    issue_low = (issue or "").lower()
    matched: list[dict[str, Any]] = []
    for rule in active:
        field = (rule.field or "").lower()
        rule_low = (rule.rule or "").lower()
        if field in fields or any(token in issue_low for token in rule_low.split()[:4]):
            matched.append({"field": rule.field, "rule": rule.rule, "id": rule.id})
    return matched[:8]


def _catalog_context(db: Session) -> list[dict[str, Any]]:
    assets = db.query(CatalogAsset).order_by(CatalogAsset.id.asc()).limit(5).all()
    return [
        {
            "asset_key": a.asset_key,
            "name": a.name,
            "pii_tier": a.pii_tier,
            "schema_fields": a.schema_fields,
        }
        for a in assets
    ]


def build_remediation_context(db: Session, record: StewardshipQueue) -> dict[str, Any]:
    quarantine = _find_quarantine_match(db, record)
    error_text = (record.issue or "").strip() or (quarantine.error if quarantine else "")
    fields = _infer_failed_fields(error_text, record.email, record.name)
    classifications = [
        {
            "field": field,
            "classification": classify_field_name(field).get("classification", "Unknown"),
        }
        for field in fields[:5]
        if field != "record"
    ]
    return {
        "stewardship_id": record.id,
        "name": record.name,
        "email": record.email,
        "issue": record.issue,
        "status": record.status,
        "owner_email": record.owner_email,
        "quarantine_error": quarantine.error if quarantine else "",
        "failed_fields": fields,
        "matching_rules": _matching_rules(db, error_text, fields),
        "field_classifications": classifications,
        "catalog_assets": _catalog_context(db),
    }


def _heuristic_remediation(record: StewardshipQueue, context: dict[str, Any]) -> dict[str, Any]:
    issue = (record.issue or context.get("quarantine_error") or "").strip()
    issue_low = issue.lower()
    email = (record.email or "").strip()
    name = (record.name or "").strip()

    base_explanation = explain_quarantine_heuristic(
        name=record.name,
        email=record.email,
        error=issue,
    )

    if "invalid email" in issue_low or (email and "@" not in email):
        return {
            "stewardship_id": record.id,
            "failure_explanation": base_explanation,
            "root_cause": "Invalid email format.",
            "suggested_fix": "Provide a valid email address (e.g. user@company.com).",
            "business_impact": "Customer communications, notifications, and account recovery may fail.",
            "governance_impact": "PII contact data quality is degraded; downstream marketing and CRM sync may reject the record.",
            "risk_score": 78,
            "risk_level": "High Risk",
            "suggested_actions": [
                "Correct the email in the source system",
                "Re-run validation after fix",
                "Approve only after steward verification",
            ],
            "source_engine": "heuristics",
        }

    if "duplicate" in issue_low:
        root = "Duplicate primary identifier." if "id" in issue_low else "Duplicate record detected."
        return {
            "stewardship_id": record.id,
            "failure_explanation": base_explanation,
            "root_cause": root,
            "suggested_fix": "Review master data merge process and consolidate duplicate golden records.",
            "business_impact": "Duplicate customer records cause inconsistent reporting and operational confusion.",
            "governance_impact": "Master data integrity is compromised; lineage and stewardship SLAs may be breached.",
            "risk_score": 82,
            "risk_level": "High Risk",
            "suggested_actions": [
                "Compare with existing master record",
                "Merge or reject duplicate",
                "Document steward decision in annotations",
            ],
            "source_engine": "heuristics",
        }

    if "required" in issue_low or "missing" in issue_low or "null" in issue_low:
        missing = "email" if not email else "name" if not name else "required attribute"
        return {
            "stewardship_id": record.id,
            "failure_explanation": base_explanation,
            "root_cause": f"Missing or null {missing}.",
            "suggested_fix": f"Populate the {missing} from the authoritative source system.",
            "business_impact": "Incomplete records cannot be used reliably in customer or operational processes.",
            "governance_impact": "Completeness rules failed; record blocked from golden master publication.",
            "risk_score": 65,
            "risk_level": "Medium Risk",
            "suggested_actions": [
                "Obtain missing value from source owner",
                "Update source feed",
                "Re-queue for pipeline validation",
            ],
            "source_engine": "heuristics",
        }

    if "phone" in issue_low:
        return {
            "stewardship_id": record.id,
            "failure_explanation": base_explanation,
            "root_cause": "Invalid phone number format or length.",
            "suggested_fix": "Normalize phone to digits-only with valid regional length.",
            "business_impact": "Contact center outreach and verification workflows may fail.",
            "governance_impact": "Contact data quality controls failed.",
            "risk_score": 58,
            "risk_level": "Medium Risk",
            "suggested_actions": ["Validate phone format", "Apply standardization rule", "Re-submit record"],
            "source_engine": "heuristics",
        }

    score = 55 if issue else 35
    return {
        "stewardship_id": record.id,
        "failure_explanation": base_explanation,
        "root_cause": issue or "Quality rule violation detected during pipeline validation.",
        "suggested_fix": "Review the issue against active rules and correct the source value or approve with documented exception.",
        "business_impact": "Record cannot progress to master data until the issue is resolved or waived.",
        "governance_impact": "Stewardship review required to maintain data contract compliance.",
        "risk_score": score,
        "risk_level": _risk_level(score),
        "suggested_actions": [
            "Review matching quality rules",
            "Consult catalog field definitions",
            "Annotate steward decision",
        ],
        "source_engine": "heuristics",
    }


def _call_groq_remediation(context: dict[str, Any]) -> dict[str, Any]:
    api_key = groq_api_key()
    if not api_key:
        raise ValueError("GROQ_API_KEY is not configured")

    with httpx.Client(timeout=groq_timeout_seconds()) as client:
        response = client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": groq_model(),
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            "Analyze this stewardship failure and recommend remediation.\n\n"
                            f"Context:\n{json.dumps(context, ensure_ascii=True, indent=2)}"
                        ),
                    },
                ],
                "temperature": 0.25,
                "max_tokens": 900,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"Groq API error ({response.status_code})")
        raw = (response.json().get("choices") or [{}])[0].get("message", {}).get("content") or ""
        raw = raw.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError("Groq response was not a JSON object")
        return data


def explain_stewardship_failure(db: Session, stewardship_id: int) -> dict[str, Any] | None:
    record = db.query(StewardshipQueue).filter(StewardshipQueue.id == stewardship_id).first()
    if not record:
        return None
    result = generate_remediation(db, record)
    return {
        "stewardship_id": stewardship_id,
        "explanation": result.get("failure_explanation", ""),
        "root_cause": result.get("root_cause", ""),
        "source_engine": result.get("source_engine", "heuristics"),
    }


def generate_remediation(db: Session, record: StewardshipQueue) -> dict[str, Any]:
    context = build_remediation_context(db, record)

    if ai_enabled() and ai_provider() == "groq" and groq_is_configured():
        try:
            data = _call_groq_remediation(context)
            score = int(data.get("risk_score") or 60)
            score = max(0, min(100, score))
            return {
                "stewardship_id": record.id,
                "failure_explanation": (data.get("failure_explanation") or "").strip(),
                "root_cause": (data.get("root_cause") or "").strip(),
                "suggested_fix": (data.get("suggested_fix") or "").strip(),
                "business_impact": (data.get("business_impact") or "").strip(),
                "governance_impact": (data.get("governance_impact") or "").strip(),
                "risk_score": score,
                "risk_level": _risk_level(score),
                "suggested_actions": [str(x) for x in (data.get("suggested_actions") or [])][:6],
                "context": {
                    "failed_fields": context.get("failed_fields"),
                    "matching_rules": context.get("matching_rules"),
                },
                "source_engine": "groq",
            }
        except (httpx.HTTPError, OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    return _heuristic_remediation(record, context)


def persist_remediation(
    db: Session,
    *,
    stewardship_id: int,
    payload: dict[str, Any],
    actor_email: str,
    source_engine: str = "heuristics",
) -> StewardshipRemediation:
    now = __import__("datetime").datetime.utcnow()
    content = json.dumps(payload, ensure_ascii=True)
    existing = (
        db.query(StewardshipRemediation)
        .filter(
            StewardshipRemediation.stewardship_id == stewardship_id,
            StewardshipRemediation.status == "pending",
        )
        .order_by(StewardshipRemediation.updated_at.desc())
        .first()
    )
    if existing:
        existing.content = content
        existing.source_engine = source_engine
        existing.updated_by = actor_email
        existing.updated_at = now
        return existing

    row = StewardshipRemediation(
        stewardship_id=stewardship_id,
        content=content,
        status="pending",
        source_engine=source_engine,
        created_by=actor_email,
        updated_by=actor_email,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def serialize_remediation(row: StewardshipRemediation) -> dict[str, Any]:
    try:
        content = json.loads(row.content or "{}")
        if not isinstance(content, dict):
            content = {}
    except (json.JSONDecodeError, TypeError):
        content = {}
    return {
        "id": row.id,
        "stewardship_id": row.stewardship_id,
        "status": row.status,
        "assigned_owner": row.assigned_owner,
        "source_engine": row.source_engine,
        "created_by": row.created_by,
        "updated_by": row.updated_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        **content,
    }


def get_latest_remediation(db: Session, stewardship_id: int) -> StewardshipRemediation | None:
    return (
        db.query(StewardshipRemediation)
        .filter(StewardshipRemediation.stewardship_id == stewardship_id)
        .order_by(StewardshipRemediation.updated_at.desc())
        .first()
    )
