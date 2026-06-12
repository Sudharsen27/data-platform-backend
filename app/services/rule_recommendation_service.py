"""AI-powered data quality and governance rule recommendations."""

from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy.orm import Session

from app.models import CatalogAsset, Rule, RuleRecommendation
from app.services.ai_copilot import ai_enabled
from app.services.llm_provider import chat_completion_json, llm_is_available
from app.services.data_classification_service import analyze_dataset, classify_field_name
from app.services.glossary_generator_service import get_saved_glossary_entries

_SYSTEM_PROMPT = """You are an Enterprise Data Governance and Data Quality Expert.

Using only the supplied metadata, recommend data quality and governance rules.
For each rule provide business justification, governance importance, and compliance impact.

Respond with valid JSON only, no markdown fences:
{
  "recommended_rules": [
    {
      "field_name": "...",
      "rule_type": "uniqueness|completeness|format|range|referential_integrity|governance|compliance",
      "rule_text": "...",
      "confidence": 85,
      "business_reason": "...",
      "governance_importance": "...",
      "compliance_impact": "..."
    }
  ]
}"""

_RULE_TYPES = (
    "uniqueness",
    "completeness",
    "format",
    "range",
    "referential_integrity",
    "governance",
    "compliance",
)


def _parse_fields(schema_fields: str) -> list[str]:
    return [f.strip() for f in (schema_fields or "").split(",") if f.strip()]


def _existing_rules_for_field(db: Session, field_name: str) -> list[Rule]:
    token = field_name.lower()
    return (
        db.query(Rule)
        .filter(Rule.field.ilike(token))
        .order_by(Rule.id.desc())
        .all()
    )


def _rule_already_covered(existing: list[Rule], rule_text: str) -> bool:
    needle = rule_text.lower()
    for row in existing:
        existing_text = (row.rule or "").lower()
        if needle in existing_text or existing_text in needle:
            return True
        if "unique" in needle and "unique" in existing_text:
            return True
        if "null" in needle and ("null" in existing_text or "required" in existing_text):
            return True
        if "email" in needle and "@" in existing_text:
            return True
    return False


def _heuristic_rules_for_field(
    field_name: str,
    *,
    dataset_name: str = "",
    classification: str = "",
    pii_tier: str = "",
    existing: list[Rule] | None = None,
) -> list[dict[str, Any]]:
    token = field_name.lower()
    existing = existing or []
    rules: list[dict[str, Any]] = []

    def add(
        rule_type: str,
        rule_text: str,
        confidence: int,
        business_reason: str,
        governance_importance: str = "",
        compliance_impact: str = "",
    ) -> None:
        if _rule_already_covered(existing, rule_text):
            return
        rules.append(
            {
                "field_name": field_name,
                "rule_type": rule_type,
                "rule_text": rule_text,
                "confidence": confidence,
                "business_reason": business_reason,
                "governance_importance": governance_importance
                or f"Supports {rule_type} controls for {field_name} on {dataset_name or 'the dataset'}.",
                "compliance_impact": compliance_impact,
            }
        )

    if token.endswith("_id") or token == "id" or token == "customer_id":
        add(
            "uniqueness",
            "Must be unique",
            98,
            "Identifier fields must be unique to prevent duplicate master records and broken joins.",
            "Critical for golden record integrity and downstream analytics.",
            "Duplicate identifiers violate data integrity policies.",
        )
        add(
            "completeness",
            "Cannot be null",
            96,
            "Primary identifiers cannot be empty or records cannot be matched across systems.",
        )

    if "email" in token:
        add(
            "format",
            "Email must contain @ and a valid domain suffix",
            95,
            "Valid email format is required for customer communication and identity verification.",
            "Prevents invalid contact data entering operational systems.",
            "Invalid contact data may breach marketing and privacy consent policies.",
        )
        add(
            "completeness",
            "Email cannot be null",
            92,
            "Email is a primary contact attribute used across customer journeys.",
        )
        if classification == "PII":
            add(
                "compliance",
                "Mask email in non-production environments",
                88,
                "PII email must be protected in lower environments.",
                compliance_impact="Supports privacy and data protection requirements.",
            )

    if any(w in token for w in ("phone", "mobile", "msisdn")):
        add(
            "format",
            "Must contain only digits",
            90,
            "Phone numbers should be normalized to digits for validation and matching.",
        )
        add(
            "format",
            "Length must be 10",
            88,
            "Standardized phone length improves validation and regional compliance checks.",
        )

    if any(w in token for w in ("date_of_birth", "dob", "birth_date")):
        add(
            "range",
            "Cannot be future date",
            94,
            "Birth dates cannot occur in the future.",
            compliance_impact="Incorrect birth dates affect age-based compliance and eligibility rules.",
        )
        add(
            "completeness",
            "Cannot be null",
            92,
            "Date of birth is required for identity verification and regulatory reporting.",
        )

    if any(w in token for w in ("created", "updated")) and ("date" in token or token.endswith("_at")):
        add(
            "range",
            "Cannot be future date",
            85,
            "Operational timestamps should reflect actual or system capture time.",
        )

    if any(w in token for w in ("amount", "salary", "revenue", "price", "balance")):
        add(
            "range",
            "Amount must be greater than or equal to zero",
            87,
            "Financial attributes should not contain negative values unless explicitly allowed.",
            governance_importance="Protects financial reporting accuracy.",
            compliance_impact="Incorrect financial values affect regulatory and management reporting.",
        )
        add(
            "completeness",
            "Cannot be null",
            84,
            "Financial metrics must be populated for reporting and controls.",
        )

    if classification in ("PII", "Sensitive"):
        add(
            "governance",
            "Restrict access to authorized stewards",
            86,
            f"{field_name} is classified as {classification} and requires controlled access.",
            governance_importance="Enforces least-privilege data access.",
            compliance_impact="Reduces exposure of regulated personal or sensitive data.",
        )

    if classification == "Financial":
        add(
            "compliance",
            "Audit all read access",
            85,
            "Financial fields require traceable access for SOX-style controls.",
        )

    if not rules:
        add(
            "completeness",
            "Cannot be null",
            75,
            f"{field_name} should be populated to support reliable reporting on {dataset_name or 'this dataset'}.",
        )

    return rules


def _compute_risk_scores(
    fields: list[str],
    recommendations: list[dict[str, Any]],
    classification: dict[str, Any] | None,
    existing_rule_count: int,
) -> dict[str, Any]:
    cls = classification or {}
    pii_count = cls.get("pii_count", 0)
    sensitive_count = cls.get("sensitive_count", 0)
    risk_score = cls.get("risk_score", 0)
    rec_count = len(recommendations)
    gap = max(0, len(fields) * 2 - existing_rule_count)

    quality_score = min(100, 20 + gap * 8 + max(0, 30 - rec_count * 2))
    governance_score = min(100, risk_score + sensitive_count * 5 + max(0, 40 - existing_rule_count * 3))
    compliance_score = min(100, pii_count * 12 + sensitive_count * 10 + max(0, 35 - existing_rule_count * 2))

    def band(score: int) -> str:
        if score >= 70:
            return "High Risk"
        if score >= 40:
            return "Medium Risk"
        return "Low Risk"

    return {
        "data_quality_risk_score": quality_score,
        "data_quality_risk_level": band(quality_score),
        "governance_risk_score": governance_score,
        "governance_risk_level": band(governance_score),
        "compliance_risk_score": compliance_score,
        "compliance_risk_level": band(compliance_score),
        "missing_rule_gaps": gap,
    }


def _build_context(db: Session, asset: CatalogAsset) -> dict[str, Any]:
    fields = _parse_fields(asset.schema_fields)
    classification = analyze_dataset(db, asset.id)
    glossary = get_saved_glossary_entries(db, catalog_asset_id=asset.id, status="approved")
    existing_rules = db.query(Rule).filter(Rule.status == "active").all()
    field_rules = {f: _existing_rules_for_field(db, f) for f in fields}

    lineage_summary = {}
    if asset.lineage_node_key:
        try:
            from app.services.lineage_impact_service import analyze_asset_impact_by_id

            impact = analyze_asset_impact_by_id(db, asset.id)
            if impact:
                lineage_summary = {
                    "impact_score": impact.get("impact_score", 0),
                    "downstream_count": impact.get("downstream_count", 0),
                }
        except Exception:
            pass

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "domain": asset.domain,
        "pii_tier": asset.pii_tier,
        "schema_fields": fields,
        "classification": classification,
        "existing_rules": [
            {"field": r.field, "rule": r.rule, "status": r.status} for r in existing_rules[:30]
        ],
        "field_existing_rules": {
            f: [{"rule": r.rule} for r in field_rules.get(f, [])] for f in fields
        },
        "glossary_terms": [
            {"field": g.field_name, "title": g.title, "definition": g.definition[:160]}
            for g in glossary[:15]
        ],
        "lineage": lineage_summary,
    }


def _call_llm_recommendations(
    context: dict[str, Any], *, field_name: str = ""
) -> tuple[list[dict[str, Any]], str]:
    prompt = (
        f"Recommend data quality rules for field '{field_name}'."
        if field_name
        else f"Recommend data quality rules for dataset '{context.get('dataset_name', '')}'."
    )
    data, engine = chat_completion_json(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=f"{prompt}\n\nContext:\n{json.dumps(context, ensure_ascii=True, indent=2)}",
        temperature=0.2,
        max_tokens=1200,
    )
    items = data.get("recommended_rules") or []
    if not isinstance(items, list):
        raise RuntimeError("Invalid LLM rules payload")
    return items, engine


def _normalize_recommendation(item: dict[str, Any], default_field: str = "") -> dict[str, Any]:
    rule_type = (item.get("rule_type") or "governance").strip().lower()
    if rule_type not in _RULE_TYPES:
        rule_type = "governance"
    confidence = item.get("confidence", 80)
    try:
        confidence = int(confidence)
    except (TypeError, ValueError):
        confidence = 80
    confidence = max(0, min(100, confidence))
    return {
        "field_name": (item.get("field_name") or default_field).strip(),
        "rule_type": rule_type,
        "rule_text": (item.get("rule_text") or "").strip(),
        "confidence": confidence,
        "business_reason": (item.get("business_reason") or "").strip(),
        "governance_importance": (item.get("governance_importance") or "").strip(),
        "compliance_impact": (item.get("compliance_impact") or "").strip(),
    }


def recommend_rules_for_field(
    db: Session,
    *,
    field_name: str,
    dataset_id: int | None = None,
    dataset_name: str = "",
    pii_tier: str = "",
) -> dict[str, Any]:
    field_name = (field_name or "").strip()
    if not field_name:
        raise ValueError("field_name is required")

    asset = None
    if dataset_id is not None:
        asset = db.query(CatalogAsset).filter(CatalogAsset.id == dataset_id).first()
        if asset:
            dataset_name = asset.name
            pii_tier = asset.pii_tier

    classification = classify_field_name(
        field_name,
        dataset_name=dataset_name,
        tags=asset.tags if asset else "",
        description=asset.description if asset else "",
    ).get("classification", "")

    existing = _existing_rules_for_field(db, field_name)
    heuristic = _heuristic_rules_for_field(
        field_name,
        dataset_name=dataset_name,
        classification=classification,
        pii_tier=pii_tier,
        existing=existing,
    )

    rules = heuristic
    source_engine = "heuristics"

    if asset and ai_enabled() and llm_is_available():
        try:
            context = _build_context(db, asset)
            llm_items, engine = _call_llm_recommendations(context, field_name=field_name)
            merged: list[dict[str, Any]] = []
            seen: set[tuple[str, str]] = set()
            for item in llm_items + heuristic:
                norm = _normalize_recommendation(item, default_field=field_name)
                if not norm["rule_text"]:
                    continue
                key = (norm["field_name"].lower(), norm["rule_text"].lower())
                if key in seen:
                    continue
                if _rule_already_covered(existing, norm["rule_text"]):
                    continue
                seen.add(key)
                merged.append(norm)
            if merged:
                rules = merged
                source_engine = engine
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    risk = _compute_risk_scores(
        [field_name],
        rules,
        {"pii_count": 1 if classification == "PII" else 0, "sensitive_count": 1 if classification == "Sensitive" else 0, "risk_score": 50},
        len(existing),
    )

    return {
        "dataset_id": dataset_id,
        "field_name": field_name,
        "classification": classification,
        "rules": [{**r, "source_engine": source_engine} for r in rules],
        "risk_analysis": risk,
        "source_engine": source_engine,
    }


def recommend_rules_for_dataset(db: Session, dataset_id: int) -> dict[str, Any] | None:
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == dataset_id).first()
    if not asset:
        return None

    context = _build_context(db, asset)
    fields = context["schema_fields"]
    classification = context.get("classification") or {}
    all_rules: list[dict[str, Any]] = []
    source_engine = "heuristics"

    for field in fields:
        field_result = recommend_rules_for_field(
            db,
            field_name=field,
            dataset_id=dataset_id,
            dataset_name=asset.name,
            pii_tier=asset.pii_tier,
        )
        for rule in field_result.get("rules") or []:
            all_rules.append(rule)
        if field_result.get("source_engine") in ("groq", "azure_openai"):
            source_engine = field_result["source_engine"]

    if ai_enabled() and llm_is_available() and not fields:
        try:
            llm_items, engine = _call_llm_recommendations(context)
            for item in llm_items:
                norm = _normalize_recommendation(item)
                if norm["rule_text"]:
                    all_rules.append({**norm, "source_engine": engine})
            source_engine = engine
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    existing_count = len(context.get("existing_rules") or [])
    risk = _compute_risk_scores(fields, all_rules, classification, existing_count)

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "recommended_rules": all_rules,
        "risk_analysis": risk,
        "source_engine": source_engine,
    }


def serialize_recommendation(row: RuleRecommendation) -> dict[str, Any]:
    return {
        "id": row.id,
        "catalog_asset_id": row.catalog_asset_id,
        "field_name": row.field_name,
        "rule_type": row.rule_type,
        "rule_text": row.rule_text,
        "confidence": row.confidence,
        "business_reason": row.business_reason,
        "governance_importance": row.governance_importance,
        "compliance_impact": row.compliance_impact,
        "status": row.status,
        "source_engine": row.source_engine,
        "approved_rule_id": row.approved_rule_id,
        "created_by": row.created_by,
        "updated_by": row.updated_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def persist_recommendations(
    db: Session,
    *,
    catalog_asset_id: int,
    recommendations: list[dict[str, Any]],
    actor_email: str,
    source_engine: str = "heuristics",
) -> list[RuleRecommendation]:
    saved: list[RuleRecommendation] = []
    now = __import__("datetime").datetime.utcnow()
    for item in recommendations:
        field_name = (item.get("field_name") or "").strip().lower()
        rule_text = (item.get("rule_text") or "").strip()
        if not rule_text:
            continue
        existing = (
            db.query(RuleRecommendation)
            .filter(
                RuleRecommendation.catalog_asset_id == catalog_asset_id,
                RuleRecommendation.field_name == field_name,
                RuleRecommendation.rule_text == rule_text,
                RuleRecommendation.status == "pending",
            )
            .first()
        )
        if existing:
            saved.append(existing)
            continue
        row = RuleRecommendation(
            catalog_asset_id=catalog_asset_id,
            field_name=field_name,
            rule_type=(item.get("rule_type") or "governance").strip().lower(),
            rule_text=rule_text,
            confidence=int(item.get("confidence") or 80),
            business_reason=(item.get("business_reason") or "").strip(),
            governance_importance=(item.get("governance_importance") or "").strip(),
            compliance_impact=(item.get("compliance_impact") or "").strip(),
            status="pending",
            source_engine=item.get("source_engine") or source_engine,
            created_by=actor_email,
            updated_by=actor_email,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
        saved.append(row)
    db.flush()
    return saved
