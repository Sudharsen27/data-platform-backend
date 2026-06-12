"""Enterprise dataset documentation generation — Groq with governance metadata context."""

from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy.orm import Session

from app.models import CatalogAsset, DatasetDocumentation, GlossaryEntry, Rule
from app.services.ai_copilot import ai_enabled
from app.services.llm_provider import chat_completion_json, llm_is_available
from app.services.data_classification_service import analyze_dataset, classify_field_name
from app.services.glossary_generator_service import (
    generate_field_glossary,
    get_saved_glossary_entries,
)

_SYSTEM_PROMPT = """You are an Enterprise Data Governance Documentation Expert.

Create concise, professional dataset documentation using only the metadata provided.
Focus on business value, governance, data quality, compliance, and stewardship.

Respond with valid JSON only, no markdown fences:
{
  "title": "...",
  "summary": "...",
  "business_description": "...",
  "purpose": "...",
  "key_fields": [{"field_name": "...", "description": "...", "classification": "..."}],
  "owner_recommendation": "...",
  "governance_notes": "...",
  "classification_summary": "...",
  "quality_expectations": "...",
  "usage_guidelines": "...",
  "compliance_considerations": "..."
}"""


def _parse_fields(schema_fields: str) -> list[str]:
    return [f.strip() for f in (schema_fields or "").split(",") if f.strip()]


def _related_rules(db: Session, asset: CatalogAsset, fields: list[str]) -> list[dict[str, Any]]:
    rules = db.query(Rule).filter(Rule.status == "active").order_by(Rule.id.desc()).limit(50).all()
    matched: list[dict[str, Any]] = []
    field_set = {f.lower() for f in fields}
    for rule in rules:
        if rule.field.lower() in field_set or any(f in (rule.rule or "").lower() for f in field_set):
            matched.append(
                {
                    "id": rule.id,
                    "field": rule.field,
                    "rule": rule.rule,
                    "status": rule.status,
                }
            )
    return matched[:10]


def _build_key_fields(
    db: Session,
    asset: CatalogAsset,
    fields: list[str],
    classification: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    class_map = {
        f["field_name"]: f for f in (classification or {}).get("all_fields") or []
    }
    saved_glossary = {
        row.field_name: row
        for row in get_saved_glossary_entries(db, catalog_asset_id=asset.id)
        if row.field_name
    }
    key_fields: list[dict[str, Any]] = []
    for field in fields[:20]:
        cls = class_map.get(field, {})
        saved = saved_glossary.get(field.lower())
        if saved:
            description = saved.definition or saved.title
        else:
            description = generate_field_glossary(
                field_name=field,
                dataset_name=asset.name,
                description=asset.description,
                tags=asset.tags,
                domain=asset.domain,
                pii_tier=asset.pii_tier,
            ).get("definition", "")
        key_fields.append(
            {
                "field_name": field,
                "description": description[:400],
                "classification": cls.get("classification", "Unknown"),
            }
        )
    return key_fields


def _lineage_summary(db: Session, asset: CatalogAsset) -> dict[str, Any]:
    if not (asset.lineage_node_key or "").strip():
        return {}
    try:
        from app.services.lineage_impact_service import analyze_asset_impact_by_id

        impact = analyze_asset_impact_by_id(db, asset.id)
        if not impact:
            return {}
        return {
            "impact_score": impact.get("impact_score", 0),
            "downstream_count": impact.get("downstream_count", 0),
            "upstream_count": impact.get("upstream_count", 0),
            "summary": impact.get("summary", ""),
            "critical_dependencies": [
                d.get("name") for d in (impact.get("critical_dependencies") or [])[:5]
            ],
        }
    except Exception:
        return {}


def build_documentation_context(db: Session, asset: CatalogAsset) -> dict[str, Any]:
    fields = _parse_fields(asset.schema_fields)
    classification = analyze_dataset(db, asset.id)
    rules = _related_rules(db, asset, fields)
    lineage = _lineage_summary(db, asset)
    glossary_entries = get_saved_glossary_entries(db, catalog_asset_id=asset.id, status="approved")
    key_fields = _build_key_fields(db, asset, fields, classification)

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "asset_type": asset.asset_type,
        "domain": asset.domain,
        "owner_email": asset.owner_email,
        "description": asset.description,
        "tags": asset.tags,
        "pii_tier": asset.pii_tier,
        "schema_fields": fields,
        "sla_hours": asset.sla_hours,
        "contract_version": asset.contract_version,
        "lineage_node_key": asset.lineage_node_key,
        "classification": classification,
        "key_fields": key_fields,
        "quality_rules": rules,
        "lineage": lineage,
        "glossary_count": len(glossary_entries),
        "approved_glossary_terms": [
            {"field": g.field_name, "title": g.title, "definition": g.definition[:200]}
            for g in glossary_entries[:10]
        ],
    }


def _heuristic_documentation(asset: CatalogAsset, context: dict[str, Any]) -> dict[str, Any]:
    classification = context.get("classification") or {}
    key_fields = context.get("key_fields") or []
    lineage = context.get("lineage") or {}
    rules = context.get("quality_rules") or []

    pii_count = classification.get("pii_count", 0)
    risk_score = classification.get("risk_score", 0)
    dataset_class = classification.get("dataset_classification", "Unknown")
    owner = asset.owner_email or "Assign a data owner from the business domain"

    field_names = ", ".join(f["field_name"] for f in key_fields[:6]) or "No schema fields registered"
    lineage_note = ""
    if lineage.get("summary"):
        lineage_note = (
            f" Lineage impact score {lineage.get('impact_score', 0)} with "
            f"{lineage.get('downstream_count', 0)} downstream dependencies."
        )

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "title": asset.name,
        "summary": (
            f"{asset.name} is a governed {asset.asset_type or 'dataset'} in the {asset.domain or 'enterprise'} domain. "
            f"It contains {len(key_fields)} registered attributes and is classified as {dataset_class} "
            f"with a governance risk score of {risk_score}/100.{lineage_note}"
        ),
        "business_description": (
            asset.description
            or f"{asset.name} provides curated {asset.domain or 'business'} data for operational and analytical consumption."
        ),
        "purpose": (
            f"Supports {asset.domain or 'business'} processes, reporting, and stewardship workflows. "
            f"Primary attributes include: {field_names}."
        ),
        "key_fields": key_fields,
        "owner_recommendation": (
            f"Recommended data owner: {owner}. "
            "Ensure stewardship coverage for classification reviews, glossary maintenance, and SLA adherence."
        ),
        "governance_notes": (
            f"PII tier: {asset.pii_tier}. Contract version {asset.contract_version} with SLA {asset.sla_hours}h. "
            f"Contains {pii_count} PII-classified field(s). "
            f"Tags: {asset.tags or 'none'}."
        ),
        "classification_summary": (
            f"Dataset classification: {dataset_class}. Risk score {risk_score}/100. "
            f"PII fields: {classification.get('pii_count', 0)}, "
            f"Sensitive: {classification.get('sensitive_count', 0)}, "
            f"Financial: {classification.get('financial_count', 0)}."
        ),
        "quality_expectations": (
            f"Active quality rules: {len(rules)}. "
            + (
                "; ".join(f"{r['field']}: {r['rule'][:60]}" for r in rules[:3])
                if rules
                else "Register validation rules for critical attributes."
            )
        ),
        "usage_guidelines": (
            "Consume through approved analytics and operational channels. "
            "Apply masking for PII in non-production. "
            "Reference the business glossary for field definitions before downstream publication."
        ),
        "compliance_considerations": (
            "Align access with role-based policies. "
            + (
                "PII and sensitive fields require encryption, masking, and retention review."
                if pii_count > 0
                else "Document public fields in the enterprise glossary and monitor schema changes."
            )
        ),
        "source_engine": "heuristics",
    }


def _call_llm_documentation(context: dict[str, Any]) -> tuple[dict[str, Any], str]:
    slim_context = {
        k: context.get(k)
        for k in (
            "dataset_name",
            "dataset_key",
            "asset_type",
            "domain",
            "owner_email",
            "description",
            "tags",
            "pii_tier",
            "schema_fields",
            "key_fields",
            "classification",
            "quality_rules",
            "lineage",
            "approved_glossary_terms",
        )
    }
    return chat_completion_json(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=(
            "Generate enterprise dataset documentation.\n\n"
            f"Context (JSON):\n{json.dumps(slim_context, ensure_ascii=True, indent=2)}"
        ),
        temperature=0.25,
        max_tokens=1400,
    )


def generate_dataset_documentation(db: Session, dataset_id: int) -> dict[str, Any] | None:
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == dataset_id).first()
    if not asset:
        return None

    context = build_documentation_context(db, asset)

    if ai_enabled() and llm_is_available():
        try:
            data, engine = _call_llm_documentation(context)
            key_fields = data.get("key_fields")
            if not isinstance(key_fields, list) or not key_fields:
                key_fields = context.get("key_fields") or []
            return {
                "dataset_id": asset.id,
                "dataset_key": asset.asset_key,
                "title": (data.get("title") or asset.name).strip(),
                "summary": (data.get("summary") or "").strip(),
                "business_description": (data.get("business_description") or "").strip(),
                "purpose": (data.get("purpose") or "").strip(),
                "key_fields": key_fields,
                "owner_recommendation": (data.get("owner_recommendation") or "").strip(),
                "governance_notes": (data.get("governance_notes") or "").strip(),
                "classification_summary": (data.get("classification_summary") or "").strip(),
                "quality_expectations": (data.get("quality_expectations") or "").strip(),
                "usage_guidelines": (data.get("usage_guidelines") or "").strip(),
                "compliance_considerations": (data.get("compliance_considerations") or "").strip(),
                "source_engine": engine,
            }
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    return _heuristic_documentation(asset, context)


def get_saved_documentation(db: Session, catalog_asset_id: int) -> DatasetDocumentation | None:
    return (
        db.query(DatasetDocumentation)
        .filter(DatasetDocumentation.catalog_asset_id == catalog_asset_id)
        .order_by(DatasetDocumentation.updated_at.desc())
        .first()
    )


def serialize_documentation_row(row: DatasetDocumentation) -> dict[str, Any]:
    try:
        content = json.loads(row.content or "{}")
        if not isinstance(content, dict):
            content = {}
    except (json.JSONDecodeError, TypeError):
        content = {}
    return {
        **content,
        "id": row.id,
        "catalog_asset_id": row.catalog_asset_id,
        "dataset_id": row.catalog_asset_id,
        "title": row.title,
        "status": row.status,
        "source_engine": row.source_engine or content.get("source_engine") or "heuristics",
        "created_by": row.created_by,
        "updated_by": row.updated_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def documentation_to_markdown(doc: dict[str, Any]) -> str:
    lines = [
        f"# {doc.get('title', 'Dataset Documentation')}",
        "",
        f"**Dataset key:** {doc.get('dataset_key', '')}",
        "",
        "## Summary",
        doc.get("summary", ""),
        "",
        "## Business Description",
        doc.get("business_description", ""),
        "",
        "## Business Purpose",
        doc.get("purpose", ""),
        "",
        "## Data Owner Recommendation",
        doc.get("owner_recommendation", ""),
        "",
        "## Key Fields",
    ]
    for field in doc.get("key_fields") or []:
        lines.append(
            f"- **{field.get('field_name', '')}** ({field.get('classification', 'Unknown')}): "
            f"{field.get('description', '')}"
        )
    lines.extend(
        [
            "",
            "## Classification Summary",
            doc.get("classification_summary", ""),
            "",
            "## Governance Notes",
            doc.get("governance_notes", ""),
            "",
            "## Data Quality Expectations",
            doc.get("quality_expectations", ""),
            "",
            "## Usage Guidelines",
            doc.get("usage_guidelines", ""),
            "",
            "## Compliance Considerations",
            doc.get("compliance_considerations", ""),
            "",
        ]
    )
    return "\n".join(lines)


def documentation_to_text(doc: dict[str, Any]) -> str:
    return documentation_to_markdown(doc).replace("**", "").replace("# ", "").replace("## ", "")
