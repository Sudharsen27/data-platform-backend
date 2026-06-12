"""Business glossary generation — Groq with governance-focused heuristics."""

from __future__ import annotations

import json
import re
from typing import Any

from sqlalchemy.orm import Session

from app.models import CatalogAsset, GlossaryEntry
from app.services.ai_copilot import ai_enabled
from app.services.llm_provider import chat_completion_json, llm_is_available

_SYSTEM_PROMPT = """You are a Data Governance and Business Glossary Expert.

Generate concise, enterprise-ready glossary definitions.
Avoid generic descriptions.
Focus on business meaning, data usage, governance relevance, and data stewardship context.

Respond with valid JSON only, no markdown fences:
{
  "title": "...",
  "definition": "...",
  "usage": "...",
  "governance_notes": "...",
  "examples": ["...", "..."]
}"""


def _humanize_field(field_name: str) -> str:
    return re.sub(r"\s+", " ", field_name.replace("_", " ").strip()).title()


def _infer_field_type(field_name: str) -> str:
    token = field_name.lower()
    if "email" in token:
        return "email"
    if "phone" in token or "mobile" in token:
        return "phone"
    if token.endswith("_id") or token == "id":
        return "identifier"
    if "date" in token or token.endswith("_at"):
        return "timestamp"
    if any(w in token for w in ("amount", "salary", "revenue", "price")):
        return "numeric"
    return "text"


def _heuristic_field_glossary(
    *,
    field_name: str,
    dataset_name: str = "",
    field_type: str = "",
    description: str = "",
    tags: str = "",
    classification: str = "",
) -> dict[str, Any]:
    title = _humanize_field(field_name)
    ftype = field_type or _infer_field_type(field_name)
    ds = dataset_name or "the enterprise data catalog"
    token = field_name.lower()

    if "email" in token:
        return {
            "title": "Customer Email Address" if "customer" in token else title,
            "definition": (
                f"Stores the primary email address associated with the {title.lower()} "
                "attribute used for customer communication, account recovery, and notifications."
            ),
            "usage": f"Used in {ds} for contact validation, outreach, and identity matching across systems.",
            "governance_notes": "Treat as PII. Apply masking in non-production environments and restrict access to authorized stewards.",
            "examples": ["customer@example.com", "billing.contact@company.com"],
        }
    if token.endswith("_id") or token == "customer_id":
        return {
            "title": "Customer Identifier" if "customer" in token else title,
            "definition": (
                f"A unique identifier assigned to a business entity for the {title.lower()} "
                "and used to correlate records across operational, analytical, and reporting systems."
            ),
            "usage": f"Referenced in {ds} as a join key for master data, lineage, and downstream consumption.",
            "governance_notes": "Maintain uniqueness and referential integrity. Document source system precedence in stewardship policy.",
            "examples": ["CUST-0001842", "10029384"],
        }
    if "phone" in token:
        return {
            "title": title,
            "definition": f"Captures the telephone contact number for {title.lower()} used in customer service and verification workflows.",
            "usage": f"Consumed by {ds} for outreach, fraud checks, and contact preference management.",
            "governance_notes": "Classify as PII. Apply format validation and regional privacy controls.",
            "examples": ["+1-555-010-2000"],
        }
    if "name" in token:
        return {
            "title": title,
            "definition": f"Represents the human-readable name associated with {title.lower()} in business and operational contexts.",
            "usage": f"Displayed in {ds} for stewardship review, matching, and customer-facing processes.",
            "governance_notes": "PII field — align with data quality rules for completeness and standardization.",
            "examples": ["Jane Doe", "Acme Corporation"],
        }
    if any(w in token for w in ("created", "updated")) and ("date" in token or token.endswith("_at")):
        return {
            "title": title,
            "definition": f"Operational timestamp indicating when the related {title.lower()} event or record state was captured.",
            "usage": f"Supports auditability, SLA monitoring, and temporal analysis within {ds}.",
            "governance_notes": "Typically non-PII. Ensure timezone consistency and retention alignment.",
            "examples": ["2026-03-15T10:22:00Z"],
        }

    gov_note = f"Classification context: {classification}." if classification else "Review with the data owner before publishing to the enterprise glossary."
    return {
        "title": title,
        "definition": (
            f"Business attribute '{field_name}' registered on {ds}. "
            f"{description.strip() or 'It represents governed metadata used in enterprise data workflows.'}"
        ),
        "usage": f"Used within {ds} for reporting, stewardship, and cross-system data exchange.",
        "governance_notes": gov_note,
        "examples": [],
    }


def _heuristic_dataset_glossary(asset: CatalogAsset) -> dict[str, Any]:
    fields = [f.strip() for f in (asset.schema_fields or "").split(",") if f.strip()]
    field_summary = ", ".join(fields[:8]) if fields else "no registered schema fields"
    return {
        "dataset_title": asset.name,
        "dataset_definition": (
            f"{asset.name} is a governed {asset.asset_type or 'dataset'} in the {asset.domain or 'enterprise'} domain. "
            f"{asset.description or 'It provides curated data for operational and analytical consumption.'}"
        ),
        "business_usage": (
            f"Supports {asset.domain or 'business'} processes, stewardship workflows, and downstream analytics. "
            f"Key attributes include: {field_summary}."
        ),
        "governance_notes": (
            f"PII tier: {asset.pii_tier}. Owner: {asset.owner_email or 'unassigned'}. "
            f"Contract version {asset.contract_version} with SLA {asset.sla_hours}h."
        ),
        "field_glossaries": [],
    }


def _call_llm_glossary(*, prompt: str, context: dict[str, Any]) -> tuple[dict[str, Any], str]:
    user_content = (
        f"{prompt}\n\nContext (JSON):\n{json.dumps(context, ensure_ascii=True, indent=2)}"
    )
    return chat_completion_json(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=user_content,
        temperature=0.25,
        max_tokens=900,
    )


def generate_field_glossary(
    *,
    field_name: str,
    dataset_name: str = "",
    field_type: str = "",
    description: str = "",
    tags: str = "",
    classification: str = "",
    domain: str = "",
    pii_tier: str = "",
) -> dict[str, Any]:
    field_name = (field_name or "").strip()
    if not field_name:
        raise ValueError("field_name is required")

    context = {
        "field_name": field_name,
        "dataset_name": dataset_name,
        "field_type": field_type or _infer_field_type(field_name),
        "description": description,
        "tags": tags,
        "classification": classification,
        "domain": domain,
        "pii_tier": pii_tier,
    }

    if ai_enabled() and llm_is_available():
        try:
            data, engine = _call_llm_glossary(
                prompt=f"Generate a business glossary entry for field '{field_name}'.",
                context=context,
            )
            return {
                "field_name": field_name,
                "title": (data.get("title") or _humanize_field(field_name)).strip(),
                "definition": (data.get("definition") or "").strip(),
                "usage": (data.get("usage") or "").strip(),
                "governance_notes": (data.get("governance_notes") or "").strip(),
                "examples": [str(x) for x in (data.get("examples") or [])][:5],
                "source_engine": engine,
            }
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    base = _heuristic_field_glossary(
        field_name=field_name,
        dataset_name=dataset_name,
        field_type=field_type,
        description=description,
        tags=tags,
        classification=classification,
    )
    return {
        "field_name": field_name,
        **base,
        "source_engine": "heuristics",
    }


def generate_dataset_glossary(db: Session, asset_id: int) -> dict[str, Any] | None:
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
    if not asset:
        return None

    fields = [f.strip() for f in (asset.schema_fields or "").split(",") if f.strip()]
    context = {
        "dataset_id": asset.id,
        "dataset_name": asset.name,
        "asset_key": asset.asset_key,
        "asset_type": asset.asset_type,
        "domain": asset.domain,
        "description": asset.description,
        "tags": asset.tags,
        "pii_tier": asset.pii_tier,
        "schema_fields": fields,
    }

    field_glossaries = [
        generate_field_glossary(
            field_name=field,
            dataset_name=asset.name,
            description=asset.description,
            tags=asset.tags,
            domain=asset.domain,
            pii_tier=asset.pii_tier,
        )
        for field in fields[:20]
    ]

    if ai_enabled() and llm_is_available():
        try:
            data, engine = _call_llm_glossary(
                prompt=f"Generate a dataset-level business glossary entry for '{asset.name}'.",
                context=context,
            )
            return {
                "dataset_id": asset.id,
                "dataset_key": asset.asset_key,
                "dataset_title": (data.get("title") or data.get("dataset_title") or asset.name).strip(),
                "dataset_definition": (data.get("definition") or data.get("dataset_definition") or "").strip(),
                "business_usage": (data.get("usage") or data.get("business_usage") or "").strip(),
                "governance_notes": (data.get("governance_notes") or "").strip(),
                "field_glossaries": field_glossaries,
                "source_engine": engine,
            }
        except (OSError, RuntimeError, ValueError, json.JSONDecodeError):
            pass

    base = _heuristic_dataset_glossary(asset)
    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        **base,
        "field_glossaries": field_glossaries,
        "source_engine": "heuristics",
    }


def get_saved_glossary_entries(
    db: Session,
    *,
    catalog_asset_id: int | None = None,
    field_name: str = "",
    status: str = "",
) -> list[GlossaryEntry]:
    q = db.query(GlossaryEntry).order_by(GlossaryEntry.updated_at.desc())
    if catalog_asset_id is not None:
        q = q.filter(GlossaryEntry.catalog_asset_id == catalog_asset_id)
    if field_name.strip():
        q = q.filter(GlossaryEntry.field_name == field_name.strip().lower())
    if status.strip():
        q = q.filter(GlossaryEntry.status == status.strip().lower())
    return q.limit(100).all()


def serialize_glossary_entry(row: GlossaryEntry) -> dict[str, Any]:
    examples: list[str] = []
    try:
        parsed = json.loads(row.examples or "[]")
        if isinstance(parsed, list):
            examples = [str(x) for x in parsed]
    except (json.JSONDecodeError, TypeError):
        examples = []
    return {
        "id": row.id,
        "catalog_asset_id": row.catalog_asset_id,
        "field_name": row.field_name,
        "title": row.title,
        "definition": row.definition,
        "usage": row.usage,
        "governance_notes": row.governance_notes,
        "examples": examples,
        "status": row.status,
        "source_engine": row.source_engine,
        "created_by": row.created_by,
        "updated_by": row.updated_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }
