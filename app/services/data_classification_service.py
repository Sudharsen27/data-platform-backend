"""Rule-based data classification for catalog fields and datasets."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy.orm import Session

from app.models import CatalogAsset, Rule

CLASSIFICATIONS = ("PII", "Sensitive", "Financial", "Confidential", "Public")

_SEVERITY_RANK = {
    "Sensitive": 5,
    "PII": 4,
    "Financial": 3,
    "Confidential": 2,
    "Public": 1,
}

_FIELD_RULES: list[tuple[str, str, int, str]] = [
    # (regex, classification, confidence, reason template)
    (r"(^|_)(email|e_mail|mail)(_|$)", "PII", 96, "Field name indicates email contact information"),
    (r"(^|_)(phone|mobile|msisdn|telephone)(_|$)", "PII", 95, "Field name indicates phone number"),
    (r"(^|_)(customer_name|first_name|last_name|full_name|person_name|name)(_|$)", "PII", 92, "Field name indicates personal name"),
    (r"(^|_)(address|street|postal|zip_code|city)(_|$)", "PII", 88, "Field name indicates location or address data"),
    (r"(^|_)(ssn|social_security|national_id|passport)(_|$)", "Sensitive", 97, "Field name indicates government identity number"),
    (r"(aadhaar|adhaar)", "Sensitive", 98, "Field name indicates Aadhaar identifier"),
    (r"(^|_)(pan|pan_number|tax_id|tin)(_|$)", "Sensitive", 96, "Field name indicates tax or PAN identifier"),
    (r"(^|_)(dob|date_of_birth|birth_date)(_|$)", "Sensitive", 94, "Field name indicates date of birth"),
    (r"(^|_)(salary|compensation|pay_rate|wage)(_|$)", "Confidential", 93, "Field name indicates employee compensation"),
    (r"(^|_)(revenue|income|profit|margin|ebitda|arr|mrr)(_|$)", "Financial", 94, "Field name indicates financial metric"),
    (r"(^|_)(amount|balance|price|cost|payment)(_|$)", "Financial", 88, "Field name indicates monetary value"),
    (r"(^|_)(credit_card|card_number|cvv|iban|account_number)(_|$)", "Sensitive", 97, "Field name indicates payment or account identifier"),
    (r"(customer_id|user_id|client_id)", "PII", 78, "Field name links records to an identifiable customer or user"),
    (r"(^|_)(ip_address|device_id|cookie_id)(_|$)", "PII", 85, "Field name indicates online identifier"),
    (r"(^|_)(created_at|created_date|updated_at|modified_at|timestamp)(_|$)", "Public", 82, "Field name indicates operational timestamp"),
    (r"(^|_)(status|version|type|category|segment)(_|$)", "Public", 75, "Field name indicates non-sensitive reference attribute"),
]

_DATASET_HINTS: list[tuple[str, str, int]] = [
    (r"customer|client|person|contact", "PII", 80),
    (r"payroll|compensation|salary", "Confidential", 85),
    (r"finance|revenue|billing|payment", "Financial", 82),
    (r"public|reference|lookup", "Public", 70),
]

_RECOMMENDATIONS_BY_CLASS: dict[str, list[str]] = {
    "PII": [
        "Masking required in non-production environments",
        "Encrypt at rest",
        "Restrict access to authorized stewards",
        "Define retention policy aligned with privacy regulations",
        "Data owner review required",
    ],
    "Sensitive": [
        "Encryption required",
        "Strict access restrictions",
        "Tokenize or hash where possible",
        "Retention policy review",
        "Data owner review required",
    ],
    "Financial": [
        "Restrict access to finance and analytics roles",
        "Audit all read access",
        "Mask in operational reports where not required",
    ],
    "Confidential": [
        "Limit distribution to need-to-know roles",
        "Encrypt at rest",
        "Retention policy review",
    ],
    "Public": [
        "Document in business glossary",
        "No masking required by default",
    ],
}


def _normalize_field(name: str) -> str:
    return re.sub(r"[\s\-]+", "_", (name or "").strip().lower())


def classify_field_name(
    field_name: str,
    *,
    dataset_name: str = "",
    description: str = "",
    tags: str = "",
) -> dict[str, Any]:
    """Classify a single field using name patterns and optional dataset context."""
    token = _normalize_field(field_name)
    if not token:
        raise ValueError("field_name is required")

    best: dict[str, Any] | None = None
    for pattern, classification, confidence, reason in _FIELD_RULES:
        if re.search(pattern, token):
            candidate = {
                "field_name": field_name.strip(),
                "classification": classification,
                "confidence": confidence,
                "reason": reason,
            }
            if best is None or confidence > best["confidence"]:
                best = candidate

    context_blob = f"{dataset_name} {description} {tags}".lower()
    if best is None:
        for pattern, classification, confidence in _DATASET_HINTS:
            if re.search(pattern, context_blob) and re.search(pattern, token):
                best = {
                    "field_name": field_name.strip(),
                    "classification": classification,
                    "confidence": confidence,
                    "reason": f"Field and dataset context suggest {classification.lower()} data",
                }
                break

    if best is None:
        if token.endswith("_id") or token == "id":
            best = {
                "field_name": field_name.strip(),
                "classification": "Confidential",
                "confidence": 65,
                "reason": "Identifier field — treat as confidential until business glossary confirms sensitivity",
            }
        else:
            best = {
                "field_name": field_name.strip(),
                "classification": "Public",
                "confidence": 60,
                "reason": "No strong PII, sensitive, or financial indicators in field name or metadata",
            }

    best["recommendations"] = list(_RECOMMENDATIONS_BY_CLASS.get(best["classification"], []))
    return best


def _parse_schema_fields(raw: str) -> list[str]:
    return [f.strip() for f in (raw or "").split(",") if f.strip()]


def _aggregate_dataset_classification(field_results: list[dict[str, Any]]) -> str:
    if not field_results:
        return "Public"
    ranked = sorted(
        field_results,
        key=lambda r: _SEVERITY_RANK.get(r["classification"], 0),
        reverse=True,
    )
    return ranked[0]["classification"]


def _compute_risk_score(field_results: list[dict[str, Any]], pii_tier: str = "") -> int:
    counts = {c: 0 for c in CLASSIFICATIONS}
    for row in field_results:
        counts[row["classification"]] = counts.get(row["classification"], 0) + 1

    score = 10
    score += counts.get("PII", 0) * 8
    score += counts.get("Sensitive", 0) * 10
    score += counts.get("Financial", 0) * 6
    score += counts.get("Confidential", 0) * 4
    if pii_tier in ("restricted", "confidential"):
        score += 12
    return min(100, score)


def _governance_recommendations(field_results: list[dict[str, Any]]) -> list[str]:
    recs: list[str] = []
    seen: set[str] = set()
    classes_present = {r["classification"] for r in field_results}

    if "PII" in classes_present or "Sensitive" in classes_present:
        for item in ("Masking required", "Encryption required", "Access restrictions"):
            if item not in seen:
                recs.append(item)
                seen.add(item)
    if "Financial" in classes_present or "Confidential" in classes_present:
        for item in ("Access restrictions", "Data owner review"):
            if item not in seen:
                recs.append(item)
                seen.add(item)
    if field_results:
        if "Retention policy" not in seen:
            recs.append("Retention policy")
            seen.add("Retention policy")

    for row in field_results:
        for rec in row.get("recommendations") or []:
            if rec not in seen:
                recs.append(rec)
                seen.add(rec)
    return recs[:12]


def analyze_dataset(
    db: Session,
    asset_id: int,
) -> dict[str, Any] | None:
    """Bulk classification for all schema fields on a catalog asset."""
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
    if not asset:
        return None

    fields = _parse_schema_fields(asset.schema_fields)
    field_results = [
        classify_field_name(
            field,
            dataset_name=asset.name,
            description=asset.description,
            tags=asset.tags,
        )
        for field in fields
    ]

    pii_fields = [r for r in field_results if r["classification"] == "PII"]
    sensitive_fields = [r for r in field_results if r["classification"] == "Sensitive"]
    financial_fields = [r for r in field_results if r["classification"] == "Financial"]
    confidential_fields = [r for r in field_results if r["classification"] == "Confidential"]
    public_fields = [r for r in field_results if r["classification"] == "Public"]

    related_rules = []
    if fields:
        for rule in db.query(Rule).filter(Rule.status == "active").all():
            for field in fields:
                if field.lower() in (rule.field or "").lower():
                    related_rules.append(
                        {"id": rule.id, "field": rule.field, "rule": rule.rule}
                    )
                    break

    risk_score = _compute_risk_score(field_results, asset.pii_tier)
    dataset_classification = _aggregate_dataset_classification(field_results)
    if asset.pii_tier == "restricted" and _SEVERITY_RANK.get(dataset_classification, 0) < 4:
        dataset_classification = "Sensitive"

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "dataset_classification": dataset_classification,
        "registered_pii_tier": asset.pii_tier,
        "risk_score": risk_score,
        "field_count": len(field_results),
        "pii_count": len(pii_fields),
        "sensitive_count": len(sensitive_fields),
        "financial_count": len(financial_fields),
        "confidential_count": len(confidential_fields),
        "public_count": len(public_fields),
        "pii_fields": pii_fields,
        "sensitive_fields": sensitive_fields,
        "financial_fields": financial_fields,
        "confidential_fields": confidential_fields,
        "public_fields": public_fields,
        "all_fields": field_results,
        "related_rules": related_rules,
        "recommendations": _governance_recommendations(field_results),
        "summary": (
            f"{asset.name}: {len(pii_fields)} PII, {len(sensitive_fields)} sensitive, "
            f"{len(financial_fields)} financial field(s). Risk score {risk_score}/100."
        ),
    }


def find_datasets_with_classification(
    db: Session,
    classification: str,
) -> list[dict[str, Any]]:
    """Find catalog assets containing at least one field of the given classification."""
    target = classification.strip().title()
    if target == "Pii":
        target = "PII"
    matches: list[dict[str, Any]] = []
    for asset in db.query(CatalogAsset).order_by(CatalogAsset.name.asc()).all():
        analysis = analyze_dataset(db, asset.id)
        if not analysis:
            continue
        bucket_key = {
            "PII": "pii_fields",
            "Sensitive": "sensitive_fields",
            "Financial": "financial_fields",
            "Confidential": "confidential_fields",
            "Public": "public_fields",
        }.get(target, "pii_fields")
        if analysis.get(bucket_key):
            matches.append(
                {
                    "dataset_id": asset.id,
                    "dataset_name": asset.name,
                    "dataset_key": asset.asset_key,
                    "field_count": len(analysis.get(bucket_key) or []),
                    "risk_score": analysis["risk_score"],
                }
            )
    return matches
