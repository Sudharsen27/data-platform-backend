"""Governance health scoring for datasets, domains, and the platform."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import (
    AuditLog,
    CatalogAsset,
    DatasetDocumentation,
    GlossaryEntry,
    QuarantineData,
    Rule,
    StewardshipQueue,
)
from app.services.dashboard_metrics import GOVERNANCE_AUDIT_ACTIONS, SENSITIVE_PII_TIERS
from app.services.data_classification_service import analyze_dataset

GOVERNANCE_TARGET_SCORE = 70

DIMENSION_KEYS = (
    "metadata_coverage",
    "glossary_coverage",
    "documentation_coverage",
    "classification_coverage",
    "rule_coverage",
    "lineage_coverage",
    "stewardship_resolution_rate",
    "audit_compliance",
)

DIMENSION_LABELS = {
    "metadata_coverage": "Metadata Coverage",
    "glossary_coverage": "Glossary Coverage",
    "documentation_coverage": "Documentation Coverage",
    "classification_coverage": "Classification Coverage",
    "rule_coverage": "Rule Coverage",
    "lineage_coverage": "Lineage Coverage",
    "stewardship_resolution_rate": "Stewardship Resolution",
    "audit_compliance": "Audit Compliance",
}

DIMENSION_WEIGHTS = {key: round(1 / len(DIMENSION_KEYS), 4) for key in DIMENSION_KEYS}

_RECOMMENDATIONS: dict[str, str] = {
    "metadata_coverage": "Assign data owners and complete catalog metadata (description, schema fields, domain).",
    "glossary_coverage": "Generate and approve business glossary terms for key fields.",
    "documentation_coverage": "Create and approve dataset documentation with purpose and governance notes.",
    "classification_coverage": "Run classification analysis and align PII tier with field sensitivity.",
    "rule_coverage": "Define active data quality rules for critical fields and reduce quarantine failures.",
    "lineage_coverage": "Link catalog assets to lineage nodes and document downstream dependencies.",
    "stewardship_resolution_rate": "Resolve pending stewardship tasks and assign owners.",
    "audit_compliance": "Increase governance audit activity (approvals, rule changes, pipeline runs).",
}


def _parse_fields(schema_fields: str) -> list[str]:
    return [f.strip() for f in (schema_fields or "").split(",") if f.strip()]


def _score_metadata(asset: CatalogAsset) -> tuple[int, list[str]]:
    gaps: list[str] = []
    points = 0
    checks = [
        (bool((asset.owner_email or "").strip()), "owner_email", 25),
        (bool((asset.description or "").strip()), "description", 25),
        (bool(_parse_fields(asset.schema_fields)), "schema_fields", 25),
        (
            bool((asset.domain or "").strip()) and bool((asset.pii_tier or "").strip()),
            "domain_and_pii_tier",
            25,
        ),
    ]
    for ok, label, weight in checks:
        if ok:
            points += weight
        else:
            gaps.append(label)
    return points, gaps


def _score_glossary(db: Session, asset_id: int, field_count: int) -> tuple[int, list[str]]:
    if field_count == 0:
        return 40, ["schema_fields"]
    approved = (
        db.query(GlossaryEntry)
        .filter(
            GlossaryEntry.catalog_asset_id == asset_id,
            func.lower(GlossaryEntry.status) == "approved",
        )
        .count()
    )
    coverage = min(100, round((approved / field_count) * 100))
    gaps = [] if coverage >= 80 else ["glossary_terms"]
    return coverage, gaps


def _score_documentation(db: Session, asset_id: int) -> tuple[int, list[str]]:
    row = (
        db.query(DatasetDocumentation)
        .filter(DatasetDocumentation.catalog_asset_id == asset_id)
        .order_by(DatasetDocumentation.updated_at.desc())
        .first()
    )
    if not row:
        return 0, ["documentation"]
    status = (row.status or "").lower()
    if status == "approved":
        return 100, []
    if status in ("draft", "pending"):
        return 55, ["approved_documentation"]
    return 30, ["documentation"]


def _score_classification(db: Session, asset: CatalogAsset, field_count: int) -> tuple[int, list[str]]:
    if field_count == 0:
        return 35, ["schema_fields"]
    analysis = analyze_dataset(db, asset.id) or {}
    classified = int(analysis.get("field_count") or field_count)
    base = min(100, round((classified / field_count) * 100))
    tier = (asset.pii_tier or "").lower()
    if tier in SENSITIVE_PII_TIERS and analysis.get("pii_count", 0) == 0:
        base = max(0, base - 15)
    gaps = [] if base >= 75 else ["classification"]
    return base, gaps


def _score_lineage(asset: CatalogAsset, db: Session) -> tuple[int, list[str]]:
    if not (asset.lineage_node_key or "").strip():
        return 0, ["lineage_node_key"]
    score = 70
    try:
        from app.services.lineage_impact_service import analyze_asset_impact_by_id

        impact = analyze_asset_impact_by_id(db, asset.id)
        if impact:
            downstream = int(impact.get("downstream_count") or 0)
            if downstream > 0:
                score = min(100, 75 + min(downstream * 5, 25))
    except Exception:
        score = 80
    return score, [] if score >= 70 else ["lineage"]


def _score_rules(db: Session, fields: list[str]) -> tuple[int, list[str]]:
    if not fields:
        return 30, ["schema_fields"]
    active_rules = db.query(Rule).filter(func.lower(Rule.status) == "active").all()
    covered = 0
    for field in fields:
        token = field.lower()
        if any(token in (r.field or "").lower() or (r.field or "").lower() in token for r in active_rules):
            covered += 1
    coverage = min(100, round((covered / len(fields)) * 100))

    total = db.query(QuarantineData).count()
    if total:
        has_error = (QuarantineData.error.isnot(None)) & (QuarantineData.error != "")
        failed = db.query(QuarantineData).filter(has_error).count()
        success_rate = round(((total - failed) / total) * 100)
        coverage = min(100, round((coverage * 0.7) + (success_rate * 0.3)))

    gaps = [] if coverage >= GOVERNANCE_TARGET_SCORE else ["quality_rules"]
    return coverage, gaps


def _score_stewardship(db: Session) -> tuple[int, list[str]]:
    total = db.query(StewardshipQueue).count()
    if total == 0:
        return 90, []
    resolved = (
        db.query(StewardshipQueue)
        .filter(StewardshipQueue.status.in_(("approved", "resolved")))
        .count()
    )
    pending = db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()
    rate = round((resolved / total) * 100)
    if pending > 10:
        rate = max(0, rate - 15)
    gaps = [] if rate >= 70 else ["stewardship_backlog"]
    return min(100, rate), gaps


def _score_audit(db: Session) -> tuple[int, list[str]]:
    week_ago = datetime.utcnow() - timedelta(days=7)
    recent = (
        db.query(AuditLog)
        .filter(
            AuditLog.timestamp >= week_ago,
            AuditLog.action.in_(GOVERNANCE_AUDIT_ACTIONS),
        )
        .count()
    )
    total = db.query(AuditLog).count()
    if recent >= 5:
        return 100, []
    if recent > 0:
        return 75, []
    if total > 0:
        return 50, ["audit_activity"]
    return 40, ["audit_activity"]


def _risk_level(score: int) -> str:
    if score >= 80:
        return "Low Risk"
    if score >= 60:
        return "Medium Risk"
    return "High Risk"


def _weighted_overall(dimensions: dict[str, int]) -> int:
    total = 0.0
    for key, weight in DIMENSION_WEIGHTS.items():
        total += dimensions.get(key, 0) * weight
    return round(total)


def _gap_severity(score: int) -> str:
    if score < 50:
        return "high"
    if score < GOVERNANCE_TARGET_SCORE:
        return "medium"
    return "low"


def _build_governance_gaps(
    dimensions: dict[str, int],
    *,
    dataset_scores: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    for key in DIMENSION_KEYS:
        score = int(dimensions.get(key, 0))
        if score >= GOVERNANCE_TARGET_SCORE:
            continue
        affected: list[str] = []
        if dataset_scores:
            affected = [
                d["dataset_name"]
                for d in dataset_scores
                if int(d.get("dimensions", {}).get(key, 100)) < GOVERNANCE_TARGET_SCORE
            ][:6]
        gaps.append(
            {
                "dimension": key,
                "label": DIMENSION_LABELS[key],
                "score": score,
                "target": GOVERNANCE_TARGET_SCORE,
                "gap": GOVERNANCE_TARGET_SCORE - score,
                "severity": _gap_severity(score),
                "affected_datasets": affected,
            }
        )
    return sorted(gaps, key=lambda row: row["score"])


def _build_trends(db: Session, dimensions: dict[str, int], overall_score: int) -> list[dict[str, Any]]:
    week_ago = datetime.utcnow() - timedelta(days=7)
    two_weeks_ago = datetime.utcnow() - timedelta(days=14)
    recent_audits = (
        db.query(AuditLog)
        .filter(
            AuditLog.timestamp >= week_ago,
            AuditLog.action.in_(GOVERNANCE_AUDIT_ACTIONS),
        )
        .count()
    )
    previous_audits = (
        db.query(AuditLog)
        .filter(
            AuditLog.timestamp >= two_weeks_ago,
            AuditLog.timestamp < week_ago,
            AuditLog.action.in_(GOVERNANCE_AUDIT_ACTIONS),
        )
        .count()
    )
    audit_delta = recent_audits - previous_audits

    def _direction(delta: int) -> str:
        if delta > 0:
            return "up"
        if delta < 0:
            return "down"
        return "flat"

    trends: list[dict[str, Any]] = [
        {
            "key": "overall_score",
            "label": "Overall Governance",
            "score": overall_score,
            "direction": _direction(overall_score - GOVERNANCE_TARGET_SCORE),
            "delta": overall_score - GOVERNANCE_TARGET_SCORE,
            "unit": "pts vs target",
        },
        {
            "key": "audit_compliance",
            "label": "Audit Activity (7d)",
            "score": dimensions.get("audit_compliance", 0),
            "direction": _direction(audit_delta),
            "delta": audit_delta,
            "unit": "events",
        },
    ]
    for key in DIMENSION_KEYS:
        score = int(dimensions.get(key, 0))
        trends.append(
            {
                "key": key,
                "label": DIMENSION_LABELS[key],
                "score": score,
                "direction": _direction(score - GOVERNANCE_TARGET_SCORE),
                "delta": score - GOVERNANCE_TARGET_SCORE,
                "unit": "pts vs target",
            }
        )
    return trends


def _build_coverage_metrics(dimensions: dict[str, int]) -> list[dict[str, Any]]:
    return [
        {
            "key": key,
            "label": DIMENSION_LABELS[key],
            "score": int(dimensions.get(key, 0)),
            "status": "healthy" if dimensions.get(key, 0) >= GOVERNANCE_TARGET_SCORE else "needs_attention",
        }
        for key in DIMENSION_KEYS
    ]


def _enrich_platform_payload(
    db: Session,
    payload: dict[str, Any],
    *,
    dataset_scores: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    dimensions = payload.get("dimensions") or {}
    overall = int(payload.get("overall_score", 0))
    gaps = _build_governance_gaps(dimensions, dataset_scores=dataset_scores)
    payload["governance_gaps"] = gaps
    payload["coverage_metrics"] = _build_coverage_metrics(dimensions)
    payload["trends"] = _build_trends(db, dimensions, overall)
    payload["missing_governance_areas"] = payload.get("missing_governance_areas") or [
        gap["label"] for gap in gaps
    ]
    return payload


def _build_recommendations(dimensions: dict[str, int], missing: list[str]) -> list[str]:
    recs: list[str] = []
    seen: set[str] = set()
    for key in DIMENSION_KEYS:
        if dimensions.get(key, 0) < 70 and key in _RECOMMENDATIONS:
            text = _RECOMMENDATIONS[key]
            if text not in seen:
                recs.append(text)
                seen.add(text)
    for gap in missing:
        if gap == "owner_email" and "Assign data owners" not in " ".join(recs):
            recs.append("Assign a data owner for accountable stewardship.")
        if gap == "glossary_terms" and _RECOMMENDATIONS["glossary_coverage"] not in seen:
            recs.append(_RECOMMENDATIONS["glossary_coverage"])
            seen.add(_RECOMMENDATIONS["glossary_coverage"])
    return recs[:8]


def compute_dataset_score(db: Session, asset: CatalogAsset) -> dict[str, Any]:
    """Governance score for a single catalog asset."""
    fields = _parse_fields(asset.schema_fields)
    stewardship_score, stewardship_gaps = _score_stewardship(db)
    audit_score, audit_gaps = _score_audit(db)

    meta_score, meta_gaps = _score_metadata(asset)
    glossary_score, glossary_gaps = _score_glossary(db, asset.id, len(fields))
    doc_score, doc_gaps = _score_documentation(db, asset.id)
    class_score, class_gaps = _score_classification(db, asset, len(fields))
    lineage_score, lineage_gaps = _score_lineage(asset, db)
    rule_score, rule_gaps = _score_rules(db, fields)

    dimensions = {
        "metadata_coverage": meta_score,
        "glossary_coverage": glossary_score,
        "documentation_coverage": doc_score,
        "classification_coverage": class_score,
        "rule_coverage": rule_score,
        "lineage_coverage": lineage_score,
        "stewardship_resolution_rate": stewardship_score,
        "audit_compliance": audit_score,
    }

    all_gaps = list(
        dict.fromkeys(
            meta_gaps
            + glossary_gaps
            + doc_gaps
            + class_gaps
            + lineage_gaps
            + rule_gaps
            + stewardship_gaps
            + audit_gaps
        )
    )
    overall = _weighted_overall(dimensions)
    risk_score = max(0, min(100, 100 - overall))
    missing_areas = [
        DIMENSION_LABELS[k]
        for k in DIMENSION_KEYS
        if dimensions[k] < 70
    ]

    dimension_details = [
        {
            "key": key,
            "label": DIMENSION_LABELS[key],
            "score": dimensions[key],
            "weight_percent": round(DIMENSION_WEIGHTS[key] * 100),
        }
        for key in DIMENSION_KEYS
    ]

    governance_gaps = _build_governance_gaps(dimensions)

    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "domain": asset.domain or "Unassigned",
        "overall_score": overall,
        "governance_score": overall,
        "risk_score": risk_score,
        "risk_level": _risk_level(overall),
        "dimensions": dimensions,
        "dimension_details": dimension_details,
        "coverage_metrics": _build_coverage_metrics(dimensions),
        "governance_gaps": governance_gaps,
        "missing_governance_areas": missing_areas,
        "recommendations": _build_recommendations(dimensions, all_gaps),
        "field_count": len(fields),
    }


def compute_domain_scores(db: Session, dataset_scores: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_domain: dict[str, list[dict[str, Any]]] = {}
    for row in dataset_scores:
        domain = row.get("domain") or "Unassigned"
        by_domain.setdefault(domain, []).append(row)

    domains: list[dict[str, Any]] = []
    for domain, rows in sorted(by_domain.items(), key=lambda x: x[0].lower()):
        overall = round(sum(r["overall_score"] for r in rows) / len(rows))
        dim_avg: dict[str, int] = {}
        for key in DIMENSION_KEYS:
            dim_avg[key] = round(sum(r["dimensions"][key] for r in rows) / len(rows))
        domains.append(
            {
                "domain": domain,
                "dataset_count": len(rows),
                "overall_score": overall,
                "risk_score": max(0, 100 - overall),
                "risk_level": _risk_level(overall),
                "dimensions": dim_avg,
                "datasets": [
                    {
                        "dataset_id": r["dataset_id"],
                        "dataset_name": r["dataset_name"],
                        "overall_score": r["overall_score"],
                    }
                    for r in sorted(rows, key=lambda x: x["overall_score"])
                ],
            }
        )
    return domains


def compute_platform_score(db: Session) -> dict[str, Any]:
    """Platform-wide governance health rollup."""
    assets = db.query(CatalogAsset).order_by(CatalogAsset.id.asc()).all()
    dataset_scores = [compute_dataset_score(db, asset) for asset in assets]

    if not dataset_scores:
        stewardship_score, _ = _score_stewardship(db)
        audit_score, _ = _score_audit(db)
        dimensions = {
            "metadata_coverage": 0,
            "glossary_coverage": 0,
            "documentation_coverage": 0,
            "classification_coverage": 0,
            "rule_coverage": 0,
            "lineage_coverage": 0,
            "stewardship_resolution_rate": stewardship_score,
            "audit_compliance": audit_score,
        }
        overall = _weighted_overall(dimensions)
        payload = {
            "scope": "platform",
            "overall_score": overall,
            "risk_score": max(0, 100 - overall),
            "risk_level": _risk_level(overall),
            "dataset_count": 0,
            "domain_count": 0,
            "dimensions": dimensions,
            "dimension_details": [
                {
                    "key": k,
                    "label": DIMENSION_LABELS[k],
                    "score": dimensions[k],
                    "weight_percent": round(DIMENSION_WEIGHTS[k] * 100),
                }
                for k in DIMENSION_KEYS
            ],
            "recommendations": _build_recommendations(dimensions, []),
            "domains": [],
            "datasets": [],
            "datasets_needing_attention": [],
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        return _enrich_platform_payload(db, payload, dataset_scores=[])

    overall = round(sum(d["overall_score"] for d in dataset_scores) / len(dataset_scores))
    dim_avg: dict[str, int] = {}
    for key in DIMENSION_KEYS:
        dim_avg[key] = round(sum(d["dimensions"][key] for d in dataset_scores) / len(dataset_scores))

    domains = compute_domain_scores(db, dataset_scores)
    needing_attention = sorted(
        [d for d in dataset_scores if d["overall_score"] < 70],
        key=lambda x: x["overall_score"],
    )[:10]

    payload = {
        "scope": "platform",
        "overall_score": overall,
        "risk_score": max(0, 100 - overall),
        "risk_level": _risk_level(overall),
        "dataset_count": len(dataset_scores),
        "domain_count": len(domains),
        "dimensions": dim_avg,
        "dimension_details": [
            {
                "key": k,
                "label": DIMENSION_LABELS[k],
                "score": dim_avg[k],
                "weight_percent": round(DIMENSION_WEIGHTS[k] * 100),
            }
            for k in DIMENSION_KEYS
        ],
        "recommendations": _build_recommendations(dim_avg, []),
        "domains": domains,
        "datasets": [
            {
                "dataset_id": d["dataset_id"],
                "dataset_name": d["dataset_name"],
                "domain": d["domain"],
                "overall_score": d["overall_score"],
                "governance_score": d["overall_score"],
                "risk_level": d["risk_level"],
            }
            for d in sorted(dataset_scores, key=lambda x: -x["overall_score"])
        ],
        "datasets_needing_attention": [
            {
                "dataset_id": d["dataset_id"],
                "dataset_name": d["dataset_name"],
                "overall_score": d["overall_score"],
                "governance_score": d["overall_score"],
                "missing_governance_areas": d["missing_governance_areas"][:4],
                "governance_gaps": d.get("governance_gaps", [])[:3],
            }
            for d in needing_attention
        ],
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
    return _enrich_platform_payload(db, payload, dataset_scores=dataset_scores)


def build_governance_dashboard(db: Session) -> dict[str, Any]:
    """Executive dashboard payload with scores, gaps, trends, and recommendations."""
    return compute_platform_score(db)


def get_dataset_governance_score(db: Session, dataset_id: int) -> dict[str, Any] | None:
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == dataset_id).first()
    if not asset:
        return None
    result = compute_dataset_score(db, asset)
    result["scope"] = "dataset"
    result["governance_score"] = result["overall_score"]
    result["trends"] = _build_trends(db, result["dimensions"], result["overall_score"])
    result["generated_at"] = datetime.utcnow().isoformat() + "Z"
    return result
