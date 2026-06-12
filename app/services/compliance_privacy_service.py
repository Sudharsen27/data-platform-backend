"""Compliance & privacy dashboard — reuses governance health metrics and classification analysis."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models import CatalogAsset
from app.services.dashboard_metrics import SENSITIVE_PII_TIERS
from app.services.data_classification_service import analyze_dataset
from app.services.governance_score_service import (
    GOVERNANCE_TARGET_SCORE,
    compute_dataset_score,
    compute_platform_score,
)

_COMPLIANCE_WEIGHTS = {
    "classification_coverage": 0.35,
    "documentation_coverage": 0.25,
    "metadata_coverage": 0.20,
    "rule_coverage": 0.20,
}

_PRIVACY_RECOMMENDATIONS: dict[str, str] = {
    "classification_coverage": "Run field-level classification on all catalog datasets and align PII tier labels.",
    "documentation_coverage": "Publish approved privacy and data-handling documentation for regulated datasets.",
    "metadata_coverage": "Complete owner, domain, and PII tier metadata for every sensitive asset.",
    "rule_coverage": "Enforce masking, format, and retention quality rules on PII and sensitive fields.",
    "missing_classification": "Classify datasets that lack field-level sensitivity labels before production use.",
    "missing_documentation": "Document lawful basis, retention, and access controls for datasets holding personal data.",
    "pii_exposure": "Review PII assets without approved documentation or active quality controls.",
    "tier_mismatch": "Reconcile catalog PII tier with detected field classifications.",
}


def _risk_level(score: int) -> str:
    if score >= 80:
        return "Low Risk"
    if score >= 60:
        return "Medium Risk"
    return "High Risk"


def _compliance_score(dimensions: dict[str, int]) -> int:
    total = 0.0
    for key, weight in _COMPLIANCE_WEIGHTS.items():
        total += int(dimensions.get(key, 0)) * weight
    return round(total)


def _asset_summary(
    asset: CatalogAsset,
    *,
    analysis: dict[str, Any] | None,
    dataset_score: dict[str, Any],
) -> dict[str, Any]:
    dims = dataset_score.get("dimensions") or {}
    analysis = analysis or {}
    return {
        "dataset_id": asset.id,
        "dataset_key": asset.asset_key,
        "dataset_name": asset.name,
        "domain": asset.domain or "Unassigned",
        "pii_tier": asset.pii_tier or "",
        "dataset_classification": analysis.get("dataset_classification", ""),
        "pii_field_count": int(analysis.get("pii_count") or 0),
        "sensitive_field_count": int(analysis.get("sensitive_count") or 0),
        "classification_score": int(dims.get("classification_coverage", 0)),
        "documentation_score": int(dims.get("documentation_coverage", 0)),
        "risk_score": int(analysis.get("risk_score") or dataset_score.get("risk_score", 0)),
        "risk_level": dataset_score.get("risk_level", "Medium Risk"),
    }


def _is_pii_asset(asset: CatalogAsset, analysis: dict[str, Any] | None) -> bool:
    tier = (asset.pii_tier or "").lower()
    if tier in SENSITIVE_PII_TIERS or tier == "pii":
        return True
    if not analysis:
        return False
    if int(analysis.get("pii_count") or 0) > 0:
        return True
    return (analysis.get("dataset_classification") or "") == "PII"


def _is_sensitive_asset(asset: CatalogAsset, analysis: dict[str, Any] | None) -> bool:
    if _is_pii_asset(asset, analysis):
        return True
    if not analysis:
        tier = (asset.pii_tier or "").lower()
        return tier in {"sensitive", "confidential", "restricted", "high"}
    if int(analysis.get("sensitive_count") or 0) > 0:
        return True
    return (analysis.get("dataset_classification") or "") in ("Sensitive", "Confidential", "Financial")


def _build_governance_risks(
    *,
    pii_assets: list[dict[str, Any]],
    missing_classification: list[dict[str, Any]],
    missing_documentation: list[dict[str, Any]],
    governance_gaps: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = []

    if missing_classification:
        risks.append(
            {
                "risk_id": "unclassified_datasets",
                "title": "Datasets missing classification",
                "severity": "high" if len(missing_classification) >= 3 else "medium",
                "category": "classification",
                "affected_datasets": [row["dataset_name"] for row in missing_classification[:6]],
                "description": (
                    f"{len(missing_classification)} dataset(s) lack adequate field-level "
                    "classification coverage for privacy governance."
                ),
            }
        )

    if missing_documentation:
        risks.append(
            {
                "risk_id": "undocumented_sensitive_data",
                "title": "Sensitive data without documentation",
                "severity": "high",
                "category": "documentation",
                "affected_datasets": [row["dataset_name"] for row in missing_documentation[:6]],
                "description": (
                    f"{len(missing_documentation)} dataset(s) are missing approved privacy "
                    "or handling documentation."
                ),
            }
        )

    pii_undocumented = [
        row
        for row in pii_assets
        if int(row.get("documentation_score") or 0) < GOVERNANCE_TARGET_SCORE
    ]
    if pii_undocumented:
        risks.append(
            {
                "risk_id": "pii_without_documentation",
                "title": "PII assets lacking documentation",
                "severity": "high",
                "category": "privacy",
                "affected_datasets": [row["dataset_name"] for row in pii_undocumented[:6]],
                "description": (
                    "Personal data assets exist without sufficient approved documentation "
                    "and governance controls."
                ),
            }
        )

    privacy_gap_keys = {"classification_coverage", "documentation_coverage", "metadata_coverage", "rule_coverage"}
    for gap in governance_gaps:
        if gap.get("dimension") not in privacy_gap_keys:
            continue
        risks.append(
            {
                "risk_id": f"gap_{gap.get('dimension')}",
                "title": f"{gap.get('label', 'Governance')} below target",
                "severity": gap.get("severity", "medium"),
                "category": "governance",
                "affected_datasets": gap.get("affected_datasets") or [],
                "description": (
                    f"Platform {gap.get('label', 'metric')} is {gap.get('score', 0)}% "
                    f"(target {gap.get('target', GOVERNANCE_TARGET_SCORE)}%)."
                ),
            }
        )

    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in risks:
        rid = row["risk_id"]
        if rid in seen:
            continue
        seen.add(rid)
        unique.append(row)
    severity_order = {"high": 0, "medium": 1, "low": 2}
    return sorted(unique, key=lambda r: severity_order.get(r.get("severity", "low"), 3))[:12]


def _build_compliance_recommendations(
    *,
    dimensions: dict[str, int],
    missing_classification_count: int,
    missing_documentation_count: int,
    pii_asset_count: int,
    governance_recommendations: list[str],
) -> list[dict[str, Any]]:
    recs: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(priority: str, title: str, description: str) -> None:
        key = title.lower()
        if key in seen:
            return
        seen.add(key)
        recs.append({"priority": priority, "title": title, "description": description})

    for dim_key, weight_key in (
        ("classification_coverage", "classification_coverage"),
        ("documentation_coverage", "documentation_coverage"),
        ("metadata_coverage", "metadata_coverage"),
        ("rule_coverage", "rule_coverage"),
    ):
        if int(dimensions.get(dim_key, 0)) < GOVERNANCE_TARGET_SCORE:
            text = _PRIVACY_RECOMMENDATIONS[weight_key]
            _add(
                "high" if dim_key == "classification_coverage" else "medium",
                text.split(".")[0],
                text,
            )

    if missing_classification_count:
        text = _PRIVACY_RECOMMENDATIONS["missing_classification"]
        _add("high", "Complete dataset classification", text)

    if missing_documentation_count:
        text = _PRIVACY_RECOMMENDATIONS["missing_documentation"]
        _add("high", "Document regulated datasets", text)

    if pii_asset_count and int(dimensions.get("documentation_coverage", 0)) < GOVERNANCE_TARGET_SCORE:
        text = _PRIVACY_RECOMMENDATIONS["pii_exposure"]
        _add("high", "Harden PII asset controls", text)

    for item in governance_recommendations:
        lower = item.lower()
        if any(token in lower for token in ("classification", "pii", "documentation", "privacy", "glossary")):
            _add("medium", item[:80], item)

    return recs[:10]


def build_compliance_dashboard(db: Session) -> dict[str, Any]:
    """Executive compliance & privacy dashboard payload."""
    governance = compute_platform_score(db)
    dimensions = governance.get("dimensions") or {}
    compliance = _compliance_score(dimensions)
    risk_score = max(0, min(100, 100 - compliance))

    assets = db.query(CatalogAsset).order_by(CatalogAsset.id.asc()).all()
    pii_assets: list[dict[str, Any]] = []
    sensitive_assets: list[dict[str, Any]] = []
    missing_classification: list[dict[str, Any]] = []
    missing_documentation: list[dict[str, Any]] = []

    for asset in assets:
        dataset_score = compute_dataset_score(db, asset)
        analysis = analyze_dataset(db, asset.id)
        summary = _asset_summary(asset, analysis=analysis, dataset_score=dataset_score)
        class_score = summary["classification_score"]
        doc_score = summary["documentation_score"]

        if class_score < GOVERNANCE_TARGET_SCORE:
            missing_classification.append(summary)
        if doc_score < GOVERNANCE_TARGET_SCORE:
            missing_documentation.append(summary)
        if _is_pii_asset(asset, analysis):
            pii_assets.append(summary)
        if _is_sensitive_asset(asset, analysis):
            sensitive_assets.append(summary)

    other_count = max(0, len(assets) - len(sensitive_assets))
    asset_distribution = [
        {"label": "PII assets", "count": len(pii_assets), "color_key": "rose"},
        {"label": "Sensitive (incl. PII)", "count": len(sensitive_assets), "color_key": "amber"},
        {"label": "Other datasets", "count": other_count, "color_key": "emerald"},
    ]

    governance_risks = _build_governance_risks(
        pii_assets=pii_assets,
        missing_classification=missing_classification,
        missing_documentation=missing_documentation,
        governance_gaps=governance.get("governance_gaps") or [],
    )

    compliance_recommendations = _build_compliance_recommendations(
        dimensions=dimensions,
        missing_classification_count=len(missing_classification),
        missing_documentation_count=len(missing_documentation),
        pii_asset_count=len(pii_assets),
        governance_recommendations=governance.get("recommendations") or [],
    )

    return {
        "scope": "platform",
        "compliance_score": compliance,
        "risk_score": risk_score,
        "risk_level": _risk_level(compliance),
        "dataset_count": len(assets),
        "pii_asset_count": len(pii_assets),
        "sensitive_asset_count": len(sensitive_assets),
        "datasets_missing_classification": len(missing_classification),
        "datasets_missing_documentation": len(missing_documentation),
        "classification_coverage": int(dimensions.get("classification_coverage", 0)),
        "documentation_coverage": int(dimensions.get("documentation_coverage", 0)),
        "governance_dimensions": dimensions,
        "governance_overall_score": int(governance.get("overall_score", 0)),
        "asset_distribution": asset_distribution,
        "coverage_chart": [
            {
                "key": key,
                "label": key.replace("_", " ").title(),
                "score": int(dimensions.get(key, 0)),
            }
            for key in _COMPLIANCE_WEIGHTS
        ],
        "missing_classification_datasets": sorted(
            missing_classification, key=lambda row: row["classification_score"]
        )[:15],
        "missing_documentation_datasets": sorted(
            missing_documentation, key=lambda row: row["documentation_score"]
        )[:15],
        "pii_assets": sorted(pii_assets, key=lambda row: -row["risk_score"])[:15],
        "governance_risks": governance_risks,
        "compliance_recommendations": compliance_recommendations,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def compute_compliance_score(db: Session) -> dict[str, Any]:
    """Summary compliance score rollup (lighter than full dashboard)."""
    dashboard = build_compliance_dashboard(db)
    return {
        "scope": "platform",
        "compliance_score": dashboard["compliance_score"],
        "risk_score": dashboard["risk_score"],
        "risk_level": dashboard["risk_level"],
        "dataset_count": dashboard["dataset_count"],
        "pii_asset_count": dashboard["pii_asset_count"],
        "sensitive_asset_count": dashboard["sensitive_asset_count"],
        "datasets_missing_classification": dashboard["datasets_missing_classification"],
        "datasets_missing_documentation": dashboard["datasets_missing_documentation"],
        "classification_coverage": dashboard["classification_coverage"],
        "documentation_coverage": dashboard["documentation_coverage"],
        "generated_at": dashboard["generated_at"],
    }
