"""Business-friendly lineage impact explanations via Groq (with heuristic fallback)."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from app.services.ai_copilot import ai_enabled, ai_provider
from app.services.copilot_service import groq_api_key, groq_is_configured, groq_model, groq_timeout_seconds

_SYSTEM_PROMPT = """You are a data governance architect explaining lineage impact to business users.

Only use the supplied impact analysis JSON. Never invent assets, systems, or counts.
If data is missing, say it is not available in the metadata repository.

Write in clear, professional prose. Use bullet points for impacted assets.
Mention impact score, downstream/upstream counts, critical dependencies, rules, reports, and master data when present."""


def _heuristic_explanation(*, question: str, impact: dict[str, Any]) -> str:
    source = impact.get("source_asset") or "the selected asset"
    field = impact.get("field")
    score = impact.get("impact_score", 0)
    downstream = impact.get("downstream_count", 0)
    upstream = impact.get("upstream_count", 0)
    rules = impact.get("rules_impacted", 0)
    reports = impact.get("reports_impacted", 0)
    master = impact.get("master_data_impacted", 0)

    subject = f"Field '{field}'" if field else source
    lines = [
        f"{subject} has an estimated lineage impact score of {score}/100.",
        f"Downstream dependencies: {downstream}. Upstream sources: {upstream}.",
    ]

    critical = impact.get("critical_dependencies") or impact.get("critical_assets") or []
    if critical:
        names = ", ".join(a.get("name", a.get("asset_key", "")) for a in critical[:6])
        lines.append(f"Critical dependencies: {names}.")

    downstream_assets = impact.get("downstream_assets") or []
    if downstream_assets:
        names = ", ".join(a.get("name", a.get("asset_key", "")) for a in downstream_assets[:6])
        lines.append(f"Potentially impacted downstream assets: {names}.")

    if rules:
        lines.append(f"{rules} active quality rule(s) may be affected.")
    if reports:
        lines.append(f"{reports} reporting/consumption asset(s) may be affected.")
    if master:
        lines.append(f"{master} golden master record(s) could require steward review.")

    if field:
        lines.insert(
            0,
            f"{field.replace('_', ' ').title()} is referenced in lineage transformations and catalog metadata.",
        )

    return " ".join(lines)


def _call_groq(*, question: str, impact: dict[str, Any]) -> str:
    api_key = groq_api_key()
    if not api_key:
        raise ValueError("GROQ_API_KEY is not configured")

    payload = {
        k: impact.get(k)
        for k in (
            "source_asset",
            "field",
            "impact_score",
            "downstream_count",
            "upstream_count",
            "datasets_impacted",
            "rules_impacted",
            "reports_impacted",
            "master_data_impacted",
            "downstream_assets",
            "upstream_assets",
            "critical_dependencies",
            "impacted_rules",
            "impacted_reports",
            "impacted_master_data",
            "summary",
        )
    }

    user_content = (
        f"User question:\n{question.strip()}\n\n"
        f"Lineage impact analysis (JSON):\n{json.dumps(payload, ensure_ascii=True, indent=2)}"
    )

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
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.2,
                "max_tokens": 800,
            },
        )
        if response.status_code != 200:
            raise RuntimeError(f"Groq API error ({response.status_code})")
        choices = response.json().get("choices") or []
        text = (choices[0].get("message") or {}).get("content") or ""
        text = re.sub(r"\s+", " ", text).strip()
        if not text:
            raise RuntimeError("Empty Groq response")
        return text


def explain_lineage_impact(*, question: str, impact: dict[str, Any]) -> dict[str, Any]:
    """Return analysis text, impacts list, and source engine."""
    impacts = []
    for bucket, level in (
        ("critical_dependencies", "high"),
        ("downstream_assets", "medium"),
        ("upstream_assets", "low"),
        ("impacted_reports", "high"),
    ):
        for item in impact.get(bucket) or []:
            impacts.append(
                {
                    "name": item.get("name") or item.get("asset_key") or item.get("label", ""),
                    "type": item.get("asset_type") or "dataset",
                    "impact_level": item.get("impact_level") or level,
                    "system": item.get("system", ""),
                    "layer": item.get("layer", ""),
                }
            )

    if not ai_enabled():
        return {
            "analysis": _heuristic_explanation(question=question, impact=impact),
            "impacts": impacts[:20],
            "source_engine": "heuristics",
        }

    if ai_provider() == "groq" and groq_is_configured():
        try:
            return {
                "analysis": _call_groq(question=question, impact=impact),
                "impacts": impacts[:20],
                "source_engine": "groq",
            }
        except (httpx.HTTPError, OSError, RuntimeError, ValueError):
            pass

    return {
        "analysis": _heuristic_explanation(question=question, impact=impact),
        "impacts": impacts[:20],
        "source_engine": "heuristics",
    }
