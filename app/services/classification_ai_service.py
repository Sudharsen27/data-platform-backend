"""AI-enhanced classification explanations via Groq (with heuristic fallback)."""

from __future__ import annotations

import json
from typing import Any

from app.services.ai_copilot import ai_enabled
from app.services.llm_provider import chat_completion_text, llm_is_available

_SYSTEM_PROMPT = """You are a data governance architect explaining data classification results.

Only use the supplied classification JSON. Never invent fields or regulations.
Write professionally for governance teams. Use bullet points for recommendations.
Include classification, reason, masking, encryption, retention, and access guidance when relevant."""


def _heuristic_field_explanation(result: dict[str, Any]) -> str:
    field = result.get("field_name", "field")
    classification = result.get("classification", "Public")
    reason = result.get("reason", "")
    recs = result.get("recommendations") or []
    lines = [
        f"Classification: {classification}",
        f"Reason: {reason}",
    ]
    if recs:
        lines.append("Recommendations:")
        lines.extend(f"- {r}" for r in recs[:5])
    return "\n".join(lines)


def _heuristic_dataset_explanation(result: dict[str, Any]) -> str:
    name = result.get("dataset_name", "Dataset")
    lines = [
        f"Dataset '{name}' is classified as {result.get('dataset_classification', 'Unknown')}.",
        f"Risk score: {result.get('risk_score', 0)}/100.",
        f"PII fields: {result.get('pii_count', 0)}. Sensitive fields: {result.get('sensitive_count', 0)}.",
    ]
    recs = result.get("recommendations") or []
    if recs:
        lines.append("Governance recommendations:")
        lines.extend(f"- {r}" for r in recs[:6])
    return "\n".join(lines)


def _call_llm(*, prompt: str, payload: dict[str, Any]) -> tuple[str, str]:
    user_content = (
        f"{prompt}\n\n"
        f"Classification data (JSON):\n{json.dumps(payload, ensure_ascii=True, indent=2)}"
    )
    return chat_completion_text(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=user_content,
        temperature=0.2,
        max_tokens=700,
    )


def enhance_field_classification(result: dict[str, Any]) -> dict[str, Any]:
    """Add AI explanation to a field classification result."""
    if not ai_enabled() or not llm_is_available():
        return {
            **result,
            "ai_explanation": _heuristic_field_explanation(result),
            "source_engine": "heuristics",
        }
    try:
        explanation, engine = _call_llm(
            prompt=f"Explain the classification for field '{result.get('field_name')}'.",
            payload=result,
        )
        return {**result, "ai_explanation": explanation, "source_engine": engine}
    except (OSError, RuntimeError, ValueError):
        return {
            **result,
            "ai_explanation": _heuristic_field_explanation(result),
            "source_engine": "heuristics",
        }


def enhance_dataset_classification(result: dict[str, Any]) -> dict[str, Any]:
    """Add AI summary to a dataset classification result."""
    if not ai_enabled() or not llm_is_available():
        return {
            **result,
            "ai_summary": _heuristic_dataset_explanation(result),
            "source_engine": "heuristics",
        }
    try:
        summary, engine = _call_llm(
            prompt=f"Summarize governance classification for dataset '{result.get('dataset_name')}'.",
            payload={
                k: result.get(k)
                for k in (
                    "dataset_name",
                    "dataset_classification",
                    "risk_score",
                    "pii_count",
                    "sensitive_count",
                    "financial_count",
                    "confidential_count",
                    "pii_fields",
                    "sensitive_fields",
                    "recommendations",
                    "summary",
                )
            },
        )
        return {**result, "ai_summary": summary, "source_engine": engine}
    except (OSError, RuntimeError, ValueError):
        return {
            **result,
            "ai_summary": _heuristic_dataset_explanation(result),
            "source_engine": "heuristics",
        }
