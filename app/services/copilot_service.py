"""AI Governance Copilot — Groq LLM with metadata-grounded responses."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx

from app.services.ai_copilot import ai_enabled, ai_provider
from app.services.governance_context_service import build_governance_context

_SYSTEM_PROMPT = """You are an AI Governance Copilot for a Metadata Management platform.

Only answer using supplied metadata.
If information is unavailable, clearly say that it is not available in the metadata repository.
Never hallucinate.

Focus on:
- Catalog
- Governance
- Glossary
- Lineage
- Rules
- Stewardship

Respond in clear, concise prose. Use bullet points when listing multiple items.
Cite asset keys, field names, or rule IDs from the metadata when relevant."""


def groq_api_key() -> str:
    return os.getenv("GROQ_API_KEY", "").strip()


def groq_model() -> str:
    return os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip() or "llama-3.3-70b-versatile"


def groq_timeout_seconds() -> float:
    try:
        return max(10.0, float(os.getenv("GROQ_TIMEOUT_SECONDS", "60")))
    except ValueError:
        return 60.0


def groq_is_configured() -> bool:
    return bool(groq_api_key())


def _format_context_for_prompt(context: dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=True, indent=2)


def _call_groq(*, question: str, context: dict[str, Any], page_context: dict[str, Any] | None = None) -> str:
    api_key = groq_api_key()
    if not api_key:
        raise ValueError("GROQ_API_KEY is not configured")

    page_block = ""
    if page_context:
        page_block = f"\nCurrent UI page context (JSON):\n{json.dumps(page_context, ensure_ascii=True, indent=2)}\n"

    user_content = (
        f"User question:\n{question.strip()}\n"
        f"{page_block}\n"
        f"Governance metadata context (JSON):\n{_format_context_for_prompt(context)}"
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
                "max_tokens": 1024,
            },
        )
        if response.status_code != 200:
            detail = response.text[:300]
            raise RuntimeError(f"Groq API error ({response.status_code}): {detail}")

        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError("Groq API returned no choices")
        message = choices[0].get("message") or {}
        text = (message.get("content") or "").strip()
        if not text:
            raise RuntimeError("Groq API returned empty content")
        return re.sub(r"\s+", " ", text).strip()


def _heuristic_answer(*, question: str, context: dict[str, Any]) -> str:
    """Fallback when Groq is unavailable — summarize retrieved metadata only."""
    q = (question or "").lower()
    parts: list[str] = []

    datasets = context.get("datasets") or []
    if datasets and any(w in q for w in ("dataset", "catalog", "list", "available", "summarize")):
        names = ", ".join(d["name"] for d in datasets[:6])
        parts.append(f"Catalog datasets in scope: {names}.")

    glossary = context.get("glossary_terms") or []
    if glossary:
        term_lines = [f"{t['term']} (from {t['source_asset']})" for t in glossary[:6]]
        parts.append(f"Glossary-related fields: {', '.join(term_lines)}.")

    if "customer" in q and datasets:
        customer_assets = [d for d in datasets if "customer" in (d.get("name") or "").lower()
                          or "customer" in (d.get("asset_key") or "").lower()]
        if customer_assets:
            parts.append(
                "Customer-related assets: "
                + ", ".join(f"{d['name']} [{d['asset_key']}]" for d in customer_assets)
            )

    rules = context.get("rules") or []
    if rules or "rule" in q or "quality" in q:
        if rules:
            rule_text = "; ".join(f"Rule #{r['id']} on {r['field']}: {r['rule']}" for r in rules[:5])
            parts.append(f"Active quality rules: {rule_text}.")
        elif "rule" in q or "quality" in q:
            parts.append("No matching quality rules found in the metadata repository.")

    lineage = context.get("lineage") or {}
    nodes = lineage.get("nodes") or []
    edges = lineage.get("edges") or []
    if "lineage" in q and (nodes or edges):
        if nodes:
            parts.append("Lineage nodes: " + ", ".join(f"{n['label']} ({n['key']})" for n in nodes[:5]))
        if edges:
            flow = " → ".join(
                f"{e['source_key']}→{e['target_key']}" for e in edges[:4]
            )
            parts.append(f"Data flow: {flow}.")

    master = context.get("master_data") or []
    if master:
        parts.append(
            "Master data samples: "
            + ", ".join(f"{m['name']} <{m['email']}>" for m in master[:4])
        )

    stewardship = context.get("stewardship") or []
    if stewardship:
        parts.append(
            f"Stewardship: {len(stewardship)} matching task(s), "
            f"e.g. #{stewardship[0]['id']} — {stewardship[0]['issue'][:80]}."
        )

    if not parts:
        counts = context.get("summary_counts") or {}
        parts.append(
            "Based on the metadata repository: "
            f"{counts.get('datasets', 0)} catalog asset(s), "
            f"{counts.get('rules', 0)} rule(s), "
            f"{counts.get('lineage_nodes', 0)} lineage node(s). "
            "No closely matching records for this question — try naming a catalog asset, field, or domain."
        )

    return " ".join(parts)


def answer_governance_question(
    db,
    *,
    question: str,
    page_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Retrieve governance context and produce a grounded answer.
    Returns dict with keys: answer, sources, source_engine, context_summary.
    """
    trimmed = (question or "").strip()
    if not trimmed:
        raise ValueError("Question is required")

    context = build_governance_context(db, trimmed, page_context=page_context)
    sources = context.get("sources") or []
    ui_page_context = context.get("page_context") or {}

    if not ai_enabled():
        answer = _heuristic_answer(question=trimmed, context=context)
        return {
            "answer": answer,
            "sources": sources,
            "source_engine": "heuristics",
            "context_summary": context.get("summary_counts") or {},
        }

    if ai_provider() == "groq" and groq_is_configured():
        try:
            answer = _call_groq(
                question=trimmed,
                context=context,
                page_context=ui_page_context or page_context,
            )
            return {
                "answer": answer,
                "sources": sources,
                "source_engine": "groq",
                "context_summary": context.get("summary_counts") or {},
            }
        except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
            fallback = _heuristic_answer(question=trimmed, context=context)
            return {
                "answer": (
                    f"{fallback} (LLM unavailable: {str(exc)[:120]}. "
                    "Showing metadata summary only.)"
                ),
                "sources": sources,
                "source_engine": "heuristics",
                "context_summary": context.get("summary_counts") or {},
            }

    answer = _heuristic_answer(question=trimmed, context=context)
    return {
        "answer": answer,
        "sources": sources,
        "source_engine": "heuristics",
        "context_summary": context.get("summary_counts") or {},
    }
