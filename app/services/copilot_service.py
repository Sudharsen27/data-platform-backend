"""AI Governance Copilot — Groq / Azure OpenAI with metadata-grounded responses."""

from __future__ import annotations

import json
from typing import Any

import httpx

from app.services.ai_copilot import ai_enabled
from app.services.governance_context_service import build_governance_context
from app.services.llm_provider import chat_completion_text, llm_is_available

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


def _format_context_for_prompt(context: dict[str, Any]) -> str:
    return json.dumps(context, ensure_ascii=True, indent=2)


def _call_llm_copilot(
    *, question: str, context: dict[str, Any], page_context: dict[str, Any] | None = None
) -> tuple[str, str]:
    page_block = ""
    if page_context:
        page_block = f"\nCurrent UI page context (JSON):\n{json.dumps(page_context, ensure_ascii=True, indent=2)}\n"

    user_content = (
        f"User question:\n{question.strip()}\n"
        f"{page_block}\n"
        f"Governance metadata context (JSON):\n{_format_context_for_prompt(context)}"
    )
    return chat_completion_text(
        system_prompt=_SYSTEM_PROMPT,
        user_prompt=user_content,
        temperature=0.2,
        max_tokens=1024,
    )


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
        for term in glossary[:4]:
            title = term.get("title") or term.get("term")
            definition = term.get("definition") or ""
            if definition and len(definition) > 40:
                parts.append(f"{title}: {definition}")
            else:
                parts.append(f"{title} (from {term.get('source_asset', 'catalog')})")
    glossary_analysis = context.get("glossary_analysis") or {}
    field_glossary = glossary_analysis.get("field") if isinstance(glossary_analysis, dict) else None
    if field_glossary:
        parts.append(
            f"Glossary — {field_glossary.get('title', field_glossary.get('field_name', 'field'))}: "
            f"{field_glossary.get('definition', '')}"
        )
    dataset_glossary = glossary_analysis.get("dataset") if isinstance(glossary_analysis, dict) else None
    if dataset_glossary:
        parts.append(
            f"Dataset glossary — {dataset_glossary.get('dataset_title', 'Dataset')}: "
            f"{dataset_glossary.get('dataset_definition', '')}"
        )

    remediation = context.get("remediation_analysis") or {}
    if remediation:
        parts.append(
            f"Remediation — Root cause: {remediation.get('root_cause', '')}. "
            f"Suggested fix: {remediation.get('suggested_fix', '')}"
        )
        if remediation.get("business_impact"):
            parts.append(f"Business impact: {remediation['business_impact']}")
        if remediation.get("governance_impact"):
            parts.append(f"Governance impact: {remediation['governance_impact']}")

    rule_rec = context.get("rule_recommendation_analysis") or {}
    if rule_rec:
        rules_list = rule_rec.get("rules") or rule_rec.get("recommended_rules") or []
        if rules_list:
            lines = [
                f"{r.get('field_name')}: {r.get('rule_text')} ({r.get('rule_type')}, {r.get('confidence')}%)"
                for r in rules_list[:6]
            ]
            parts.append("Recommended rules: " + "; ".join(lines))
        risk = rule_rec.get("risk_analysis") or {}
        if risk:
            parts.append(
                f"Risk — Quality: {risk.get('data_quality_risk_level', 'N/A')}, "
                f"Governance: {risk.get('governance_risk_level', 'N/A')}, "
                f"Compliance: {risk.get('compliance_risk_level', 'N/A')}."
            )

    score_analysis = context.get("governance_score_analysis") or {}
    if score_analysis:
        scope = score_analysis.get("scope", "platform")
        overall = score_analysis.get("overall_score", 0)
        risk = score_analysis.get("risk_level", "N/A")
        parts.append(
            f"Governance health ({scope}): overall score {overall}/100, risk level {risk}."
        )
        dims = score_analysis.get("dimensions") or {}
        if dims:
            low_dims = [
                f"{k.replace('_', ' ')}: {v}%"
                for k, v in sorted(dims.items(), key=lambda x: x[1])[:4]
            ]
            parts.append("Dimension scores (lowest): " + ", ".join(low_dims) + ".")
        missing = score_analysis.get("missing_governance_areas") or []
        if missing:
            parts.append("Gaps: " + "; ".join(missing[:5]) + ".")
        attention = score_analysis.get("datasets_needing_attention") or []
        if attention and any(w in q for w in ("attention", "gap", "which dataset", "need")):
            names = ", ".join(
                f"{d.get('dataset_name')} ({d.get('overall_score')}/100)"
                for d in attention[:5]
            )
            parts.append(f"Datasets needing attention: {names}.")
        recs = score_analysis.get("recommendations") or []
        if recs:
            parts.append("Recommendations: " + " ".join(recs[:3]))
        if any(w in q for w in ("recommend", "improve", "improvement")):
            gaps = score_analysis.get("governance_gaps") or []
            if gaps:
                gap_lines = [
                    f"{g.get('label')} ({g.get('score')}% — gap {g.get('gap')} pts)"
                    for g in gaps[:4]
                ]
                parts.append("Priority improvements: " + "; ".join(gap_lines) + ".")

    doc_analysis = context.get("documentation_analysis") or {}
    if doc_analysis:
        parts.append(
            f"Dataset documentation — {doc_analysis.get('title', 'Dataset')}: "
            f"{doc_analysis.get('summary', '')}"
        )
        if doc_analysis.get("purpose"):
            parts.append(f"Purpose: {doc_analysis['purpose']}")
        if doc_analysis.get("governance_notes"):
            parts.append(f"Governance notes: {doc_analysis['governance_notes']}")
        key_fields = doc_analysis.get("key_fields") or []
        if key_fields and any(w in q for w in ("key field", "explain field", "fields")):
            field_lines = [
                f"{f.get('field_name')}: {f.get('description', '')[:120]}"
                for f in key_fields[:5]
            ]
            parts.append("Key fields: " + "; ".join(field_lines))

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

    if llm_is_available():
        try:
            answer, engine = _call_llm_copilot(
                question=trimmed,
                context=context,
                page_context=ui_page_context or page_context,
            )
            return {
                "answer": answer,
                "sources": sources,
                "source_engine": engine,
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
