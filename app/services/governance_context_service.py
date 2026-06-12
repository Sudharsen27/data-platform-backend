"""Retrieve relevant governance metadata for AI copilot context (no full DB dump)."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models import (
    CatalogAsset,
    GlossaryEntry,
    LineageEdge,
    LineageNode,
    MasterData,
    QuarantineData,
    RecordAnnotation,
    Rule,
    StewardshipQueue,
)

_REMEDIATION_QUESTION_HINTS = (
    "why did this record fail",
    "why did this fail",
    "how can i fix",
    "how to fix",
    "business impact",
    "remediation",
    "root cause",
    "suggested fix",
    "explain failure",
)

_RULE_RECOMMENDATION_HINTS = (
    "rule",
    "rules",
    "quality check",
    "quality checks",
    "validation",
    "missing governance",
    "recommend",
    "suggest rule",
    "what rules",
    "data quality rule",
)

_DOCUMENTATION_QUESTION_HINTS = (
    "documentation",
    "document",
    "purpose of this dataset",
    "business purpose",
    "governance notes",
    "key fields",
    "usage guidelines",
    "compliance",
    "data owner",
    "describe customer master",
    "describe dataset",
)

_GLOSSARY_QUESTION_HINTS = (
    "glossary",
    "definition",
    "define",
    "what is",
    "explain",
    "describe",
    "business term",
    "meaning of",
)

_CLASSIFICATION_QUESTION_HINTS = (
    "pii",
    "sensitive",
    "classification",
    "classify",
    "masking",
    "governance risk",
    "confidential",
    "financial data",
    "which datasets contain",
    "sensitive fields",
)

_GOVERNANCE_SCORE_HINTS = (
    "governance score",
    "governance health",
    "governance maturity",
    "governance gap",
    "governance gaps",
    "need attention",
    "needs attention",
    "which datasets",
    "overall score",
    "health dashboard",
    "kpi",
    "coverage",
    "recommend governance",
    "governance improvement",
    "improve governance",
)

_IMPACT_QUESTION_HINTS = (
    "impact",
    "downstream",
    "upstream",
    "depend",
    "what happens if",
    "what happens when",
    "which reports",
    "which datasets",
    "critical",
    "affected",
)

_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "is",
        "are",
        "was",
        "were",
        "what",
        "which",
        "who",
        "how",
        "why",
        "when",
        "where",
        "show",
        "list",
        "explain",
        "describe",
        "tell",
        "me",
        "about",
        "for",
        "of",
        "in",
        "on",
        "to",
        "and",
        "or",
        "with",
        "related",
        "available",
        "datasets",
        "dataset",
        "data",
        "all",
        "any",
        "can",
        "you",
        "do",
        "does",
        "my",
        "our",
        "this",
        "that",
        "these",
        "those",
        "from",
        "summarize",
        "summary",
        "governance",
        "assets",
        "asset",
        "terms",
        "term",
        "rules",
        "rule",
        "quality",
        "business",
        "lineage",
        "master",
        "stewardship",
        "glossary",
        "catalog",
        "contain",
        "contains",
        "information",
    }
)

_MAX_CATALOG = 8
_MAX_RULES = 10
_MAX_LINEAGE_NODES = 8
_MAX_LINEAGE_EDGES = 12
_MAX_STEWARDSHIP = 6
_MAX_MASTER = 6
_MAX_GLOSSARY_TERMS = 12
_MAX_ANNOTATIONS = 5


def _tokenize(question: str) -> list[str]:
    raw = re.findall(r"[a-zA-Z][a-zA-Z0-9_]*", (question or "").lower())
    tokens: list[str] = []
    for token in raw:
        if len(token) < 2 or token in _STOP_WORDS:
            continue
        if token not in tokens:
            tokens.append(token)
    return tokens


def _matches_text(text: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    hay = (text or "").lower()
    return any(token in hay for token in tokens)


def _catalog_to_dict(asset: CatalogAsset) -> dict[str, Any]:
    return {
        "id": asset.id,
        "asset_key": asset.asset_key,
        "name": asset.name,
        "asset_type": asset.asset_type,
        "domain": asset.domain,
        "owner_email": asset.owner_email,
        "description": asset.description,
        "tags": asset.tags,
        "pii_tier": asset.pii_tier,
        "lineage_node_key": asset.lineage_node_key,
        "schema_fields": asset.schema_fields,
        "sla_hours": asset.sla_hours,
        "contract_version": asset.contract_version,
    }


def _load_saved_glossary_map(
    db: Session, asset_ids: list[int], tokens: list[str]
) -> dict[tuple[int, str], GlossaryEntry]:
    if not asset_ids:
        return {}
    q = db.query(GlossaryEntry).filter(GlossaryEntry.catalog_asset_id.in_(asset_ids))
    if tokens:
        clauses = []
        for token in tokens:
            clauses.append(GlossaryEntry.field_name.ilike(f"%{token}%"))
            clauses.append(GlossaryEntry.title.ilike(f"%{token}%"))
            clauses.append(GlossaryEntry.definition.ilike(f"%{token}%"))
        q = q.filter(or_(*clauses))
    rows = q.order_by(GlossaryEntry.updated_at.desc()).limit(_MAX_GLOSSARY_TERMS * 2).all()
    result: dict[tuple[int, str], GlossaryEntry] = {}
    for row in rows:
        key = (row.catalog_asset_id, (row.field_name or "").lower())
        if key not in result:
            result[key] = row
    return result


def _glossary_entry_to_term(row: GlossaryEntry, asset: CatalogAsset | None = None) -> dict[str, Any]:
    return {
        "term": row.field_name or (asset.name if asset else ""),
        "title": row.title,
        "definition": row.definition,
        "usage": row.usage,
        "governance_notes": row.governance_notes,
        "status": row.status,
        "source_asset": asset.asset_key if asset else "",
        "domain": asset.domain if asset else "",
        "saved": True,
    }


def _extract_glossary_terms(
    db: Session,
    assets: list[CatalogAsset],
    tokens: list[str],
) -> list[dict[str, Any]]:
    terms: list[dict[str, Any]] = []
    seen: set[str] = set()
    asset_by_id = {a.id: a for a in assets}
    saved_map = _load_saved_glossary_map(db, [a.id for a in assets], tokens)

    for (asset_id, field_key), row in saved_map.items():
        asset = asset_by_id.get(asset_id)
        term_key = field_key or f"dataset:{asset_id}"
        if term_key in seen:
            continue
        seen.add(term_key)
        terms.append(_glossary_entry_to_term(row, asset))
        if len(terms) >= _MAX_GLOSSARY_TERMS:
            return terms

    for asset in assets:
        fields = [f.strip() for f in (asset.schema_fields or "").split(",") if f.strip()]
        for field in fields:
            key = field.lower()
            if key in seen:
                continue
            if tokens and not _matches_text(field, tokens) and not _matches_text(asset.name, tokens):
                continue
            saved = saved_map.get((asset.id, key))
            if saved:
                seen.add(key)
                terms.append(_glossary_entry_to_term(saved, asset))
                continue
            seen.add(key)
            terms.append(
                {
                    "term": field,
                    "title": field.replace("_", " ").title(),
                    "definition": (
                        f"Field on {asset.name} ({asset.asset_key}) in {asset.domain or 'general'} domain."
                    ),
                    "source_asset": asset.asset_key,
                    "domain": asset.domain,
                    "saved": False,
                }
            )
            if len(terms) >= _MAX_GLOSSARY_TERMS:
                return terms
    return terms


def _score_asset(asset: CatalogAsset, tokens: list[str]) -> int:
    if not tokens:
        return 0
    score = 0
    blob = " ".join(
        [
            asset.name or "",
            asset.asset_key or "",
            asset.domain or "",
            asset.description or "",
            asset.tags or "",
            asset.schema_fields or "",
        ]
    ).lower()
    for token in tokens:
        if token in blob:
            score += 2
    return score


def _page_context_tokens(page_context: dict[str, Any] | None) -> list[str]:
    if not page_context:
        return []
    extra: list[str] = []
    for key in (
        "asset_name",
        "asset_key",
        "node_key",
        "node_label",
        "rule_field",
        "rule_text",
        "task_name",
        "task_issue",
        "record_name",
        "record_email",
        "page",
    ):
        raw = str(page_context.get(key) or "").strip()
        if raw:
            extra.extend(_tokenize(raw))
    return extra


def _fetch_page_context_records(
    db: Session, page_context: dict[str, Any] | None
) -> dict[str, Any]:
    if not page_context:
        return {}
    snapshot: dict[str, Any] = {"page": page_context.get("page") or ""}
    asset_id = page_context.get("asset_id")
    if asset_id is not None and str(asset_id).strip().isdigit():
        row = db.query(CatalogAsset).filter(CatalogAsset.id == int(asset_id)).first()
        if row:
            snapshot["focused_catalog_asset"] = _catalog_to_dict(row)
    asset_key = str(page_context.get("asset_key") or "").strip()
    if not snapshot.get("focused_catalog_asset") and asset_key:
        row = db.query(CatalogAsset).filter(CatalogAsset.asset_key == asset_key).first()
        if row:
            snapshot["focused_catalog_asset"] = _catalog_to_dict(row)
    node_key = str(page_context.get("node_key") or "").strip()
    if node_key:
        node = db.query(LineageNode).filter(LineageNode.key == node_key).first()
        if node:
            snapshot["focused_lineage_node"] = {
                "key": node.key,
                "label": node.label,
                "node_type": node.node_type,
                "system": node.system,
                "layer": node.layer,
            }
    rule_id = page_context.get("rule_id")
    if rule_id is not None and str(rule_id).strip().isdigit():
        rule = db.query(Rule).filter(Rule.id == int(rule_id)).first()
        if rule:
            snapshot["focused_rule"] = {
                "id": rule.id,
                "field": rule.field,
                "rule": rule.rule,
                "status": rule.status,
            }
    stewardship_id = page_context.get("stewardship_id")
    if stewardship_id is not None and str(stewardship_id).strip().isdigit():
        task = db.query(StewardshipQueue).filter(StewardshipQueue.id == int(stewardship_id)).first()
        if task:
            snapshot["focused_stewardship_task"] = {
                "id": task.id,
                "name": task.name,
                "email": task.email,
                "issue": task.issue,
                "status": task.status,
                "owner_email": task.owner_email,
            }
    master_id = page_context.get("master_id")
    if master_id is not None and str(master_id).strip().isdigit():
        master = db.query(MasterData).filter(MasterData.id == int(master_id)).first()
        if master:
            snapshot["focused_master_record"] = {
                "id": master.id,
                "name": master.name,
                "email": master.email,
                "source_queue_id": master.source_queue_id,
            }
    return snapshot


def build_governance_context(
    db: Session,
    question: str,
    page_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a compact, question-relevant governance context payload."""
    tokens = _tokenize(question)
    tokens.extend(_page_context_tokens(page_context))
    tokens = list(dict.fromkeys(tokens))
    question_lower = (question or "").lower()
    page_snapshot = _fetch_page_context_records(db, page_context)

    catalog_query = db.query(CatalogAsset)
    if tokens:
        clauses = []
        for token in tokens:
            pattern = f"%{token}%"
            clauses.extend(
                [
                    CatalogAsset.name.ilike(pattern),
                    CatalogAsset.asset_key.ilike(pattern),
                    CatalogAsset.domain.ilike(pattern),
                    CatalogAsset.description.ilike(pattern),
                    CatalogAsset.tags.ilike(pattern),
                    CatalogAsset.schema_fields.ilike(pattern),
                ]
            )
        catalog_query = catalog_query.filter(or_(*clauses))
    catalog_assets = catalog_query.order_by(CatalogAsset.id.asc()).limit(_MAX_CATALOG * 3).all()
    focused_asset = (page_snapshot.get("focused_catalog_asset") or {}) if page_snapshot else {}
    if focused_asset.get("asset_key"):
        focused_row = db.query(CatalogAsset).filter(
            CatalogAsset.asset_key == focused_asset["asset_key"]
        ).first()
        if focused_row and focused_row not in catalog_assets:
            catalog_assets = [focused_row, *catalog_assets]
    if tokens:
        catalog_assets = sorted(catalog_assets, key=lambda a: _score_asset(a, tokens), reverse=True)
    catalog_assets = catalog_assets[:_MAX_CATALOG]

    include_rules = bool(tokens) or "rule" in question_lower or "quality" in question_lower
    rules: list[Rule] = []
    if include_rules:
        rules_query = db.query(Rule).filter(Rule.status == "active")
        if tokens:
            rule_clauses = []
            for token in tokens:
                pattern = f"%{token}%"
                rule_clauses.extend([Rule.field.ilike(pattern), Rule.rule.ilike(pattern)])
            rules_query = rules_query.filter(or_(*rule_clauses))
        rules = rules_query.order_by(Rule.id.asc()).limit(_MAX_RULES).all()

    lineage_node_keys = {a.lineage_node_key for a in catalog_assets if a.lineage_node_key}
    asset_keys = {a.asset_key for a in catalog_assets}

    lineage_nodes_query = db.query(LineageNode)
    if tokens or lineage_node_keys:
        node_clauses = []
        for token in tokens:
            pattern = f"%{token}%"
            node_clauses.extend(
                [
                    LineageNode.key.ilike(pattern),
                    LineageNode.label.ilike(pattern),
                    LineageNode.system.ilike(pattern),
                    LineageNode.layer.ilike(pattern),
                ]
            )
        if lineage_node_keys:
            node_clauses.append(LineageNode.key.in_(lineage_node_keys))
        lineage_nodes_query = lineage_nodes_query.filter(or_(*node_clauses))
    lineage_nodes = lineage_nodes_query.order_by(LineageNode.id.asc()).limit(_MAX_LINEAGE_NODES).all()

    node_keys = {n.key for n in lineage_nodes} | lineage_node_keys | asset_keys
    lineage_edges: list[LineageEdge] = []
    if node_keys:
        lineage_edges = (
            db.query(LineageEdge)
            .filter(
                or_(
                    LineageEdge.source_key.in_(node_keys),
                    LineageEdge.target_key.in_(node_keys),
                )
            )
            .order_by(LineageEdge.id.asc())
            .limit(_MAX_LINEAGE_EDGES)
            .all()
        )

    include_stewardship = bool(tokens) or "steward" in question_lower
    stewardship: list[StewardshipQueue] = []
    if include_stewardship:
        stewardship_query = db.query(StewardshipQueue)
        if tokens:
            stew_clauses = []
            for token in tokens:
                pattern = f"%{token}%"
                stew_clauses.extend(
                    [
                        StewardshipQueue.name.ilike(pattern),
                        StewardshipQueue.email.ilike(pattern),
                        StewardshipQueue.issue.ilike(pattern),
                        StewardshipQueue.owner_email.ilike(pattern),
                    ]
                )
            stewardship_query = stewardship_query.filter(or_(*stew_clauses))
        stewardship = (
            stewardship_query.order_by(StewardshipQueue.id.desc()).limit(_MAX_STEWARDSHIP).all()
        )

    include_master = bool(tokens) or any(
        w in question_lower for w in ("master", "golden", "customer master")
    )
    master_records: list[MasterData] = []
    if include_master:
        master_query = db.query(MasterData)
        if tokens:
            master_clauses = []
            for token in tokens:
                pattern = f"%{token}%"
                master_clauses.extend(
                    [MasterData.name.ilike(pattern), MasterData.email.ilike(pattern)]
                )
            master_query = master_query.filter(or_(*master_clauses))
        master_records = master_query.order_by(MasterData.id.desc()).limit(_MAX_MASTER).all()

    annotations: list[RecordAnnotation] = []
    if tokens and master_records:
        record_ids = [m.id for m in master_records]
        ann_clauses = [RecordAnnotation.record_id.in_(record_ids)]
        for token in tokens:
            ann_clauses.append(RecordAnnotation.comment.ilike(f"%{token}%"))
        annotations = (
            db.query(RecordAnnotation)
            .filter(or_(*ann_clauses))
            .order_by(RecordAnnotation.id.desc())
            .limit(_MAX_ANNOTATIONS)
            .all()
        )

    list_all_datasets = not tokens or any(
        w in question_lower for w in ("list", "available", "all datasets", "summarize governance")
    )
    if list_all_datasets and len(catalog_assets) < 3:
        catalog_assets = (
            db.query(CatalogAsset).order_by(CatalogAsset.id.asc()).limit(_MAX_CATALOG).all()
        )

    glossary_terms = _extract_glossary_terms(db, catalog_assets, tokens)
    if not glossary_terms and tokens:
        glossary_terms = _extract_glossary_terms(
            db,
            db.query(CatalogAsset).order_by(CatalogAsset.id.asc()).limit(_MAX_CATALOG).all(),
            tokens,
        )

    datasets = [_catalog_to_dict(a) for a in catalog_assets]
    sources: list[dict[str, str]] = []
    for asset in catalog_assets:
        sources.append({"type": "catalog", "id": asset.asset_key, "label": asset.name})
    for rule in rules:
        sources.append({"type": "rule", "id": str(rule.id), "label": f"{rule.field}: {rule.rule[:60]}"})
    for node in lineage_nodes:
        sources.append({"type": "lineage", "id": node.key, "label": node.label})

    classification_analysis = None
    if any(h in question_lower for h in _CLASSIFICATION_QUESTION_HINTS) or (
        (page_context or {}).get("page") == "catalog"
    ):
        from app.services.data_classification_service import (
            analyze_dataset,
            classify_field_name,
            find_datasets_with_classification,
        )

        pc = page_context or {}
        if any(w in question_lower for w in ("which datasets contain pii", "datasets contain pii", "contain pii")):
            classification_analysis = {
                "datasets_with_pii": find_datasets_with_classification(db, "PII"),
            }
        elif "sensitive" in question_lower and "field" in question_lower:
            classification_analysis = {
                "datasets_with_sensitive": find_datasets_with_classification(db, "Sensitive"),
            }
        elif pc.get("asset_id") and str(pc.get("asset_id")).strip().isdigit():
            classification_analysis = analyze_dataset(db, int(pc["asset_id"]))
        elif "customer_email" in question_lower or "explain classification" in question_lower:
            field_hint = "customer_email"
            for token in tokens:
                if "_" in token or token in ("email", "phone", "salary"):
                    field_hint = token
                    break
            classification_analysis = {
                "field": classify_field_name(
                    field_hint,
                    dataset_name=pc.get("asset_name", ""),
                    description=pc.get("description", ""),
                ),
                "datasets_with_pii": find_datasets_with_classification(db, "PII")
                if "masking" in question_lower or "pii" in question_lower
                else [],
            }

    glossary_analysis = None
    if any(h in question_lower for h in _GLOSSARY_QUESTION_HINTS) or "glossary" in question_lower:
        from app.services.glossary_generator_service import (
            generate_dataset_glossary,
            generate_field_glossary,
            get_saved_glossary_entries,
        )

        pc = page_context or {}
        field_hint = ""
        for token in tokens:
            if "_" in token or token.endswith("id") or token in ("email", "phone", "name"):
                field_hint = token
                break
        if not field_hint and "customer_email" in question_lower:
            field_hint = "customer_email"
        if not field_hint and "customer_id" in question_lower:
            field_hint = "customer_id"

        if pc.get("asset_id") and str(pc.get("asset_id")).strip().isdigit():
            asset_id = int(pc["asset_id"])
            if field_hint:
                saved = get_saved_glossary_entries(
                    db, catalog_asset_id=asset_id, field_name=field_hint, status="approved"
                )
                if saved:
                    row = saved[0]
                    glossary_analysis = {
                        "field": {
                            "field_name": row.field_name,
                            "title": row.title,
                            "definition": row.definition,
                            "usage": row.usage,
                            "governance_notes": row.governance_notes,
                            "source": "saved",
                        }
                    }
                else:
                    glossary_analysis = {
                        "field": generate_field_glossary(
                            field_name=field_hint,
                            dataset_name=pc.get("asset_name", ""),
                            description=pc.get("description", ""),
                        )
                    }
            elif any(w in question_lower for w in ("describe", "dataset", "what is")):
                glossary_analysis = {"dataset": generate_dataset_glossary(db, asset_id)}
        elif field_hint:
            glossary_analysis = {
                "field": generate_field_glossary(
                    field_name=field_hint,
                    dataset_name=pc.get("asset_name", ""),
                )
            }
        elif glossary_terms:
            glossary_analysis = {"matched_terms": glossary_terms[:5]}

    remediation_analysis = None
    if any(h in question_lower for h in _REMEDIATION_QUESTION_HINTS) or (
        "why" in question_lower and "fail" in question_lower
    ):
        from app.services.remediation_service import (
            explain_stewardship_failure,
            generate_remediation,
            get_latest_remediation,
            serialize_remediation,
        )

        pc = page_context or {}
        sid = pc.get("stewardship_id")
        if sid is not None and str(sid).strip().isdigit():
            stewardship_id = int(sid)
            saved = get_latest_remediation(db, stewardship_id)
            if saved and saved.status in ("accepted", "resolved", "pending"):
                remediation_analysis = serialize_remediation(saved)
            else:
                record = (
                    db.query(StewardshipQueue)
                    .filter(StewardshipQueue.id == stewardship_id)
                    .first()
                )
                if record:
                    remediation_analysis = generate_remediation(db, record)

    rule_recommendation_analysis = None
    if any(h in question_lower for h in _RULE_RECOMMENDATION_HINTS) or (
        "should apply" in question_lower and "rule" in question_lower
    ):
        from app.models import RuleRecommendation
        from app.services.rule_recommendation_service import (
            recommend_rules_for_dataset,
            recommend_rules_for_field,
        )

        pc = page_context or {}
        field_hint = ""
        for token in tokens:
            if "_" in token or token in ("email", "phone", "customer_id"):
                field_hint = token
                break
        if "customer_email" in question_lower:
            field_hint = "customer_email"

        asset_id_raw = pc.get("asset_id")
        if asset_id_raw is not None and str(asset_id_raw).strip().isdigit():
            asset_id = int(asset_id_raw)
            pending = (
                db.query(RuleRecommendation)
                .filter(
                    RuleRecommendation.catalog_asset_id == asset_id,
                    RuleRecommendation.status == "pending",
                )
                .order_by(RuleRecommendation.confidence.desc())
                .limit(10)
                .all()
            )
            if field_hint:
                rule_recommendation_analysis = recommend_rules_for_field(
                    db,
                    field_name=field_hint,
                    dataset_id=asset_id,
                )
            else:
                rule_recommendation_analysis = recommend_rules_for_dataset(db, asset_id)
            if pending:
                rule_recommendation_analysis = rule_recommendation_analysis or {}
                rule_recommendation_analysis["pending_recommendations"] = [
                    {
                        "field_name": p.field_name,
                        "rule_text": p.rule_text,
                        "confidence": p.confidence,
                        "status": p.status,
                    }
                    for p in pending
                ]
        elif field_hint:
            rule_recommendation_analysis = recommend_rules_for_field(
                db,
                field_name=field_hint,
                dataset_name=pc.get("asset_name", ""),
            )

    documentation_analysis = None
    if any(h in question_lower for h in _DOCUMENTATION_QUESTION_HINTS) or (
        "purpose" in question_lower and "dataset" in question_lower
    ):
        from app.services.dataset_documentation_service import (
            generate_dataset_documentation,
            get_saved_documentation,
            serialize_documentation_row,
        )

        pc = page_context or {}
        asset_id_raw = pc.get("asset_id")
        if asset_id_raw is not None and str(asset_id_raw).strip().isdigit():
            asset_id = int(asset_id_raw)
            saved = get_saved_documentation(db, asset_id)
            if saved and saved.status == "approved":
                documentation_analysis = serialize_documentation_row(saved)
            else:
                documentation_analysis = generate_dataset_documentation(db, asset_id)

    governance_score_analysis = None
    if any(h in question_lower for h in _GOVERNANCE_SCORE_HINTS) or (
        (page_context or {}).get("page") in ("governance", "dashboard")
    ):
        from app.services.governance_score_service import (
            build_governance_dashboard,
            get_dataset_governance_score,
        )

        pc = page_context or {}
        asset_id_raw = pc.get("asset_id")
        if asset_id_raw is not None and str(asset_id_raw).strip().isdigit():
            governance_score_analysis = get_dataset_governance_score(db, int(asset_id_raw))
        else:
            governance_score_analysis = build_governance_dashboard(db)

    lineage_impact_analysis = None
    if any(h in question_lower for h in _IMPACT_QUESTION_HINTS) or (
        (page_context or {}).get("page") == "lineage"
    ):
        from app.services.lineage_impact_service import (
            analyze_asset_impact_by_id,
            analyze_comprehensive_by_field,
            analyze_comprehensive_by_node,
            resolve_impact_from_question,
        )

        pc = page_context or {}
        asset_id = pc.get("asset_id")
        node_key = (pc.get("node_key") or "").strip()
        field_name = (pc.get("impact_field") or pc.get("field") or "").strip()
        if asset_id is not None and str(asset_id).strip().isdigit():
            lineage_impact_analysis = analyze_asset_impact_by_id(db, int(asset_id))
        elif node_key:
            lineage_impact_analysis = analyze_comprehensive_by_node(db, node_key)
        elif field_name:
            lineage_impact_analysis = analyze_comprehensive_by_field(db, field_name)
        else:
            lineage_impact_analysis = resolve_impact_from_question(db, question)
        if lineage_impact_analysis:
            for asset in (lineage_impact_analysis.get("critical_dependencies") or [])[:5]:
                sources.append(
                    {
                        "type": "lineage_impact",
                        "id": asset.get("asset_key") or asset.get("lineage_node_key", ""),
                        "label": asset.get("name", "critical dependency"),
                    }
                )

    return {
        "question_tokens": tokens,
        "page_context": page_snapshot,
        "summary_counts": {
            "datasets": len(datasets),
            "glossary_terms": len(glossary_terms),
            "rules": len(rules),
            "lineage_nodes": len(lineage_nodes),
            "lineage_edges": len(lineage_edges),
            "stewardship_tasks": len(stewardship),
            "master_records": len(master_records),
            "quarantine_rows": db.query(QuarantineData).count() if "quarantine" in question_lower else None,
        },
        "datasets": datasets,
        "glossary_terms": glossary_terms,
        "rules": [
            {
                "id": r.id,
                "field": r.field,
                "rule": r.rule,
                "status": r.status,
                "created_by": r.created_by,
            }
            for r in rules
        ],
        "lineage": {
            "nodes": [
                {
                    "key": n.key,
                    "label": n.label,
                    "node_type": n.node_type,
                    "system": n.system,
                    "layer": n.layer,
                }
                for n in lineage_nodes
            ],
            "edges": [
                {
                    "source_key": e.source_key,
                    "target_key": e.target_key,
                    "transformation": e.transformation,
                    "criticality": e.criticality,
                }
                for e in lineage_edges
            ],
        },
        "stewardship": [
            {
                "id": s.id,
                "name": s.name,
                "email": s.email,
                "issue": s.issue,
                "status": s.status,
                "owner_email": s.owner_email,
            }
            for s in stewardship
        ],
        "master_data": [
            {
                "id": m.id,
                "name": m.name,
                "email": m.email,
                "source_queue_id": m.source_queue_id,
            }
            for m in master_records
        ],
        "annotations": [
            {
                "id": a.id,
                "record_id": a.record_id,
                "comment": a.comment,
                "status": a.status,
            }
            for a in annotations
        ],
        "glossary_analysis": glossary_analysis,
        "documentation_analysis": documentation_analysis,
        "remediation_analysis": remediation_analysis,
        "rule_recommendation_analysis": rule_recommendation_analysis,
        "classification_analysis": classification_analysis,
        "governance_score_analysis": governance_score_analysis,
        "lineage_impact_analysis": (
            {
                k: lineage_impact_analysis.get(k)
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
                    "summary",
                )
            }
            if lineage_impact_analysis
            else None
        ),
        "sources": sources,
    }
