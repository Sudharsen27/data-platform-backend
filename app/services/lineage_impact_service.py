"""Comprehensive lineage impact analysis — extends lineage_impact graph traversal."""

from __future__ import annotations

import re
from collections import defaultdict, deque
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.models import CatalogAsset, LineageEdge, LineageNode, MasterData, Rule
from app.services.lineage_impact import analyze_field_impact, analyze_node_impact


def _upstream_keys(start_key: str, edges: list[LineageEdge]) -> set[str]:
    reverse: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        reverse[edge.target_key].append(edge.source_key)

    seen: set[str] = {start_key}
    queue: deque[str] = deque([start_key])
    while queue:
        current = queue.popleft()
        for source in reverse.get(current, []):
            if source in seen:
                continue
            seen.add(source)
            queue.append(source)
    return seen


def _downstream_keys(start_key: str, edges: list[LineageEdge]) -> set[str]:
    adj: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        adj[edge.source_key].append(edge.target_key)

    seen: set[str] = {start_key}
    queue: deque[str] = deque([start_key])
    while queue:
        current = queue.popleft()
        for target in adj.get(current, []):
            if target in seen:
                continue
            seen.add(target)
            queue.append(target)
    return seen


def _serialize_asset(asset: CatalogAsset) -> dict[str, Any]:
    return {
        "id": asset.id,
        "asset_key": asset.asset_key,
        "name": asset.name,
        "asset_type": asset.asset_type,
        "domain": asset.domain,
        "lineage_node_key": asset.lineage_node_key,
        "layer": "",
        "system": "",
        "impact_level": "medium",
    }


def _serialize_node(node: LineageNode, *, impact_level: str = "medium") -> dict[str, Any]:
    return {
        "key": node.key,
        "label": node.label,
        "node_type": node.node_type,
        "system": node.system,
        "layer": node.layer,
        "impact_level": impact_level,
    }


def _impact_level_for_node(node: LineageNode, *, is_critical_edge: bool) -> str:
    if is_critical_edge or node.layer in ("golden", "consumption"):
        return "high"
    if node.layer == "staging":
        return "medium"
    return "low"


def _compute_impact_score(
    *,
    downstream_count: int,
    upstream_count: int,
    critical_count: int,
    reports_count: int,
    rules_count: int,
    master_count: int,
) -> int:
    score = 20
    score += min(30, downstream_count * 6)
    score += min(20, critical_count * 10)
    score += min(15, reports_count * 8)
    score += min(10, rules_count * 4)
    score += min(5, master_count * 3)
    if upstream_count > 0:
        score += min(5, upstream_count * 2)
    return min(100, max(0, score))


def _rules_for_field(db: Session, field_token: str) -> list[Rule]:
    if not field_token:
        return []
    pattern = f"%{field_token}%"
    return (
        db.query(Rule)
        .filter(
            Rule.status == "active",
            or_(Rule.field.ilike(pattern), Rule.rule.ilike(pattern)),
        )
        .order_by(Rule.id.asc())
        .all()
    )


def _rules_for_assets(db: Session, assets: list[CatalogAsset], field_token: str = "") -> list[Rule]:
    fields: set[str] = set()
    if field_token:
        fields.add(field_token.lower())
    for asset in assets:
        for part in (asset.schema_fields or "").split(","):
            token = part.strip().lower()
            if token:
                fields.add(token)

    if not fields:
        return _rules_for_field(db, field_token)

    clauses = []
    for token in fields:
        pattern = f"%{token}%"
        clauses.extend([Rule.field.ilike(pattern), Rule.rule.ilike(pattern)])
    return (
        db.query(Rule)
        .filter(Rule.status == "active", or_(*clauses))
        .order_by(Rule.id.asc())
        .limit(20)
        .all()
    )


def _master_data_for_nodes(db: Session, node_keys: set[str]) -> list[MasterData]:
    if "mdm.customer_master" in node_keys or any("master" in k for k in node_keys):
        return db.query(MasterData).order_by(MasterData.id.desc()).limit(10).all()
    return []


def build_comprehensive_impact(
    db: Session,
    *,
    base_result: dict,
    anchor_label: str = "",
) -> dict[str, Any]:
    """Enrich a lineage_impact result with upstream/downstream, score, and governance counts."""
    edges = db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()
    anchor_key = base_result.get("anchor_node_key") or ""
    field = (base_result.get("field") or "").strip().lower()

    affected_keys = set(base_result.get("affected_node_keys") or [])
    if not affected_keys and anchor_key:
        affected_keys = _downstream_keys(anchor_key, edges)

    upstream_keys: set[str] = set()
    downstream_keys: set[str] = set()
    if anchor_key:
        upstream_keys = _upstream_keys(anchor_key, edges) - {anchor_key}
        downstream_keys = _downstream_keys(anchor_key, edges) - {anchor_key}
    elif affected_keys:
        downstream_keys = set(affected_keys)

    node_key_set = set(affected_keys) | set(upstream_keys)
    if anchor_key:
        node_key_set.add(anchor_key)
    if node_key_set:
        all_nodes = (
            db.query(LineageNode)
            .filter(LineageNode.key.in_(list(node_key_set)))
            .order_by(LineageNode.id.asc())
            .all()
        )
    else:
        all_nodes = []
    node_by_key = {n.key: n for n in all_nodes}

    critical_edge_keys: set[str] = set()
    for edge in edges:
        if edge.criticality == "high" and (
            edge.source_key in affected_keys
            or edge.target_key in affected_keys
            or edge.source_key == anchor_key
            or edge.target_key == anchor_key
        ):
            critical_edge_keys.add(edge.target_key)
            critical_edge_keys.add(edge.source_key)

    catalog_assets = base_result.get("catalog_assets") or []
    if not catalog_assets and affected_keys:
        catalog_assets = (
            db.query(CatalogAsset)
            .filter(CatalogAsset.lineage_node_key.in_(list(affected_keys)))
            .order_by(CatalogAsset.name.asc())
            .all()
        )

    downstream_assets: list[dict] = []
    upstream_assets: list[dict] = []
    critical_assets: list[dict] = []
    reports: list[dict] = []

    for asset in catalog_assets:
        serialized = _serialize_asset(asset)
        node = node_by_key.get(asset.lineage_node_key)
        if node:
            serialized["layer"] = node.layer
            serialized["system"] = node.system
            level = _impact_level_for_node(
                node, is_critical_edge=asset.lineage_node_key in critical_edge_keys
            )
            serialized["impact_level"] = level
        if asset.lineage_node_key in downstream_keys or (
            asset.lineage_node_key in affected_keys and asset.lineage_node_key != anchor_key
        ):
            downstream_assets.append(serialized)
        if asset.lineage_node_key in upstream_keys:
            upstream_assets.append(serialized)
        if serialized["impact_level"] == "high":
            critical_assets.append(serialized)
        if asset.asset_type == "view" or (node and node.layer == "consumption"):
            reports.append(serialized)

    for key in downstream_keys:
        node = node_by_key.get(key)
        if node and not any(a.get("lineage_node_key") == key for a in downstream_assets):
            downstream_assets.append(
                {
                    "id": None,
                    "asset_key": key,
                    "name": node.label,
                    "asset_type": node.node_type,
                    "domain": "",
                    "lineage_node_key": key,
                    "layer": node.layer,
                    "system": node.system,
                    "impact_level": _impact_level_for_node(
                        node, is_critical_edge=key in critical_edge_keys
                    ),
                }
            )

    rules = _rules_for_assets(db, catalog_assets, field)
    master_records = _master_data_for_nodes(db, affected_keys | downstream_keys)

    impact_score = _compute_impact_score(
        downstream_count=len(downstream_keys) or max(0, len(affected_keys) - 1),
        upstream_count=len(upstream_keys),
        critical_count=len(critical_assets),
        reports_count=len(reports),
        rules_count=len(rules),
        master_count=len(master_records),
    )

    source_name = anchor_label or anchor_key or field or "Unknown"
    if not anchor_label and anchor_key:
        node = node_by_key.get(anchor_key)
        if node:
            source_name = node.label

    return {
        "source_asset": source_name,
        "anchor_node_key": anchor_key or None,
        "field": field or None,
        "impact_score": impact_score,
        "downstream_count": len(downstream_keys) or max(0, len(affected_keys) - (1 if anchor_key else 0)),
        "upstream_count": len(upstream_keys),
        "datasets_impacted": len(downstream_assets) + len(upstream_assets),
        "rules_impacted": len(rules),
        "reports_impacted": len(reports),
        "master_data_impacted": len(master_records),
        "downstream_assets": downstream_assets,
        "upstream_assets": upstream_assets,
        "critical_assets": critical_assets,
        "critical_dependencies": critical_assets,
        "impacted_rules": [
            {"id": r.id, "field": r.field, "rule": r.rule, "status": r.status}
            for r in rules
        ],
        "impacted_reports": reports,
        "impacted_master_data": [
            {
                "id": m.id,
                "name": m.name,
                "email": m.email,
                "source_queue_id": m.source_queue_id,
            }
            for m in master_records
        ],
        "summary": base_result.get("summary") or "",
        "nodes": base_result.get("nodes") or [],
        "edges": base_result.get("edges") or [],
        "affected_node_keys": base_result.get("affected_node_keys") or [],
    }


def analyze_asset_impact_by_id(db: Session, asset_id: int) -> dict[str, Any] | None:
    asset = db.query(CatalogAsset).filter(CatalogAsset.id == asset_id).first()
    if not asset:
        return None
    lineage_key = (asset.lineage_node_key or asset.asset_key or "").strip()
    if not lineage_key:
        return None
    base = analyze_node_impact(db, lineage_key)
    if base is None:
        return None
    result = build_comprehensive_impact(db, base_result=base, anchor_label=asset.name)
    result["source_asset_id"] = asset.id
    result["source_asset_key"] = asset.asset_key
    return result


def analyze_comprehensive_by_node(db: Session, node_key: str) -> dict[str, Any] | None:
    base = analyze_node_impact(db, node_key)
    if base is None:
        return None
    node = db.query(LineageNode).filter(LineageNode.key == node_key.strip()).first()
    label = node.label if node else node_key
    return build_comprehensive_impact(db, base_result=base, anchor_label=label)


def analyze_comprehensive_by_field(db: Session, field: str) -> dict[str, Any] | None:
    base = analyze_field_impact(db, field)
    if base is None:
        return None
    return build_comprehensive_impact(db, base_result=base, anchor_label=field)


_FIELD_PATTERN = re.compile(r"\b([a-z][a-z0-9_]*(?:_id|_key)?)\b", re.I)
_ASSET_HINTS = (
    "customer master",
    "customer_master",
    "customer 360",
    "customer_master",
    "mdm.customer_master",
)


def parse_impact_question(question: str) -> dict[str, str]:
    """Extract field or asset hints from a natural-language impact question."""
    q = (question or "").strip()
    q_lower = q.lower()
    result: dict[str, str] = {}

    for hint in _ASSET_HINTS:
        if hint.replace("_", " ") in q_lower or hint in q_lower:
            if "customer master" in q_lower or "customer_master" in q_lower:
                result["node_key"] = "mdm.customer_master"
                result["asset_name"] = "Customer Master"
            elif "customer 360" in q_lower:
                result["node_key"] = "bi.customer_360"
                result["asset_name"] = "Customer 360 Mart"
            break

    field_match = re.search(
        r"(?:if|when|changing?|change to)\s+([A-Za-z][A-Za-z0-9_]*)\s+(?:changes?|is changed|changes)",
        q,
        re.I,
    )
    if field_match:
        result["field"] = field_match.group(1).lower()
    else:
        id_match = re.search(r"\b([A-Za-z][A-Za-z0-9_]*_id)\b", q)
        if id_match:
            result["field"] = id_match.group(1).lower()

    if "customer_id" in q_lower.replace("-", "_"):
        result["field"] = "customer_id"

    return result


def resolve_impact_from_question(db: Session, question: str) -> dict[str, Any] | None:
    """Resolve comprehensive impact analysis from a natural-language question."""
    hints = parse_impact_question(question)
    if hints.get("node_key"):
        return analyze_comprehensive_by_node(db, hints["node_key"])
    if hints.get("field"):
        return analyze_comprehensive_by_field(db, hints["field"])

    q_lower = question.lower()
    if "customer" in q_lower and "master" in q_lower:
        return analyze_comprehensive_by_node(db, "mdm.customer_master")

    tokens = [t for t in _FIELD_PATTERN.findall(q_lower) if len(t) > 2]
    for token in tokens:
        if token.endswith("_id") or token in ("email", "phone", "name"):
            return analyze_comprehensive_by_field(db, token)

    node = (
        db.query(LineageNode)
        .filter(
            or_(
                LineageNode.label.ilike(f"%{question[:40]}%"),
                LineageNode.key.ilike(f"%{question[:40]}%"),
            )
        )
        .first()
    )
    if node:
        return analyze_comprehensive_by_node(db, node.key)
    return None
