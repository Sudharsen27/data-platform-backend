"""Downstream lineage impact analysis (node- or field-scoped)."""

from __future__ import annotations

from collections import defaultdict, deque

from sqlalchemy.orm import Session

from app.models import CatalogAsset, LineageEdge, LineageNode


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


def _edges_for_keys(keys: set[str], edges: list[LineageEdge]) -> list[LineageEdge]:
    return [
        e
        for e in edges
        if e.source_key in keys and e.target_key in keys
    ]


def _catalog_for_keys(db: Session, keys: set[str]) -> list[CatalogAsset]:
    if not keys:
        return []
    return (
        db.query(CatalogAsset)
        .filter(CatalogAsset.lineage_node_key.in_(list(keys)))
        .order_by(CatalogAsset.name.asc())
        .all()
    )


def analyze_node_impact(db: Session, node_key: str) -> dict:
    key = node_key.strip()
    node = db.query(LineageNode).filter(LineageNode.key == key).first()
    if not node:
        return None

    edges = db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()
    affected_keys = _downstream_keys(key, edges)
    nodes = (
        db.query(LineageNode)
        .filter(LineageNode.key.in_(list(affected_keys)))
        .order_by(LineageNode.id.asc())
        .all()
    )
    subgraph_edges = _edges_for_keys(affected_keys, edges)
    catalog = _catalog_for_keys(db, affected_keys)

    return {
        "anchor_node_key": key,
        "field": None,
        "affected_node_keys": sorted(affected_keys),
        "nodes": nodes,
        "edges": subgraph_edges,
        "catalog_assets": catalog,
        "summary": (
            f"Changing {node.label} ({key}) may impact "
            f"{len(affected_keys) - 1} downstream node(s) and {len(catalog)} catalog asset(s)."
        ),
    }


def analyze_field_impact(db: Session, field: str) -> dict:
    token = field.strip().lower()
    if not token:
        return None

    edges = db.query(LineageEdge).order_by(LineageEdge.id.asc()).all()
    matching_edges = [
        e for e in edges if token in (e.transformation or "").lower()
    ]
    if not matching_edges:
        return {
            "anchor_node_key": None,
            "field": token,
            "affected_node_keys": [],
            "nodes": [],
            "edges": [],
            "catalog_assets": [],
            "summary": f"No lineage transformations reference field '{token}'.",
        }

    affected_keys: set[str] = set()
    for edge in matching_edges:
        affected_keys |= _downstream_keys(edge.source_key, edges)
        affected_keys.add(edge.source_key)
        affected_keys.add(edge.target_key)

    nodes = (
        db.query(LineageNode)
        .filter(LineageNode.key.in_(list(affected_keys)))
        .order_by(LineageNode.id.asc())
        .all()
    )
    subgraph_edges = _edges_for_keys(affected_keys, edges)
    catalog = _catalog_for_keys(db, affected_keys)

    return {
        "anchor_node_key": matching_edges[0].source_key,
        "field": token,
        "affected_node_keys": sorted(affected_keys),
        "nodes": nodes,
        "edges": subgraph_edges,
        "catalog_assets": catalog,
        "summary": (
            f"Field '{token}' appears in {len(matching_edges)} transformation(s); "
            f"{len(affected_keys)} node(s) and {len(catalog)} catalog asset(s) may be affected."
        ),
    }
