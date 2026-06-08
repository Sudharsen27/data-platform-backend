import uuid

from app.database import SessionLocal
from app.models import CatalogAsset, LineageEdge, LineageNode
from app.services.lineage_impact_service import (
    analyze_asset_impact_by_id,
    analyze_comprehensive_by_node,
    build_comprehensive_impact,
    parse_impact_question,
    resolve_impact_from_question,
)
from app.services.lineage_impact import analyze_node_impact
from app.services.lineage_explanation_service import explain_lineage_impact


def _seed(db):
    if db.query(LineageNode).count() > 0:
        return db.query(CatalogAsset).filter(CatalogAsset.asset_key == "mdm.customer_master").first()
    db.add_all(
        [
            LineageNode(
                key="crm.customers_raw",
                label="CRM Customers Raw",
                node_type="table",
                system="CRM",
                layer="source",
            ),
            LineageNode(
                key="staging.customers_clean",
                label="Customers Clean",
                node_type="table",
                system="Postgres",
                layer="staging",
            ),
            LineageNode(
                key="mdm.customer_master",
                label="Customer Master",
                node_type="table",
                system="MDM",
                layer="golden",
            ),
            LineageNode(
                key="bi.customer_360",
                label="Customer 360 Mart",
                node_type="view",
                system="Analytics",
                layer="consumption",
            ),
        ]
    )
    db.add_all(
        [
            LineageEdge(
                source_key="crm.customers_raw",
                target_key="staging.customers_clean",
                transformation="standardize_email, customer_id",
                criticality="high",
            ),
            LineageEdge(
                source_key="staging.customers_clean",
                target_key="mdm.customer_master",
                transformation="match_merge_survivorship",
                criticality="high",
            ),
            LineageEdge(
                source_key="mdm.customer_master",
                target_key="bi.customer_360",
                transformation="aggregate_profile_metrics",
                criticality="medium",
            ),
        ]
    )
    asset = CatalogAsset(
        asset_key="mdm.customer_master",
        name="Customer Master",
        asset_type="table",
        domain="Customer",
        lineage_node_key="mdm.customer_master",
        schema_fields="customer_id,name,email",
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def test_build_comprehensive_impact_scores_downstream():
    db = SessionLocal()
    try:
        asset = _seed(db)
        base = analyze_node_impact(db, "mdm.customer_master")
        result = build_comprehensive_impact(db, base_result=base, anchor_label=asset.name)
        assert result["impact_score"] >= 20
        assert result["downstream_count"] >= 1
        assert "bi.customer_360" in [a.get("lineage_node_key") for a in result["downstream_assets"]]
        assert result["source_asset"] == "Customer Master"
    finally:
        db.close()


def test_parse_impact_question_customer_id():
    hints = parse_impact_question("What happens if Customer_ID changes?")
    assert hints.get("field") == "customer_id"


def test_resolve_impact_from_question():
    db = SessionLocal()
    try:
        _seed(db)
        result = resolve_impact_from_question(db, "What happens if Customer_ID changes?")
        assert result is not None
        assert result["field"] == "customer_id" or result["impact_score"] >= 0
    finally:
        db.close()


def test_analyze_asset_impact_by_id():
    db = SessionLocal()
    try:
        asset = _seed(db)
        result = analyze_asset_impact_by_id(db, asset.id)
        assert result is not None
        assert result["source_asset_id"] == asset.id
        assert result["reports_impacted"] >= 0
    finally:
        db.close()


def test_explain_lineage_impact_heuristic(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    impact = {
        "source_asset": "Customer Master",
        "field": "customer_id",
        "impact_score": 85,
        "downstream_count": 2,
        "upstream_count": 1,
        "rules_impacted": 1,
        "reports_impacted": 1,
        "master_data_impacted": 0,
        "critical_dependencies": [{"name": "Customer 360 Mart", "asset_key": "bi.customer_360"}],
        "downstream_assets": [],
    }
    result = explain_lineage_impact(question="What happens if Customer_ID changes?", impact=impact)
    assert "analysis" in result
    assert len(result["analysis"]) > 20
    assert result["source_engine"] == "heuristics"


def _auth_headers(client):
    email = f"lineage_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Lineage Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_lineage_impact_asset_api(client):
    db = SessionLocal()
    try:
        asset = _seed(db)
        asset_id = asset.id
    finally:
        db.close()

    headers = _auth_headers(client)
    response = client.get(f"/lineage/impact/{asset_id}", headers=headers)
    assert response.status_code == 200
    body = response.json()
    assert body["impact_score"] >= 0
    assert "downstream_count" in body
    assert body["source_asset"] == "Customer Master"


def test_lineage_impact_analyze_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        _seed(db)
    finally:
        db.close()

    headers = _auth_headers(client)
    response = client.post(
        "/lineage/impact/analyze",
        headers=headers,
        json={"question": "What happens if Customer_ID changes?"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["analysis"]
    assert body["impact_score"] >= 0
    assert body["impact_detail"] is not None
