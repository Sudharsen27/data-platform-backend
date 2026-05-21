from app.database import SessionLocal
from app.models import CatalogAsset, LineageEdge, LineageNode
from app.services.lineage_impact import analyze_field_impact, analyze_node_impact


def _seed_lineage(db):
    if db.query(LineageNode).count() > 0:
        return
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
                node_type="table",
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
                transformation="standardize_email, trim_name",
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
    db.add(
        CatalogAsset(
            asset_key="mdm.customer_master",
            name="Customer Master",
            lineage_node_key="mdm.customer_master",
        )
    )
    db.commit()


def test_node_impact_includes_downstream_chain():
    db = SessionLocal()
    try:
        _seed_lineage(db)
        result = analyze_node_impact(db, "crm.customers_raw")
        assert result is not None
        keys = set(result["affected_node_keys"])
        assert "crm.customers_raw" in keys
        assert "staging.customers_clean" in keys
        assert "mdm.customer_master" in keys
        assert "bi.customer_360" in keys
        assert len(result["catalog_assets"]) >= 1
    finally:
        db.close()


def test_field_impact_finds_email_transformations():
    db = SessionLocal()
    try:
        _seed_lineage(db)
        result = analyze_field_impact(db, "email")
        assert result is not None
        assert result["field"] == "email"
        assert len(result["affected_node_keys"]) >= 2
        assert "email" in result["summary"].lower()
    finally:
        db.close()


def test_lineage_impact_api(client):
    db = SessionLocal()
    try:
        _seed_lineage(db)
    finally:
        db.close()

    login = client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "AdminPass123!"},
    )
    if login.status_code != 200:
        register = client.post(
            "/auth/register",
            json={
                "email": "admin@example.com",
                "password": "AdminPass123!",
                "full_name": "Admin",
                "company_name": "Test",
            },
        )
        assert register.status_code == 200
        login = client.post(
            "/auth/login",
            json={"email": "admin@example.com", "password": "AdminPass123!"},
        )
    assert login.status_code == 200
    token = login.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    by_node = client.get(
        "/lineage/impact?node_key=mdm.customer_master",
        headers=headers,
    )
    assert by_node.status_code == 200
    body = by_node.json()
    assert "bi.customer_360" in body["affected_node_keys"]
    assert body["summary"]

    by_field = client.get("/lineage/impact?field=email", headers=headers)
    assert by_field.status_code == 200
    assert by_field.json()["field"] == "email"
