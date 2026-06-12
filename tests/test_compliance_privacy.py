import uuid

from app.database import SessionLocal
from app.models import CatalogAsset
from app.services.compliance_privacy_service import (
    build_compliance_dashboard,
    compute_compliance_score,
)


def _seed_pii_asset(db, **kwargs):
    row = CatalogAsset(
        asset_key=kwargs.get("asset_key", f"compliance.{uuid.uuid4().hex[:8]}"),
        name=kwargs.get("name", "Customer PII"),
        domain=kwargs.get("domain", "Customer"),
        owner_email=kwargs.get("owner_email", "owner@example.com"),
        description=kwargs.get("description", "Customer personal data"),
        schema_fields=kwargs.get(
            "schema_fields", "customer_id,customer_email,customer_name,phone"
        ),
        pii_tier=kwargs.get("pii_tier", "confidential"),
        lineage_node_key=kwargs.get("lineage_node_key", "crm.customers"),
        tags=kwargs.get("tags", "pii,customer"),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_compliance_score_summary():
    db = SessionLocal()
    try:
        _seed_pii_asset(db)
        result = compute_compliance_score(db)
        assert result["scope"] == "platform"
        assert "compliance_score" in result
        assert result["pii_asset_count"] >= 1
        assert result["sensitive_asset_count"] >= 1
        assert "datasets_missing_classification" in result
        assert "datasets_missing_documentation" in result
    finally:
        db.close()


def test_compliance_dashboard_payload():
    db = SessionLocal()
    try:
        _seed_pii_asset(db, name="Undocumented PII", schema_fields="email,ssn,address")
        result = build_compliance_dashboard(db)
        assert result["scope"] == "platform"
        assert result["compliance_score"] >= 0
        assert result["asset_distribution"]
        assert result["coverage_chart"]
        assert isinstance(result["governance_risks"], list)
        assert isinstance(result["compliance_recommendations"], list)
        assert result["governance_overall_score"] >= 0
    finally:
        db.close()


def _auth_headers(client):
    email = f"compliance_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Compliance Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_compliance_api_endpoints(client):
    db = SessionLocal()
    try:
        _seed_pii_asset(db, name="API Compliance Dataset")
        db.commit()
    finally:
        db.close()

    headers = _auth_headers(client)
    score = client.get("/compliance/score", headers=headers)
    assert score.status_code == 200
    score_body = score.json()
    assert score_body["scope"] == "platform"
    assert "compliance_score" in score_body
    assert "pii_asset_count" in score_body

    dashboard = client.get("/compliance/dashboard", headers=headers)
    assert dashboard.status_code == 200
    dash_body = dashboard.json()
    assert "governance_risks" in dash_body
    assert "compliance_recommendations" in dash_body
    assert "missing_classification_datasets" in dash_body
    assert "pii_assets" in dash_body
