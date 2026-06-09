import uuid

from app.database import SessionLocal
from app.models import CatalogAsset, GlossaryEntry, Rule, StewardshipQueue
from app.services.governance_score_service import (
    compute_dataset_score,
    compute_platform_score,
    get_dataset_governance_score,
)


def _seed_asset(db, **kwargs):
    row = CatalogAsset(
        asset_key=kwargs.get("asset_key", f"test.{uuid.uuid4().hex[:8]}"),
        name=kwargs.get("name", "Customer Master"),
        domain=kwargs.get("domain", "Customer"),
        owner_email=kwargs.get("owner_email", "owner@example.com"),
        description=kwargs.get("description", "Golden customer records"),
        schema_fields=kwargs.get("schema_fields", "customer_id,customer_email,customer_name"),
        pii_tier=kwargs.get("pii_tier", "confidential"),
        lineage_node_key=kwargs.get("lineage_node_key", "crm.customers"),
        tags=kwargs.get("tags", "master,customer"),
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_dataset_score_with_metadata():
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        db.add(
            GlossaryEntry(
                catalog_asset_id=asset.id,
                field_name="customer_email",
                title="Customer Email",
                definition="Primary email",
                status="approved",
                created_by="test",
                updated_by="test",
            )
        )
        db.add(Rule(field="customer_email", rule="Must be valid email", status="active"))
        db.commit()

        result = compute_dataset_score(db, asset)
        assert result["overall_score"] >= 50
        assert result["dataset_name"] == "Customer Master"
        assert "metadata_completeness" in result["dimensions"]
        assert result["risk_score"] == 100 - result["overall_score"]
    finally:
        db.close()


def test_platform_score_empty_catalog():
    db = SessionLocal()
    try:
        result = compute_platform_score(db)
        assert result["scope"] == "platform"
        assert "overall_score" in result
        assert "dimensions" in result
    finally:
        db.close()


def _auth_headers(client):
    email = f"govscore_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Gov Score Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_governance_score_api(client):
    db = SessionLocal()
    try:
        asset = _seed_asset(db, name="API Test Dataset")
        asset_id = asset.id
        db.add(StewardshipQueue(name="A", email="a@x.com", issue="test", status="approved"))
        db.commit()
    finally:
        db.close()

    headers = _auth_headers(client)
    platform = client.get("/governance/score", headers=headers)
    assert platform.status_code == 200
    body = platform.json()
    assert body["scope"] == "platform"
    assert "overall_score" in body
    assert "dimension_details" in body

    dataset = client.get(f"/governance/score/{asset_id}", headers=headers)
    assert dataset.status_code == 200
    ds_body = dataset.json()
    assert ds_body["dataset_id"] == asset_id
    assert ds_body["overall_score"] >= 0


def test_governance_score_not_found(client):
    headers = _auth_headers(client)
    response = client.get("/governance/score/999999", headers=headers)
    assert response.status_code == 404
