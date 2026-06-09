import uuid

from app.database import SessionLocal
from app.models import CatalogAsset, GlossaryEntry
from app.services.glossary_generator_service import (
    generate_dataset_glossary,
    generate_field_glossary,
    serialize_glossary_entry,
)


def _seed_asset(db):
    existing = db.query(CatalogAsset).filter(CatalogAsset.asset_key == "test.glossary_ds").first()
    if existing:
        return existing
    asset = CatalogAsset(
        asset_key="test.glossary_ds",
        name="customer_master",
        domain="Customer",
        description="Golden customer master for CRM and analytics",
        tags="customer,master",
        pii_tier="confidential",
        schema_fields="customer_id,customer_email,customer_name,created_date",
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def test_generate_customer_email_glossary_heuristic(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    result = generate_field_glossary(
        field_name="customer_email",
        dataset_name="customer_master",
    )
    assert result["title"]
    assert "email" in result["definition"].lower()
    assert result["usage"]
    assert result["governance_notes"]
    assert result["source_engine"] == "heuristics"
    assert len(result["examples"]) >= 1


def test_generate_customer_id_glossary_heuristic(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    result = generate_field_glossary(field_name="customer_id", dataset_name="customer_master")
    assert "identifier" in result["definition"].lower() or "unique" in result["definition"].lower()
    assert result["title"]


def test_generate_dataset_glossary(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        result = generate_dataset_glossary(db, asset.id)
        assert result is not None
        assert result["dataset_title"] == "customer_master"
        assert result["dataset_definition"]
        assert result["business_usage"]
        assert len(result["field_glossaries"]) == 4
    finally:
        db.close()


def test_serialize_glossary_entry_roundtrip():
    row = GlossaryEntry(
        catalog_asset_id=1,
        field_name="customer_email",
        title="Customer Email Address",
        definition="Primary email for customer communication.",
        usage="CRM outreach",
        governance_notes="PII",
        examples='["a@b.com"]',
        status="approved",
        source_engine="heuristics",
        created_by="tester@example.com",
        updated_by="tester@example.com",
    )
    data = serialize_glossary_entry(row)
    assert data["examples"] == ["a@b.com"]
    assert data["status"] == "approved"


def _auth_headers(client):
    email = f"glossary_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Glossary Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_glossary_generate_field_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    headers = _auth_headers(client)
    response = client.post(
        "/glossary/generate",
        headers=headers,
        json={"field_name": "customer_email", "dataset_name": "customer_master"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["title"]
    assert body["definition"]
    assert body["governance_notes"]


def test_glossary_generate_dataset_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        asset_id = asset.id
    finally:
        db.close()

    headers = _auth_headers(client)
    response = client.post(
        "/glossary/generate-dataset",
        headers=headers,
        json={"dataset_id": asset_id},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["dataset_title"]
    assert body["dataset_definition"]
    assert len(body["field_glossaries"]) >= 1


def test_glossary_save_and_list_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        asset_id = asset.id
    finally:
        db.close()

    headers = _auth_headers(client)
    save_resp = client.post(
        "/glossary/save",
        headers=headers,
        json={
            "catalog_asset_id": asset_id,
            "field_name": "customer_email",
            "title": "Customer Email Address",
            "definition": "Approved enterprise definition for customer email.",
            "usage": "Notifications and account recovery.",
            "governance_notes": "PII — mask in lower environments.",
            "examples": ["user@example.com"],
            "status": "approved",
        },
    )
    assert save_resp.status_code == 200
    saved = save_resp.json()
    assert saved["status"] == "approved"
    assert saved["id"]

    list_resp = client.get(
        f"/glossary/entries?dataset_id={asset_id}&field_name=customer_email",
        headers=headers,
    )
    assert list_resp.status_code == 200
    items = list_resp.json()["items"]
    assert any(i["definition"].startswith("Approved enterprise") for i in items)
