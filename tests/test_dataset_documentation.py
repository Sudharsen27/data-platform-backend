import uuid

from app.database import SessionLocal
from app.models import CatalogAsset
from app.services.dataset_documentation_service import (
    documentation_to_markdown,
    documentation_to_text,
    generate_dataset_documentation,
)


def _seed_asset(db):
    existing = db.query(CatalogAsset).filter(CatalogAsset.asset_key == "test.doc_ds").first()
    if existing:
        return existing
    asset = CatalogAsset(
        asset_key="test.doc_ds",
        name="Customer Master",
        domain="Customer",
        description="Golden customer master for CRM and analytics",
        tags="customer,master,pii",
        pii_tier="confidential",
        owner_email="steward@example.com",
        schema_fields="customer_id,customer_email,customer_name,salary,created_date",
        sla_hours=24,
        contract_version="1.0",
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def test_generate_customer_master_documentation_heuristic(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        result = generate_dataset_documentation(db, asset.id)
        assert result is not None
        assert result["title"] == "Customer Master"
        assert result["summary"]
        assert result["purpose"]
        assert result["governance_notes"]
        assert result["usage_guidelines"]
        assert result["compliance_considerations"]
        assert len(result["key_fields"]) >= 3
        assert result["source_engine"] == "heuristics"
    finally:
        db.close()


def test_documentation_export_formats(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        doc = generate_dataset_documentation(db, asset.id)
        md = documentation_to_markdown(doc)
        txt = documentation_to_text(doc)
        assert "# Customer Master" in md
        assert "Summary" in md
        assert "customer_email" in md or "Key Fields" in md
        assert "Customer Master" in txt
    finally:
        db.close()


def _auth_headers(client):
    email = f"doc_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Doc Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_documentation_generate_api(client, monkeypatch):
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
        "/documentation/generate",
        headers=headers,
        json={"dataset_id": asset_id},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["title"]
    assert body["summary"]
    assert body["governance_notes"]
    assert body["usage_guidelines"]
    assert len(body["key_fields"]) >= 1


def test_documentation_save_and_export_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        asset_id = asset.id
    finally:
        db.close()

    headers = _auth_headers(client)
    generated = client.post(
        "/documentation/generate",
        headers=headers,
        json={"dataset_id": asset_id},
    ).json()

    save_resp = client.post(
        "/documentation/save",
        headers=headers,
        json={
            "catalog_asset_id": asset_id,
            "title": generated["title"],
            "summary": generated["summary"],
            "business_description": generated.get("business_description", ""),
            "purpose": generated["purpose"],
            "key_fields": generated["key_fields"],
            "owner_recommendation": generated.get("owner_recommendation", ""),
            "governance_notes": generated["governance_notes"],
            "classification_summary": generated.get("classification_summary", ""),
            "quality_expectations": generated.get("quality_expectations", ""),
            "usage_guidelines": generated["usage_guidelines"],
            "compliance_considerations": generated.get("compliance_considerations", ""),
            "status": "approved",
        },
    )
    assert save_resp.status_code == 200
    saved = save_resp.json()
    assert saved["status"] == "approved"

    get_resp = client.get(f"/documentation/dataset/{asset_id}", headers=headers)
    assert get_resp.status_code == 200
    assert get_resp.json()["summary"] == generated["summary"]

    export_resp = client.post(
        "/documentation/export?format=markdown",
        headers=headers,
        json={"dataset_id": asset_id},
    )
    assert export_resp.status_code == 200
    export_body = export_resp.json()
    assert export_body["format"] == "markdown"
    assert "Customer Master" in export_body["content"]
