import uuid

from app.database import SessionLocal
from app.models import CatalogAsset
from app.services.classification_ai_service import enhance_field_classification
from app.services.data_classification_service import (
    analyze_dataset,
    classify_field_name,
    find_datasets_with_classification,
)


def _seed_asset(db):
    existing = db.query(CatalogAsset).filter(CatalogAsset.asset_key == "test.classify_ds").first()
    if existing:
        return existing
    asset = CatalogAsset(
        asset_key="test.classify_ds",
        name="Customer Dataset",
        domain="Customer",
        description="Customer contact and financial attributes",
        tags="customer,pii",
        pii_tier="confidential",
        schema_fields="customer_name,customer_email,phone_number,aadhaar_number,salary,revenue,created_date",
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def test_classify_customer_email_as_pii():
    result = classify_field_name("customer_email", dataset_name="Customer Master")
    assert result["classification"] == "PII"
    assert result["confidence"] >= 90
    assert "email" in result["reason"].lower()
    assert len(result["recommendations"]) > 0


def test_classify_aadhaar_as_sensitive():
    result = classify_field_name("aadhaar_number")
    assert result["classification"] == "Sensitive"
    assert result["confidence"] >= 95


def test_classify_salary_and_revenue():
    salary = classify_field_name("salary")
    revenue = classify_field_name("revenue")
    assert salary["classification"] == "Confidential"
    assert revenue["classification"] == "Financial"


def test_classify_created_date_as_public():
    result = classify_field_name("created_date")
    assert result["classification"] == "Public"


def test_analyze_dataset_bulk_counts():
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        result = analyze_dataset(db, asset.id)
        assert result is not None
        assert result["pii_count"] >= 2
        assert result["sensitive_count"] >= 1
        assert result["risk_score"] >= 30
        assert result["dataset_classification"] in ("PII", "Sensitive", "Confidential", "Financial")
        assert len(result["recommendations"]) > 0
    finally:
        db.close()


def test_find_datasets_with_pii():
    db = SessionLocal()
    try:
        _seed_asset(db)
        matches = find_datasets_with_classification(db, "PII")
        assert any(m["dataset_key"] == "test.classify_ds" for m in matches)
    finally:
        db.close()


def test_enhance_field_heuristic(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    base = classify_field_name("customer_email")
    enhanced = enhance_field_classification(base)
    assert enhanced["ai_explanation"]
    assert enhanced["source_engine"] == "heuristics"


def _auth_headers(client):
    email = f"classify_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Classify Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_classification_analyze_field_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    headers = _auth_headers(client)
    response = client.post(
        "/classification/analyze-field",
        headers=headers,
        json={"field_name": "customer_email"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["classification"] == "PII"
    assert body["confidence"] >= 90


def test_classification_analyze_dataset_api(client, monkeypatch):
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
        "/classification/analyze-dataset",
        headers=headers,
        json={"dataset_id": asset_id},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["risk_score"] >= 0
    assert body["pii_count"] >= 1
    assert body["dataset_classification"]
