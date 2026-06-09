import uuid

from app.database import SessionLocal
from app.models import CatalogAsset, Rule
from app.services.rule_engine import evaluate_row
from app.services.rule_recommendation_service import (
    recommend_rules_for_dataset,
    recommend_rules_for_field,
)


def _seed_asset(db):
    existing = db.query(CatalogAsset).filter(CatalogAsset.asset_key == "test.rule_rec_ds").first()
    if existing:
        return existing
    asset = CatalogAsset(
        asset_key="test.rule_rec_ds",
        name="Customer Master",
        domain="Customer",
        description="Customer golden record",
        tags="customer,pii",
        pii_tier="confidential",
        schema_fields="customer_id,customer_email,phone_number,date_of_birth",
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


def test_recommend_customer_email_rules(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        result = recommend_rules_for_field(
            db,
            field_name="customer_email",
            dataset_id=asset.id,
        )
        rules = result["rules"]
        texts = " ".join(r["rule_text"].lower() for r in rules)
        assert "email" in texts or "@" in texts
        assert any(r["rule_type"] in ("format", "completeness", "compliance") for r in rules)
        assert result["risk_analysis"]["data_quality_risk_level"]
    finally:
        db.close()


def test_recommend_customer_id_uniqueness(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        result = recommend_rules_for_field(db, field_name="customer_id", dataset_name="Customer Master")
        texts = " ".join(r["rule_text"].lower() for r in result["rules"])
        assert "unique" in texts
        assert "null" in texts
    finally:
        db.close()


def test_recommend_phone_and_dob_rules(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        phone = recommend_rules_for_field(db, field_name="phone_number")
        dob = recommend_rules_for_field(db, field_name="date_of_birth")
        phone_text = " ".join(r["rule_text"].lower() for r in phone["rules"])
        dob_text = " ".join(r["rule_text"].lower() for r in dob["rules"])
        assert "digit" in phone_text
        assert "10" in phone_text
        assert "future" in dob_text
    finally:
        db.close()


def test_rule_engine_supports_recommended_patterns():
    from app.models import Rule as RuleModel

    rules = [
        RuleModel(id=1, field="phone_number", rule="Must contain only digits", status="active", created_by="test"),
        RuleModel(id=2, field="date_of_birth", rule="Cannot be future date", status="active", created_by="test"),
    ]
    phone_violations = evaluate_row({"phone_number": "abc"}, rules)
    assert any("digits" in v.message for v in phone_violations)
    future_violations = evaluate_row({"date_of_birth": "2099-01-01"}, rules)
    assert any("future" in v.message for v in future_violations)


def _auth_headers(client):
    email = f"rules_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Rules Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_rules_recommend_dataset_api(client, monkeypatch):
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
        "/rules/recommend",
        headers=headers,
        json={"dataset_id": asset_id},
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["recommended_rules"]) >= 2
    assert body["risk_analysis"]["governance_risk_level"]


def test_rules_recommend_field_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    headers = _auth_headers(client)
    response = client.post(
        "/rules/recommend-field",
        headers=headers,
        json={"field_name": "customer_email"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["field_name"] == "customer_email"
    assert len(body["rules"]) >= 1


def test_approve_recommendation_creates_rule(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        asset = _seed_asset(db)
        asset_id = asset.id
    finally:
        db.close()

    headers = _auth_headers(client)
    rec = client.post(
        "/rules/recommend",
        headers=headers,
        json={"dataset_id": asset_id},
    ).json()
    pending = next((r for r in rec["recommended_rules"] if r.get("id")), None)
    if pending is None:
        listed = client.get(
            f"/rules/recommendations?dataset_id={asset_id}&status=pending",
            headers=headers,
        ).json()
        pending = (listed.get("items") or [None])[0]
    assert pending is not None

    approve = client.post(
        f"/rules/recommendations/{pending['id']}/approve",
        headers=headers,
    )
    assert approve.status_code == 200
    assert approve.json()["status"] == "approved"
    assert approve.json()["approved_rule_id"] is not None

    db = SessionLocal()
    try:
        rule_count = db.query(Rule).filter(Rule.field == "customer_id").count()
        assert rule_count >= 1
    finally:
        db.close()
