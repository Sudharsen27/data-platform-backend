import uuid

from app.database import SessionLocal
from app.models import QuarantineData, Rule, User
from app.services.ai_rule_suggestions import (
    _map_error_to_rule,
    build_rule_suggestions_from_quarantine,
)


def test_map_invalid_email_pattern():
    mapped = _map_error_to_rule("Invalid email format", 50, 100)
    assert mapped is not None
    field, rule, confidence = mapped
    assert field == "email"
    assert "@" in rule
    assert confidence >= 0.55


def test_build_suggestions_from_quarantine_patterns():
    db = SessionLocal()
    try:
        db.add(QuarantineData(name="A", email="x", error="Invalid email format"))
        db.add(QuarantineData(name="B", email="y", error="Invalid email format"))
        db.add(QuarantineData(name="C", email="z", error="Duplicate email in CRM"))
        db.commit()

        suggestions = build_rule_suggestions_from_quarantine(db)
        assert len(suggestions) >= 2
        fields = {s["field"] for s in suggestions}
        assert "email" in fields
        assert all(s["occurrence_count"] > 0 for s in suggestions)
        email_rules = [s for s in suggestions if s["field"] == "email"]
        assert any("@" in s["rule"] for s in email_rules)
    finally:
        db.query(QuarantineData).delete()
        db.commit()
        db.close()


def test_generate_rules_endpoint_creates_from_quarantine(client):
    email = f"rules_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert (
        client.post(
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "full_name": "Rules Tester",
                "company_name": "Acme",
            },
        ).status_code
        == 200
    )
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        user.role = "admin"
        db.commit()
    finally:
        db.close()

    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    headers = {"Authorization": f"Bearer {token}"}

    db = SessionLocal()
    try:
        db.add(QuarantineData(name="X", email="bad", error="Invalid email format"))
        db.commit()
    finally:
        db.close()

    response = client.post("/ai/actions/generate-rules", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["action"] == "generate_rules"
    assert "quarantine" in payload["summary"].lower() or len(payload.get("details") or []) > 0

    db = SessionLocal()
    try:
        created = (
            db.query(Rule)
            .filter(Rule.created_by == "ai-copilot", Rule.field == "email")
            .all()
        )
        assert len(created) >= 1
    finally:
        db.query(Rule).filter(Rule.created_by == "ai-copilot").delete()
        db.query(QuarantineData).filter(QuarantineData.error == "Invalid email format").delete()
        db.commit()
        db.close()
