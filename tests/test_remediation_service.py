import uuid

from app.database import SessionLocal
from app.models import StewardshipQueue
from app.services.remediation_service import (
    explain_stewardship_failure,
    generate_remediation,
)


def _seed_stewardship(db, *, issue: str, email: str = "abc@", name: str = "Test User"):
    row = StewardshipQueue(
        name=name,
        email=email,
        issue=issue,
        status="pending",
        owner_email="",
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_invalid_email_remediation(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        row = _seed_stewardship(db, issue="Invalid email format", email="abc@")
        result = generate_remediation(db, row)
        assert "email" in result["root_cause"].lower() or "format" in result["root_cause"].lower()
        assert result["suggested_fix"]
        assert result["business_impact"]
        assert result["risk_score"] >= 40
    finally:
        db.close()


def test_duplicate_id_remediation(monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        row = _seed_stewardship(db, issue="customer_id duplicated", email="dup@example.com")
        result = generate_remediation(db, row)
        assert "duplicate" in result["root_cause"].lower()
        assert "merge" in result["suggested_fix"].lower() or "duplicate" in result["suggested_fix"].lower()
    finally:
        db.close()


def _auth_headers(client):
    email = f"remed_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    client.post(
        "/auth/register",
        json={"email": email, "password": password, "full_name": "Remediation Tester"},
    )
    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def test_stewardship_explain_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        row = _seed_stewardship(db, issue="Email missing", email="")
        sid = row.id
    finally:
        db.close()

    headers = _auth_headers(client)
    response = client.post(
        "/stewardship/explain",
        headers=headers,
        json={"stewardship_id": sid},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["explanation"]
    assert body["stewardship_id"] == sid


def test_stewardship_remediate_and_accept_api(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    db = SessionLocal()
    try:
        row = _seed_stewardship(db, issue="Invalid email", email="bad@")
        sid = row.id
    finally:
        db.close()

    headers = _auth_headers(client)
    remediate = client.post(
        "/stewardship/remediate",
        headers=headers,
        json={"stewardship_id": sid},
    )
    assert remediate.status_code == 200
    body = remediate.json()
    assert body["root_cause"]
    assert body["suggested_fix"]
    assert body.get("id")

    accept = client.post(
        f"/stewardship/remediation/{body['id']}/accept",
        headers=headers,
    )
    assert accept.status_code == 200
    assert accept.json()["status"] == "accepted"
