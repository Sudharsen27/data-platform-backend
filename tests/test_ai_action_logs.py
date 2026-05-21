import uuid

from app.database import SessionLocal
from app.models import AICopilotActionLog, User


def _register_admin(client):
    email = f"ailog_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert (
        client.post(
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "full_name": "AI Log Tester",
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
    return {"Authorization": f"Bearer {token}"}, email


def test_ai_action_logs_list_and_filter(client):
    headers, email = _register_admin(client)
    db = SessionLocal()
    try:
        db.add(
            AICopilotActionLog(
                action_key="generate_rules",
                user_id=email,
                status="success",
                summary="Created 2 rules",
                payload='{"created_rule_ids": [1, 2]}',
            )
        )
        db.add(
            AICopilotActionLog(
                action_key="explain_quarantine",
                user_id=email,
                status="success",
                summary="Explained error",
                payload='{"source": "heuristics"}',
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.get("/ai/actions/logs", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] >= 2
    assert len(payload["items"]) >= 2
    explain = next(i for i in payload["items"] if i["action_key"] == "explain_quarantine")
    assert explain["payload"]["source"] == "heuristics"

    filtered = client.get(
        "/ai/actions/logs?action_key=generate_rules",
        headers=headers,
    )
    assert filtered.status_code == 200
    body = filtered.json()
    assert all(item["action_key"] == "generate_rules" for item in body["items"])


def test_ai_action_logs_requires_auth(client):
    assert client.get("/ai/actions/logs").status_code == 401


def test_ai_action_logs_invalid_action_key(client):
    headers, _ = _register_admin(client)
    response = client.get("/ai/actions/logs?action_key=invalid", headers=headers)
    assert response.status_code == 400
