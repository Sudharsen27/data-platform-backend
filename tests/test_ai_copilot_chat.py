import uuid

from app.database import SessionLocal
from app.models import AICopilotActionLog, CatalogAsset


def _register(client, email: str, password: str):
    return client.post(
        "/auth/register",
        json={
            "email": email,
            "password": password,
            "full_name": "Copilot Tester",
        },
    )


def _login(client, email: str, password: str):
    return client.post("/auth/login", json={"email": email, "password": password})


def _auth_headers(client):
    email = f"copilot_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    return {"Authorization": f"Bearer {token}"}, email


def _seed_catalog_asset():
    db = SessionLocal()
    try:
        key = f"test.customer_{uuid.uuid4().hex[:8]}"
        db.add(
            CatalogAsset(
                asset_key=key,
                name="Test Customer Master",
                asset_type="table",
                domain="Customer",
                owner_email="steward@example.com",
                description="Golden customer master for tests.",
                tags="golden,mdm",
                pii_tier="restricted",
                lineage_node_key=key,
                schema_fields="customer_id,name,email",
                sla_hours=24,
                contract_version="1.0",
            )
        )
        db.commit()
        return key
    finally:
        db.close()


def test_copilot_chat_requires_auth(client):
    assert client.post("/ai/copilot/chat", json={"question": "What is Customer Master?"}).status_code == 401


def test_copilot_chat_empty_question(client):
    headers, _ = _auth_headers(client)
    response = client.post(
        "/ai/copilot/chat",
        headers=headers,
        json={"question": "   "},
    )
    assert response.status_code == 400


def test_copilot_chat_heuristic_answer(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    _seed_catalog_asset()
    headers, email = _auth_headers(client)
    response = client.post(
        "/ai/copilot/chat",
        headers=headers,
        json={"question": "What is Customer Master?"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "answer" in payload
    assert len(payload["answer"]) > 10
    assert isinstance(payload["sources"], list)

    db = SessionLocal()
    try:
        log = (
            db.query(AICopilotActionLog)
            .filter(
                AICopilotActionLog.user_id == email,
                AICopilotActionLog.action_key == "copilot_chat",
            )
            .order_by(AICopilotActionLog.id.desc())
            .first()
        )
        assert log is not None
        assert log.status == "success"
    finally:
        db.close()


def test_copilot_chat_list_datasets(client, monkeypatch):
    monkeypatch.setenv("AI_PROVIDER", "heuristic")
    monkeypatch.delenv("GROQ_API_KEY", raising=False)
    headers, _ = _auth_headers(client)
    response = client.post(
        "/ai/copilot/chat",
        headers=headers,
        json={"question": "List available datasets"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "dataset" in payload["answer"].lower() or "catalog" in payload["answer"].lower()
