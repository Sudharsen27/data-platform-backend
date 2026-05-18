import uuid


def _register(client, email: str, password: str):
    return client.post(
        "/auth/register",
        json={
            "email": email,
            "password": password,
            "full_name": "AI Tester",
        },
    )


def _login(client, email: str, password: str):
    return client.post("/auth/login", json={"email": email, "password": password})


def _auth_headers(client):
    email = f"ai_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_ai_status_heuristic(client):
    headers = _auth_headers(client)
    response = client.get("/ai/status", headers=headers)
    assert response.status_code == 200
    payload = response.json()
    assert payload["enabled"] is True
    assert payload["provider"] == "heuristic"
    assert payload["available"] is True
    assert payload["mode"] == "heuristics"


def test_explain_quarantine_heuristic(client):
    headers = _auth_headers(client)
    response = client.post(
        "/ai/actions/explain-quarantine",
        headers=headers,
        json={
            "name": "Jane",
            "email": "bad-email",
            "error": "Invalid email format",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] in ("heuristics", "rules")
    assert "email" in payload["explanation"].lower()
    assert len(payload["explanation"]) > 20


def test_ai_insights_requires_auth(client):
    assert client.get("/ai/insights").status_code == 401
