import uuid


def _register(client, email: str, password: str, full_name: str = "Test User"):
    return client.post(
        "/auth/register",
        json={
            "full_name": full_name,
            "email": email,
            "company_name": "Acme",
            "password": password,
        },
    )


def _login(client, email: str, password: str):
    return client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )


def test_register_and_login_returns_tokens(client):
    email = "portfolio_tester@example.com"
    password = "PortfolioTest1!"
    r = _register(client, email, password)
    assert r.status_code == 200, r.text
    r2 = _login(client, email, password)
    assert r2.status_code == 200, r2.text
    data = r2.json()
    assert "access_token" in data
    assert data.get("token_type") == "bearer"


def test_login_rejects_wrong_password(client):
    email = "wrong_pw_user@example.com"
    password = "CorrectHorse1!"
    assert _register(client, email, password).status_code == 200
    r = _login(client, email, "WrongPassword9!")
    assert r.status_code == 401


def test_quarantine_import_requires_admin_and_audit(client):
    admin_email = f"admin_imp_{uuid.uuid4().hex}@example.com"
    password = "AdminUserPass9!"
    assert _register(client, admin_email, password).status_code == 200

    from app.database import SessionLocal
    from app.models import User

    db = SessionLocal()
    try:
        u = db.query(User).filter(User.email == admin_email).first()
        assert u is not None
        u.role = "admin"
        db.add(u)
        db.commit()
    finally:
        db.close()

    token = _login(client, admin_email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    payload = {
        "rows": [
            {"name": "Import One", "email": "bad@email", "error": "Invalid domain"},
            {"name": "Import Two", "email": "", "error": "Missing email"},
        ]
    }
    r = client.post("/quarantine/import", json=payload, headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["count"] == 2

    audit = client.get("/audit?action=quarantine_bulk_import", headers=headers)
    assert audit.status_code == 200
    rows = audit.json()
    assert any(x.get("action") == "quarantine_bulk_import" for x in rows)


def test_non_admin_cannot_import_quarantine(client):
    email = "regular_guy@example.com"
    password = "RegularUser9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}
    r = client.post(
        "/quarantine/import",
        json={"rows": [{"name": "X", "email": "a@b.com", "error": ""}]},
        headers=headers,
    )
    assert r.status_code == 403
