"""Verify sensitive endpoints reject unauthenticated access."""

import uuid

import pytest


def _register_and_login(client, *, role: str = "user"):
    email = f"sec_{uuid.uuid4().hex}@example.com"
    password = "SecurityTest9!"
    client.post(
        "/auth/register",
        json={
            "full_name": "Security Tester",
            "email": email,
            "company_name": "Acme",
            "password": password,
        },
    )
    if role == "admin":
        from app.database import SessionLocal
        from app.models import User

        db = SessionLocal()
        try:
            user = db.query(User).filter(User.email == email).first()
            user.role = "admin"
            db.add(user)
            db.commit()
        finally:
            db.close()

    token = client.post(
        "/auth/login",
        json={"email": email, "password": password},
    ).json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/dashboard"),
        ("GET", "/quarantine"),
        ("GET", "/quarantine/paged"),
        ("GET", "/pipeline/status"),
        ("GET", "/pipeline/runs"),
        ("GET", "/sync/jobs"),
        ("GET", "/sync/scheduler"),
        ("GET", "/analytics/snowflake"),
        ("GET", "/export/quarantine.csv"),
        ("GET", "/export/analytics.csv"),
    ],
)
def test_protected_read_endpoints_require_auth(client, method, path):
    response = client.request(method, path)
    assert response.status_code == 401, f"{method} {path} should require auth"


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", "/sync/snowflake"),
        ("POST", "/sync/jobs/1/retry"),
        ("POST", "/sync/scheduler"),
    ],
)
def test_admin_write_endpoints_require_auth(client, method, path):
    response = client.request(method, path, json={"enabled": False})
    assert response.status_code == 401, f"{method} {path} should require auth"


def test_authenticated_user_can_read_quarantine(client):
    headers = _register_and_login(client)
    response = client.get("/quarantine", headers=headers)
    assert response.status_code == 200


def test_non_admin_cannot_trigger_snowflake_sync(client):
    headers = _register_and_login(client, role="user")
    response = client.post("/sync/snowflake", headers=headers)
    assert response.status_code == 403


def test_health_and_root_remain_public(client):
    assert client.get("/health").status_code == 200
    assert client.get("/").status_code == 200
