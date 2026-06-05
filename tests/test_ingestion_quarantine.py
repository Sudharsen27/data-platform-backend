import time
import uuid

import pytest

from app.routes.ingestion import _parse_csv_row


def test_parse_csv_row_name_email():
    row = _parse_csv_row({"name": "Jane Doe", "email": "jane@example.com", "error": "Bad phone"})
    assert row == {"name": "Jane Doe", "email": "jane@example.com", "error": "Bad phone"}


def test_parse_csv_row_infers_error_for_missing_email():
    row = _parse_csv_row({"name": "No Email", "email": ""})
    assert row["error"] == "Missing email"


def test_parse_csv_row_infers_error_for_invalid_email():
    row = _parse_csv_row({"name": "Bad", "email": "not-an-email"})
    assert row["error"] == "Invalid email format"


def test_parse_csv_row_sales_schema_fallback():
    row = _parse_csv_row({"Country": "India", "Item Type": "Fruit", "Order ID": "42"})
    assert row is not None
    assert "India Fruit" in row["name"]
    assert row["email"].endswith("@sales.local")


def _register_admin(client):
    email = f"ingest_admin_{uuid.uuid4().hex}@example.com"
    password = "AdminIngest9!"
    client.post(
        "/auth/register",
        json={
            "full_name": "Ingest Admin",
            "email": email,
            "company_name": "Acme",
            "password": password,
        },
    )
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

    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    return {"Authorization": f"Bearer {token}"}


def _wait_for_job(client, headers, job_id, timeout_seconds=5):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = client.get(f"/ingestion/jobs/{job_id}", headers=headers)
        assert response.status_code == 200
        status = response.json().get("status")
        if status in {"completed", "failed"}:
            return response.json()
        time.sleep(0.1)
    pytest.fail(f"Ingestion job {job_id} did not finish in time")


def test_csv_upload_imports_to_quarantine(client):
    headers = _register_admin(client)
    csv_content = (
        "name,email,error\n"
        "John Doe,john@example.com,Missing phone\n"
        "Jane Smith,invalid-email,Invalid email format\n"
    ).encode("utf-8")

    response = client.post(
        "/ingestion/upload?target=quarantine",
        headers=headers,
        files={"file": ("test_quarantine.csv", csv_content, "text/csv")},
    )
    assert response.status_code == 200, response.text
    job = response.json()
    assert job["target"] == "quarantine"

    finished = _wait_for_job(client, headers, job["id"])
    assert finished["status"] == "completed"
    assert finished["inserted_rows"] == 2

    quarantine = client.get("/quarantine", headers=headers)
    assert quarantine.status_code == 200
    rows = quarantine.json()
    emails = {row["email"] for row in rows}
    assert "john@example.com" in emails
    assert "invalid-email" in emails


def test_csv_upload_rejects_invalid_target(client):
    headers = _register_admin(client)
    csv_content = b"name,email\nTest,test@example.com\n"
    response = client.post(
        "/ingestion/upload?target=invalid",
        headers=headers,
        files={"file": ("bad_target.csv", csv_content, "text/csv")},
    )
    assert response.status_code == 400
