import uuid

from app.database import SessionLocal
from app.models import StewardshipQueue, User
from app.services.ai_stewardship_assignments import assign_stewardship_owners


def test_assign_stewardship_owners_writes_owner_email():
    db = SessionLocal()
    try:
        db.add(
            StewardshipQueue(
                name="A",
                email="a@test.com",
                issue="Invalid email in CRM feed",
                status="pending",
            )
        )
        db.add(
            StewardshipQueue(
                name="B",
                email="b@test.com",
                issue="Product SKU mismatch",
                status="pending",
            )
        )
        db.commit()

        result = assign_stewardship_owners(db, assign_all_pending=True)
        db.commit()

        assert result["applied_count"] == 2
        rows = (
            db.query(StewardshipQueue)
            .filter(StewardshipQueue.status == "pending")
            .order_by(StewardshipQueue.id.asc())
            .all()
        )
        assert rows[0].owner_email == "steward-customer@example.com"
        assert rows[1].owner_email == "steward-product@example.com"
    finally:
        db.query(StewardshipQueue).delete()
        db.commit()
        db.close()


def test_assign_selected_ids_only():
    db = SessionLocal()
    try:
        a = StewardshipQueue(
            name="A",
            email="a@test.com",
            issue="Invalid email",
            status="pending",
        )
        b = StewardshipQueue(
            name="B",
            email="b@test.com",
            issue="Product SKU mismatch",
            status="pending",
        )
        db.add(a)
        db.add(b)
        db.commit()
        db.refresh(a)
        db.refresh(b)

        result = assign_stewardship_owners(db, task_ids=[b.id])
        db.commit()

        assert result["applied_count"] == 1
        db.refresh(a)
        db.refresh(b)
        assert (a.owner_email or "") == ""
        assert b.owner_email == "steward-product@example.com"
    finally:
        db.query(StewardshipQueue).delete()
        db.commit()
        db.close()


def test_suggest_stewardship_endpoint_applies_assignments(client):
    email = f"stew_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert (
        client.post(
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "full_name": "Stew Tester",
                "company_name": "Acme",
            },
        ).status_code
        == 200
    )
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        user.role = "admin"
        row = StewardshipQueue(
            name="C",
            email="c@test.com",
            issue="Duplicate customer email",
            status="pending",
        )
        db.add(row)
        db.commit()
        task_id = row.id
    finally:
        db.close()

    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    response = client.post(
        "/ai/actions/suggest-stewardship-owners",
        headers={"Authorization": f"Bearer {token}"},
        json={"ids": [task_id]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert "assigned" in payload["summary"].lower() or payload["details"]

    db = SessionLocal()
    try:
        row = db.query(StewardshipQueue).filter(StewardshipQueue.id == task_id).first()
        assert row.owner_email == "steward-customer@example.com"
    finally:
        db.query(StewardshipQueue).filter(StewardshipQueue.id == task_id).delete()
        db.commit()
        db.close()


def test_suggest_stewardship_requires_selection_or_bulk(client):
    email = f"stew2_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert (
        client.post(
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "full_name": "Stew Tester",
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
    response = client.post(
        "/ai/actions/suggest-stewardship-owners",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert response.status_code == 400
