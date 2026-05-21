import uuid

from app.database import SessionLocal
from app.models import MasterData, StewardshipQueue, User
from app.services.master_data_publish import publish_stewardship_to_master


def test_publish_merges_duplicate_email():
    db = SessionLocal()
    try:
        db.add(
            MasterData(
                source_queue_id=1,
                name="Old Name",
                email="same@test.com",
            )
        )
        record = StewardshipQueue(
            id=2,
            name="New Name",
            email="same@test.com",
            issue="Duplicate email",
            status="pending",
        )
        db.add(record)
        db.commit()

        master, msg, merged = publish_stewardship_to_master(db, record)
        db.commit()

        assert merged is True
        assert master.id == 1
        assert master.name == "New Name"
        assert "survivorship" in msg.lower()
        assert db.query(MasterData).count() == 1
    finally:
        db.query(MasterData).delete()
        db.query(StewardshipQueue).delete()
        db.commit()
        db.close()


def test_master_data_list_and_compare(client):
    email = f"md_{uuid.uuid4().hex}@example.com"
    password = "TestPass123!"
    assert (
        client.post(
            "/auth/register",
            json={
                "email": email,
                "password": password,
                "full_name": "MD Tester",
                "company_name": "Acme",
            },
        ).status_code
        == 200
    )
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        user.role = "admin"
        db.add(
            StewardshipQueue(
                id=99,
                name="Golden User",
                email="golden@test.com",
                issue="Fixed",
                status="approved",
            )
        )
        db.add(
            MasterData(
                source_queue_id=99,
                name="Golden User",
                email="golden@test.com",
            )
        )
        db.commit()
    finally:
        db.close()

    token = client.post("/auth/login", json={"email": email, "password": password}).json()[
        "access_token"
    ]
    headers = {"Authorization": f"Bearer {token}"}

    listing = client.get("/master-data", headers=headers)
    assert listing.status_code == 200
    assert listing.json()["total"] >= 1

    compare = client.get("/master-data/compare/99", headers=headers)
    assert compare.status_code == 200
    body = compare.json()
    assert body["is_published"] is True
    assert body["golden"]["email"] == "golden@test.com"

    db = SessionLocal()
    try:
        db.query(MasterData).filter(MasterData.source_queue_id == 99).delete()
        db.query(StewardshipQueue).filter(StewardshipQueue.id == 99).delete()
        db.commit()
    finally:
        db.close()
