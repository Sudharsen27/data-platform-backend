import uuid
from datetime import datetime, timedelta

from app.database import SessionLocal
from app.models import AuditLog, CatalogAsset, QuarantineData, Rule, StewardshipQueue, SyncJob, User


def _register(client, email: str, password: str):
    return client.post(
        "/auth/register",
        json={
            "full_name": "Dash User",
            "email": email,
            "company_name": "Acme",
            "password": password,
        },
    )


def _login(client, email: str, password: str):
    return client.post("/auth/login", json={"email": email, "password": password})


def test_dashboard_overview_returns_kpi_cards_and_alerts(client):
    email = f"dash_{uuid.uuid4().hex}@example.com"
    password = "DashboardTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    db = SessionLocal()
    try:
        db.add(
            QuarantineData(name="Bad Row", email="dup@test.com", error="Invalid email")
        )
        db.add(QuarantineData(name="Good Row", email="ok@test.com", error=""))
        db.add(
            SyncJob(
                status="failed",
                quarantine_rows_synced=1,
                rules_synced=0,
                error_message="Connection timeout",
            )
        )
        db.add(
            StewardshipQueue(
                name="Review Me",
                email="review@test.com",
                issue="Duplicate",
                status="pending",
            )
        )
        db.commit()
    finally:
        db.close()

    response = client.get("/dashboard/overview", headers=headers)
    assert response.status_code == 200, response.text
    payload = response.json()

    assert "kpi_cards" in payload
    assert len(payload["kpi_cards"]) == 4
    assert payload["kpi_cards"][0]["key"] == "completeness"
    assert "%" in payload["kpi_cards"][0]["value"]

    assert "kpi_summary" in payload
    assert "success_rate" in payload["kpi_summary"]

    assert "alerts" in payload
    assert any("failed" in alert["name"].lower() for alert in payload["alerts"])
    assert any(alert.get("href") for alert in payload["alerts"])

    assert "compliance" in payload
    assert len(payload["compliance"]["checks"]) == 4
    assert 0 <= payload["compliance"]["overall_percent"] <= 100
    check_keys = {c["key"] for c in payload["compliance"]["checks"]}
    assert "quality_rules" in check_keys
    assert "pii_ownership" in check_keys

    assert "trends" in payload
    assert "audit_activity" in payload
    assert isinstance(payload["audit_activity"], list)
    assert "sla" in payload
    assert len(payload["sla"]["widgets"]) == 5
    assert len(payload["trends"]["records_trend"]) == 7
    assert payload["trends"]["records_trend"][0]["day"]
    assert "date" in payload["trends"]["records_trend"][0]


def test_compliance_reflects_rules_and_catalog(client):
    email = f"comp_{uuid.uuid4().hex}@example.com"
    password = "ComplianceTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    db = SessionLocal()
    try:
        db.add(Rule(field="email", rule="required", status="active"))
        db.add(
            CatalogAsset(
                asset_key="pii_test_asset",
                name="Customer PII",
                pii_tier="confidential",
                owner_email="",
            )
        )
        db.add(
            AuditLog(
                user_id=email,
                action="pipeline_run",
                entity="pipeline",
                old_value="",
                new_value="ok",
            )
        )
        db.commit()
    finally:
        db.close()

    payload = client.get("/dashboard/overview", headers=headers).json()
    compliance = payload["compliance"]
    by_key = {c["key"]: c for c in compliance["checks"]}
    assert by_key["pii_ownership"]["status"] in ("needs_review", "fail")
    assert by_key["audit_trail"]["status"] == "pass"
    assert by_key["quality_rules"]["status"] in ("pass", "needs_review", "fail")


def test_trends_aggregate_sync_jobs_by_day(client):
    email = f"trend_{uuid.uuid4().hex}@example.com"
    password = "TrendTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    now = datetime.utcnow()
    db = SessionLocal()
    try:
        db.add(
            SyncJob(
                status="success",
                start_time=now,
                quarantine_rows_synced=12,
                rules_synced=2,
            )
        )
        db.add(
            SyncJob(
                status="failed",
                start_time=now - timedelta(days=1),
                quarantine_rows_synced=3,
                rules_synced=0,
                error_message="timeout",
            )
        )
        db.commit()
    finally:
        db.close()

    trends = client.get("/dashboard/overview", headers=headers).json()["trends"]
    today_point = next(p for p in trends["records_trend"] if p["date"] == now.date().isoformat())
    assert today_point["processed"] >= 12
    assert today_point["successful_jobs"] >= 1


def test_error_distribution_uses_postgres_when_snowflake_empty(client, monkeypatch):
    """Imports write to Postgres; dashboard must not show 0 errors when Snowflake is empty."""
    email = f"err_{uuid.uuid4().hex}@example.com"
    password = "ErrorDistTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    monkeypatch.setattr(
        "app.services.dashboard_metrics.get_quarantine_analytics",
        lambda: {
            "total_records": 0,
            "success_records": 0,
            "failed_records": 0,
            "success_rate": 0,
            "error_distribution": [],
        },
    )
    monkeypatch.setattr(
        "app.services.dashboard_metrics._snowflake_analytics_configured",
        lambda: True,
    )

    db = SessionLocal()
    try:
        db.add(QuarantineData(name="E1", email="x", error="Invalid email"))
        db.add(QuarantineData(name="E2", email="y", error="Invalid email"))
        db.add(QuarantineData(name="E3", email="z", error="Missing email"))
        db.commit()
    finally:
        db.close()

    trends = client.get("/dashboard/overview", headers=headers).json()["trends"]
    errors = trends["error_distribution"]
    assert len(errors) >= 2
    assert sum(item["count"] for item in errors) >= 3


def test_audit_activity_feed_on_overview(client):
    email = f"audit_feed_{uuid.uuid4().hex}@example.com"
    password = "AuditFeedTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    db = SessionLocal()
    try:
        db.add(
            AuditLog(
                user_id=email,
                action="quarantine_bulk_import",
                entity="quarantine_data",
                old_value="",
                new_value="imported_count=2",
            )
        )
        db.add(
            AuditLog(
                user_id=email,
                action="pipeline_run",
                entity="pipeline",
                old_value="",
                new_value="completed",
            )
        )
        db.commit()
    finally:
        db.close()

    payload = client.get("/dashboard/overview", headers=headers).json()
    activity = payload["audit_activity"]
    assert len(activity) >= 2
    assert activity[0]["summary"]
    assert activity[0]["href"]
    assert any(item["action"] == "quarantine_bulk_import" for item in activity)


def test_sla_widgets_reflect_backlog_and_sync(client):
    email = f"sla_{uuid.uuid4().hex}@example.com"
    password = "SlaWidgetTest9!"
    assert _register(client, email, password).status_code == 200
    token = _login(client, email, password).json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    now = datetime.utcnow()
    db = SessionLocal()
    try:
        for index in range(12):
            db.add(
                StewardshipQueue(
                    name=f"Pending {index}",
                    email=f"user{index}@test.com",
                    issue="Review",
                    status="pending",
                )
            )
        db.add(
            SyncJob(
                status="success",
                start_time=now - timedelta(hours=30),
                end_time=now - timedelta(hours=30),
                quarantine_rows_synced=5,
                rules_synced=1,
            )
        )
        db.commit()
    finally:
        db.close()

    sla = client.get("/dashboard/overview", headers=headers).json()["sla"]
    by_key = {widget["key"]: widget for widget in sla["widgets"]}
    assert by_key["stewardship_backlog"]["status"] == "fail"
    assert by_key["sync_timeliness"]["status"] == "warning"
    assert sla["overall_status"] in ("at_risk", "breach")
