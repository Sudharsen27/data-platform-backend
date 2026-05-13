def test_health_reports_database_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["database"] == "ok"
    assert body["snowflake"] in ("ok", "skipped", "failed")
    assert body["status"] in ("ok", "degraded")
