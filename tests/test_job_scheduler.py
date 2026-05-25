import pytest

from app.services import job_scheduler as js


@pytest.fixture(autouse=True)
def reset_scheduler_jobs():
    for job_type in js.JOB_IDS:
        js.disable_job(job_type)
    yield
    for job_type in js.JOB_IDS:
        js.disable_job(job_type)


def test_configure_interval_job():
    calls = []

    def callback():
        calls.append(1)

    js.configure_job(
        "pipeline",
        callback,
        enabled=True,
        trigger_type="interval",
        interval_minutes=5,
    )
    state = js.get_job_state("pipeline")
    assert state["enabled"] is True
    assert state["trigger_type"] == "interval"
    assert state["interval_minutes"] == 5
    assert state["next_run_at"] is not None


def test_configure_cron_job():
    js.configure_job(
        "snowflake_sync",
        lambda: None,
        enabled=True,
        trigger_type="cron",
        cron_expression="0 2 * * *",
    )
    state = js.get_job_state("snowflake_sync")
    assert state["enabled"] is True
    assert state["trigger_type"] == "cron"
    assert state["cron_expression"] == "0 2 * * *"


def test_disable_job_clears_state():
    js.configure_job(
        "pipeline",
        lambda: None,
        enabled=True,
        trigger_type="interval",
        interval_minutes=10,
    )
    js.disable_job("pipeline")
    state = js.get_job_state("pipeline")
    assert state["enabled"] is False
    assert state["next_run_at"] is None


def test_invalid_cron_raises():
    with pytest.raises(ValueError):
        js.configure_job(
            "pipeline",
            lambda: None,
            enabled=True,
            trigger_type="cron",
            cron_expression="not a cron",
        )
