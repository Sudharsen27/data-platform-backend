from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Literal

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

JobType = Literal["snowflake_sync", "pipeline"]
TriggerType = Literal["interval", "cron"]

JOB_IDS: dict[JobType, str] = {
    "snowflake_sync": "snowflake_sync_job",
    "pipeline": "pipeline_run_job",
}

JOB_LABELS: dict[JobType, str] = {
    "snowflake_sync": "Snowflake sync",
    "pipeline": "Governance pipeline",
}

scheduler = BackgroundScheduler()
_last_config: dict[JobType, dict] = {}


def ensure_scheduler_started():
    if not scheduler.running:
        scheduler.start()


def _iso_next_run(job) -> str | None:
    if not job or not job.next_run_time:
        return None
    run_at = job.next_run_time
    if run_at.tzinfo is None:
        run_at = run_at.replace(tzinfo=timezone.utc)
    return run_at.astimezone(timezone.utc).isoformat()


def configure_job(
    job_type: JobType,
    callback: Callable,
    *,
    enabled: bool,
    trigger_type: TriggerType = "interval",
    interval_minutes: int | None = None,
    cron_expression: str | None = None,
):
    ensure_scheduler_started()
    job_id = JOB_IDS[job_type]

    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    if not enabled:
        _last_config.pop(job_type, None)
        return

    if trigger_type == "cron":
        if not cron_expression or not cron_expression.strip():
            raise ValueError("cron_expression is required when trigger_type is cron")
        trigger = CronTrigger.from_crontab(cron_expression.strip())
        stored = {
            "trigger_type": "cron",
            "cron_expression": cron_expression.strip(),
            "interval_minutes": None,
        }
    else:
        if interval_minutes is None or interval_minutes < 1:
            raise ValueError("interval_minutes must be at least 1")
        trigger = IntervalTrigger(minutes=interval_minutes)
        stored = {
            "trigger_type": "interval",
            "interval_minutes": interval_minutes,
            "cron_expression": None,
        }

    scheduler.add_job(callback, trigger=trigger, id=job_id, replace_existing=True)
    _last_config[job_type] = stored


def disable_job(job_type: JobType):
    job_id = JOB_IDS[job_type]
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    _last_config.pop(job_type, None)


def get_job_state(job_type: JobType) -> dict:
    job_id = JOB_IDS[job_type]
    job = scheduler.get_job(job_id)
    stored = _last_config.get(job_type, {})

    if not job:
        return {
            "job_type": job_type,
            "label": JOB_LABELS[job_type],
            "enabled": False,
            "trigger_type": stored.get("trigger_type"),
            "interval_minutes": stored.get("interval_minutes"),
            "cron_expression": stored.get("cron_expression"),
            "next_run_at": None,
        }

    trigger = job.trigger
    trigger_type = stored.get("trigger_type")
    interval_minutes = stored.get("interval_minutes")
    cron_expression = stored.get("cron_expression")

    if isinstance(trigger, IntervalTrigger):
        trigger_type = trigger_type or "interval"
        interval_minutes = interval_minutes or int(trigger.interval.total_seconds() / 60)
    elif isinstance(trigger, CronTrigger):
        trigger_type = trigger_type or "cron"

    return {
        "job_type": job_type,
        "label": JOB_LABELS[job_type],
        "enabled": True,
        "trigger_type": trigger_type,
        "interval_minutes": interval_minutes,
        "cron_expression": cron_expression,
        "next_run_at": _iso_next_run(job),
    }


def get_all_job_states() -> list[dict]:
    return [get_job_state(job_type) for job_type in JOB_IDS]


def count_active_jobs() -> int:
    return sum(1 for job_type in JOB_IDS if get_job_state(job_type)["enabled"])


def any_scheduler_enabled() -> bool:
    return count_active_jobs() > 0


# Backward-compatible Snowflake-only helpers
SYNC_JOB_ID = JOB_IDS["snowflake_sync"]


def configure_sync_schedule(sync_callback, interval_minutes: int):
    configure_job(
        "snowflake_sync",
        sync_callback,
        enabled=True,
        trigger_type="interval",
        interval_minutes=interval_minutes,
    )


def disable_sync_schedule():
    disable_job("snowflake_sync")


def get_scheduler_state():
    state = get_job_state("snowflake_sync")
    return {
        "enabled": state["enabled"],
        "interval_minutes": state["interval_minutes"],
    }
