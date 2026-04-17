from apscheduler.schedulers.background import BackgroundScheduler


scheduler = BackgroundScheduler()
SYNC_JOB_ID = "snowflake_sync_job"


def ensure_scheduler_started():
    if not scheduler.running:
        scheduler.start()


def configure_sync_schedule(sync_callback, interval_minutes: int):
    ensure_scheduler_started()

    if scheduler.get_job(SYNC_JOB_ID):
        scheduler.remove_job(SYNC_JOB_ID)

    scheduler.add_job(
        sync_callback,
        trigger="interval",
        minutes=interval_minutes,
        id=SYNC_JOB_ID,
        replace_existing=True,
    )


def disable_sync_schedule():
    if scheduler.get_job(SYNC_JOB_ID):
        scheduler.remove_job(SYNC_JOB_ID)


def get_scheduler_state():
    job = scheduler.get_job(SYNC_JOB_ID)
    if not job:
        return {"enabled": False, "interval_minutes": None}

    interval = job.trigger.interval
    interval_minutes = int(interval.total_seconds() / 60)
    return {"enabled": True, "interval_minutes": interval_minutes}
