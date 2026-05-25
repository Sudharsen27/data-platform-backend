from datetime import datetime

from sqlalchemy.orm import Session

from app.models import SyncJob
from app.services.snowflake_sync import sync_postgres_to_snowflake


def run_sync_job(db: Session, triggered_by: str = "manual"):
    sync_job = SyncJob(
        status="running",
        start_time=datetime.utcnow(),
        triggered_by=triggered_by,
    )
    db.add(sync_job)
    db.commit()
    db.refresh(sync_job)

    try:
        result = sync_postgres_to_snowflake(db)
        sync_job.status = "success"
        sync_job.end_time = datetime.utcnow()
        sync_job.quarantine_rows_synced = result["quarantine_rows_synced"]
        sync_job.rules_synced = result["rules_synced"]
        db.commit()
        db.refresh(sync_job)
        return result | {"job_id": sync_job.id}
    except Exception as error:
        sync_job.status = "failed"
        sync_job.end_time = datetime.utcnow()
        sync_job.error_message = str(error)
        db.commit()
        db.refresh(sync_job)
        raise
