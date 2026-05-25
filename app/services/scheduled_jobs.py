from sqlalchemy.orm import sessionmaker

from app.services.audit_log import write_audit_log
from app.services.pipeline import get_pipeline_state, run_pipeline
from app.services.sync_jobs import run_sync_job


def run_scheduled_sync_job(session_factory: sessionmaker):
    db = session_factory()
    try:
        run_sync_job(db, triggered_by="scheduler")
    finally:
        db.close()


def run_scheduled_pipeline_job(session_factory: sessionmaker):
    db = session_factory()
    try:
        if get_pipeline_state()["status"] == "running":
            return

        result = run_pipeline(db)
        write_audit_log(
            db,
            user_id="scheduler",
            action="pipeline_run",
            entity="pipeline",
            old_value="",
            new_value=str(result)[:2000],
        )
        db.commit()
    finally:
        db.close()
