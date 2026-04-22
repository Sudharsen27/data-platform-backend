from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from app.database import Base


class QuarantineData(Base):
    __tablename__ = "quarantine_data"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, default="")
    error = Column(String, nullable=False, default="")
    match_status = Column(String, nullable=False, default="new")


class Rule(Base):
    __tablename__ = "rules"

    id = Column(Integer, primary_key=True, index=True)
    field = Column(String, nullable=False)
    rule = Column(String, nullable=False)
    status = Column(String, nullable=False, default="active")
    created_by = Column(String, nullable=False, default="system")
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class SyncJob(Base):
    __tablename__ = "sync_jobs"

    id = Column(Integer, primary_key=True, index=True)
    status = Column(String, nullable=False, default="running")
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    quarantine_rows_synced = Column(Integer, nullable=False, default=0)
    rules_synced = Column(Integer, nullable=False, default=0)
    error_message = Column(String, nullable=True)
    triggered_by = Column(String, nullable=False, default="manual")


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False, default="unknown")
    field_changed = Column(String, nullable=False)
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, index=True)
    status = Column(String, nullable=False, default="running")
    records_processed = Column(Integer, nullable=False, default=0)
    start_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
