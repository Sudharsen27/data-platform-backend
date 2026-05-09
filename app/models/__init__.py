from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String

from app.database import Base
from app.models.user import User


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
    action = Column(String, nullable=False, default="unknown")
    entity = Column(String, nullable=False, default="")
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


class StewardshipQueue(Base):
    __tablename__ = "stewardship_queue"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, default="")
    issue = Column(String, nullable=False, default="")
    status = Column(String, nullable=False, default="pending", index=True)


class MasterData(Base):
    __tablename__ = "master_data"

    id = Column(Integer, primary_key=True, index=True)
    source_queue_id = Column(Integer, nullable=False, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class LineageNode(Base):
    __tablename__ = "lineage_nodes"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, nullable=False, unique=True, index=True)
    label = Column(String, nullable=False)
    node_type = Column(String, nullable=False, default="dataset")
    system = Column(String, nullable=False, default="")
    layer = Column(String, nullable=False, default="")


class LineageEdge(Base):
    __tablename__ = "lineage_edges"

    id = Column(Integer, primary_key=True, index=True)
    source_key = Column(String, nullable=False, index=True)
    target_key = Column(String, nullable=False, index=True)
    transformation = Column(String, nullable=False, default="")
    criticality = Column(String, nullable=False, default="medium")


class AICopilotActionLog(Base):
    __tablename__ = "ai_copilot_action_logs"

    id = Column(Integer, primary_key=True, index=True)
    action_key = Column(String, nullable=False, index=True)
    user_id = Column(String, nullable=False, default="unknown")
    status = Column(String, nullable=False, default="success")
    summary = Column(String, nullable=False, default="")
    payload = Column(String, nullable=False, default="")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class CatalogAsset(Base):
    """Registered data asset for discovery and governance (catalog)."""

    __tablename__ = "catalog_assets"

    id = Column(Integer, primary_key=True, index=True)
    asset_key = Column(String, nullable=False, unique=True, index=True)
    name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False, default="table")
    domain = Column(String, nullable=False, default="")
    owner_email = Column(String, nullable=False, default="")
    description = Column(String, nullable=False, default="")
    tags = Column(String, nullable=False, default="")
    pii_tier = Column(String, nullable=False, default="internal")
    lineage_node_key = Column(String, nullable=False, default="", index=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


__all__ = [
    "AICopilotActionLog",
    "CatalogAsset",
    "AuditLog",
    "LineageEdge",
    "LineageNode",
    "MasterData",
    "PipelineRun",
    "QuarantineData",
    "Rule",
    "StewardshipQueue",
    "SyncJob",
    "User",
]
