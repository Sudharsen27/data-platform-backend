from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String

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
    owner_email = Column(String, nullable=False, default="")


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
    schema_fields = Column(String, nullable=False, default="")
    sla_hours = Column(Integer, nullable=False, default=24)
    contract_version = Column(String, nullable=False, default="1.0")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class DuplicateReview(Base):
    __tablename__ = "duplicate_reviews"

    id = Column(Integer, primary_key=True, index=True)
    left_id = Column(Integer, nullable=False, index=True)
    right_id = Column(Integer, nullable=False, index=True)
    status = Column(String, nullable=False, default="dismissed")
    reviewed_by = Column(String, nullable=False, default="system")
    note = Column(String, nullable=False, default="")
    confidence = Column(Float, nullable=False, default=0.0)
    reviewed_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"

    id = Column(Integer, primary_key=True, index=True)
    filename = Column(String, nullable=False, default="")
    status = Column(String, nullable=False, default="queued", index=True)
    total_rows = Column(Integer, nullable=False, default=0)
    processed_rows = Column(Integer, nullable=False, default=0)
    inserted_rows = Column(Integer, nullable=False, default=0)
    error_message = Column(String, nullable=False, default="")
    created_by = Column(String, nullable=False, default="system")
    target = Column(String, nullable=False, default="quarantine")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)


class RecordAnnotation(Base):
    __tablename__ = "record_annotations"

    id = Column(Integer, primary_key=True, index=True)
    record_id = Column(Integer, nullable=False, index=True)
    comment = Column(String, nullable=False, default="")
    status = Column(String, nullable=False, default="needs_review", index=True)
    created_by = Column(String, nullable=False, default="unknown")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class AnnotationHistory(Base):
    __tablename__ = "annotation_history"

    id = Column(Integer, primary_key=True, index=True)
    annotation_id = Column(Integer, nullable=False, index=True)
    action = Column(String, nullable=False, default="create")
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    acted_by = Column(String, nullable=False, default="unknown")
    acted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class GlossaryEntry(Base):
    """Approved or draft business glossary terms linked to catalog assets."""

    __tablename__ = "glossary_entries"

    id = Column(Integer, primary_key=True, index=True)
    catalog_asset_id = Column(Integer, nullable=False, index=True)
    field_name = Column(String, nullable=False, default="", index=True)
    title = Column(String, nullable=False, default="")
    definition = Column(String, nullable=False, default="")
    usage = Column(String, nullable=False, default="")
    governance_notes = Column(String, nullable=False, default="")
    examples = Column(String, nullable=False, default="[]")
    status = Column(String, nullable=False, default="draft", index=True)
    source_engine = Column(String, nullable=False, default="heuristics")
    created_by = Column(String, nullable=False, default="system")
    updated_by = Column(String, nullable=False, default="system")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class GlossaryHistory(Base):
    __tablename__ = "glossary_history"

    id = Column(Integer, primary_key=True, index=True)
    glossary_entry_id = Column(Integer, nullable=False, index=True)
    action = Column(String, nullable=False, default="create")
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    acted_by = Column(String, nullable=False, default="unknown")
    acted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class DatasetDocumentation(Base):
    """Approved or draft dataset documentation linked to catalog assets."""

    __tablename__ = "dataset_documentation"

    id = Column(Integer, primary_key=True, index=True)
    catalog_asset_id = Column(Integer, nullable=False, index=True)
    title = Column(String, nullable=False, default="")
    content = Column(String, nullable=False, default="{}")
    status = Column(String, nullable=False, default="draft", index=True)
    source_engine = Column(String, nullable=False, default="heuristics")
    created_by = Column(String, nullable=False, default="system")
    updated_by = Column(String, nullable=False, default="system")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class DatasetDocumentationHistory(Base):
    __tablename__ = "dataset_documentation_history"

    id = Column(Integer, primary_key=True, index=True)
    documentation_id = Column(Integer, nullable=False, index=True)
    action = Column(String, nullable=False, default="create")
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    acted_by = Column(String, nullable=False, default="unknown")
    acted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class RuleRecommendation(Base):
    """AI-generated rule suggestions pending steward approval."""

    __tablename__ = "rule_recommendations"

    id = Column(Integer, primary_key=True, index=True)
    catalog_asset_id = Column(Integer, nullable=False, index=True)
    field_name = Column(String, nullable=False, default="", index=True)
    rule_type = Column(String, nullable=False, default="governance")
    rule_text = Column(String, nullable=False, default="")
    confidence = Column(Integer, nullable=False, default=80)
    business_reason = Column(String, nullable=False, default="")
    governance_importance = Column(String, nullable=False, default="")
    compliance_impact = Column(String, nullable=False, default="")
    status = Column(String, nullable=False, default="pending", index=True)
    source_engine = Column(String, nullable=False, default="heuristics")
    approved_rule_id = Column(Integer, nullable=True, index=True)
    created_by = Column(String, nullable=False, default="system")
    updated_by = Column(String, nullable=False, default="system")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class RuleRecommendationHistory(Base):
    __tablename__ = "rule_recommendation_history"

    id = Column(Integer, primary_key=True, index=True)
    recommendation_id = Column(Integer, nullable=False, index=True)
    action = Column(String, nullable=False, default="create")
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    acted_by = Column(String, nullable=False, default="unknown")
    acted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class StewardshipRemediation(Base):
    """AI remediation analysis for stewardship queue records."""

    __tablename__ = "stewardship_remediations"

    id = Column(Integer, primary_key=True, index=True)
    stewardship_id = Column(Integer, nullable=False, index=True)
    content = Column(String, nullable=False, default="{}")
    status = Column(String, nullable=False, default="pending", index=True)
    assigned_owner = Column(String, nullable=False, default="")
    source_engine = Column(String, nullable=False, default="heuristics")
    created_by = Column(String, nullable=False, default="system")
    updated_by = Column(String, nullable=False, default="system")
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class StewardshipRemediationHistory(Base):
    __tablename__ = "stewardship_remediation_history"

    id = Column(Integer, primary_key=True, index=True)
    remediation_id = Column(Integer, nullable=False, index=True)
    action = Column(String, nullable=False, default="create")
    old_value = Column(String, nullable=False, default="")
    new_value = Column(String, nullable=False, default="")
    acted_by = Column(String, nullable=False, default="unknown")
    acted_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


__all__ = [
    "AICopilotActionLog",
    "CatalogAsset",
    "DatasetDocumentation",
    "DatasetDocumentationHistory",
    "DuplicateReview",
    "GlossaryEntry",
    "GlossaryHistory",
    "IngestionJob",
    "RecordAnnotation",
    "AnnotationHistory",
    "RuleRecommendation",
    "RuleRecommendationHistory",
    "AuditLog",
    "LineageEdge",
    "LineageNode",
    "MasterData",
    "PipelineRun",
    "QuarantineData",
    "Rule",
    "StewardshipQueue",
    "StewardshipRemediation",
    "StewardshipRemediationHistory",
    "SyncJob",
    "User",
]
