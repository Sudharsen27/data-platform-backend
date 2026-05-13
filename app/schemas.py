from datetime import datetime

from pydantic import BaseModel


class QuarantineBase(BaseModel):
    name: str
    email: str
    error: str = ""


class QuarantineUpdate(QuarantineBase):
    id: int


class QuarantineOut(QuarantineBase):
    id: int
    match_status: str = "new"

    class Config:
        from_attributes = True


class QuarantinePageOut(BaseModel):
    items: list[QuarantineOut]
    total: int
    offset: int
    limit: int


class QuarantineBulkImport(BaseModel):
    """Import sample or production-like bad rows into quarantine (portfolio / demos)."""

    rows: list[QuarantineBase]


class RuleBase(BaseModel):
    field: str
    rule: str
    status: str = "active"
    created_by: str = "system"


class RuleCreate(RuleBase):
    pass


class RuleUpdate(RuleBase):
    id: int


class RuleOut(RuleBase):
    id: int
    updated_at: datetime

    class Config:
        from_attributes = True


class SyncJobOut(BaseModel):
    id: int
    status: str
    start_time: datetime
    end_time: datetime | None = None
    quarantine_rows_synced: int
    rules_synced: int
    error_message: str | None = None
    triggered_by: str

    class Config:
        from_attributes = True


class SchedulerToggleRequest(BaseModel):
    enabled: bool
    interval_minutes: int = 10


class AuditLogOut(BaseModel):
    id: int
    user_id: str
    action: str
    entity: str
    old_value: str
    new_value: str
    timestamp: datetime

    class Config:
        from_attributes = True


class PipelineRunOut(BaseModel):
    id: int
    status: str
    records_processed: int
    start_time: datetime
    end_time: datetime | None = None

    class Config:
        from_attributes = True


class StewardshipOut(BaseModel):
    id: int
    name: str
    email: str
    issue: str
    status: str

    class Config:
        from_attributes = True


class StewardshipPageOut(BaseModel):
    items: list[StewardshipOut]
    total: int
    offset: int
    limit: int
    pending_total: int


class StewardshipActionRequest(BaseModel):
    id: int


class StewardshipBulkActionRequest(BaseModel):
    ids: list[int]


class StewardshipBulkOutcome(BaseModel):
    """Result of bulk approve or reject."""

    success_count: int
    skipped_not_pending: int
    missing_count: int


class LineageNodeOut(BaseModel):
    id: int
    key: str
    label: str
    node_type: str
    system: str
    layer: str

    class Config:
        from_attributes = True


class LineageEdgeOut(BaseModel):
    id: int
    source_key: str
    target_key: str
    transformation: str
    criticality: str

    class Config:
        from_attributes = True


class LineageGraphOut(BaseModel):
    nodes: list[LineageNodeOut]
    edges: list[LineageEdgeOut]


class AICopilotInsightOut(BaseModel):
    title: str
    detail: str
    priority: str


class AICopilotInsightsResponse(BaseModel):
    items: list[AICopilotInsightOut]


class AICopilotActionResponse(BaseModel):
    action: str
    summary: str
    details: list[str] = []


class DashboardOverviewOut(BaseModel):
    kpis: dict
    last_sync_job: dict | None = None
    pipeline_status: dict
    recent_jobs: list[SyncJobOut]
    lineage: LineageGraphOut
    stewardship: list[StewardshipOut]
    ai_insights: list[AICopilotInsightOut]


class CatalogAssetBase(BaseModel):
    asset_key: str
    name: str
    asset_type: str = "table"
    domain: str = ""
    owner_email: str = ""
    description: str = ""
    tags: str = ""
    pii_tier: str = "internal"
    lineage_node_key: str = ""


class CatalogAssetCreate(CatalogAssetBase):
    pass


class CatalogAssetOut(CatalogAssetBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
