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


class DashboardKpiOut(BaseModel):
    key: str
    title: str
    value: str
    delta: str
    delta_positive: bool | None = None
    delta_label: str = "vs last 7 days"
    href: str = ""


class DashboardAlertOut(BaseModel):
    name: str
    detail: str = ""
    severity: str
    href: str = ""


class ComplianceCheckOut(BaseModel):
    key: str
    label: str
    status: str
    status_label: str
    detail: str = ""
    href: str = ""


class ComplianceStatusOut(BaseModel):
    overall_percent: int
    checks: list[ComplianceCheckOut]


class DashboardTrendPointOut(BaseModel):
    day: str
    date: str
    processed: int = 0
    successful_jobs: int = 0
    failed_jobs: int = 0


class ErrorDistributionPointOut(BaseModel):
    type: str
    count: int


class DashboardTrendsOut(BaseModel):
    records_trend: list[DashboardTrendPointOut]
    error_distribution: list[ErrorDistributionPointOut]


class AuditActivityOut(BaseModel):
    id: int
    user_id: str
    action: str
    entity: str
    summary: str
    timestamp: datetime
    href: str = ""
    category: str = "governance"

    class Config:
        from_attributes = True


class SlaWidgetOut(BaseModel):
    key: str
    label: str
    status: str
    status_label: str
    metric: str
    detail: str = ""
    href: str = ""
    sla_target: str = ""


class SlaStatusOut(BaseModel):
    overall_status: str
    overall_label: str
    widgets: list[SlaWidgetOut]


class DashboardOverviewOut(BaseModel):
    kpi_summary: dict
    kpi_cards: list[DashboardKpiOut]
    alerts: list[DashboardAlertOut]
    compliance: ComplianceStatusOut
    trends: DashboardTrendsOut
    audit_activity: list[AuditActivityOut]
    sla: SlaStatusOut
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
