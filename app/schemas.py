from datetime import datetime

from pydantic import BaseModel, model_validator


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


class RuleValidateIn(BaseModel):
    name: str = ""
    email: str = ""


class RuleViolationOut(BaseModel):
    field: str
    message: str
    rule_id: int | None = None
    rule_text: str = ""


class RuleValidateOut(BaseModel):
    violations: list[RuleViolationOut]
    error: str
    active_rules: int


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


class SchedulerJobConfigRequest(BaseModel):
    job_type: str
    enabled: bool
    trigger_type: str = "interval"
    interval_minutes: int | None = 10
    cron_expression: str | None = None

    @model_validator(mode="after")
    def validate_trigger_fields(self):
        if self.job_type not in ("snowflake_sync", "pipeline"):
            raise ValueError("job_type must be snowflake_sync or pipeline")
        if self.trigger_type not in ("interval", "cron"):
            raise ValueError("trigger_type must be interval or cron")
        if not self.enabled:
            return self
        if self.trigger_type == "cron" and not (self.cron_expression or "").strip():
            raise ValueError("cron_expression is required when trigger_type is cron")
        if self.trigger_type == "interval":
            if self.interval_minutes is None or self.interval_minutes < 1:
                raise ValueError("interval_minutes must be at least 1")
        return self


class SchedulerJobStateOut(BaseModel):
    job_type: str
    label: str
    enabled: bool
    trigger_type: str | None = None
    interval_minutes: int | None = None
    cron_expression: str | None = None
    next_run_at: str | None = None


class SchedulerOverviewOut(BaseModel):
    jobs: list[SchedulerJobStateOut]


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
    owner_email: str = ""

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


class AnnotationCreateIn(BaseModel):
    record_id: int
    comment: str
    status: str = "needs_review"


class AnnotationUpdateIn(BaseModel):
    comment: str
    status: str


class AnnotationOut(BaseModel):
    id: int
    record_id: int
    comment: str
    status: str
    created_by: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AnnotationHistoryOut(BaseModel):
    id: int
    annotation_id: int
    action: str
    old_value: str
    new_value: str
    acted_by: str
    acted_at: datetime

    class Config:
        from_attributes = True


class AnnotationListOut(BaseModel):
    items: list[AnnotationOut]
    total: int
    offset: int
    limit: int


class MasterDataOut(BaseModel):
    id: int
    source_queue_id: int
    name: str
    email: str
    created_at: datetime

    class Config:
        from_attributes = True


class MasterDataPageOut(BaseModel):
    items: list[MasterDataOut]
    total: int
    offset: int
    limit: int


class MasterDataCompareOut(BaseModel):
    source_queue_id: int
    stewardship: StewardshipOut
    quarantine: QuarantineOut | None = None
    golden: MasterDataOut | None = None
    is_published: bool


class DuplicateCandidateOut(BaseModel):
    left_id: int
    right_id: int
    left_name: str
    right_name: str
    left_email: str
    right_email: str
    confidence: float
    reason: str


class DuplicateCandidatesOut(BaseModel):
    items: list[DuplicateCandidateOut]
    total: int


class DuplicateMergeRequest(BaseModel):
    left_id: int
    right_id: int
    survivor_id: int | None = None


class DuplicateRejectRequest(BaseModel):
    left_id: int
    right_id: int
    note: str = ""


class IngestionJobOut(BaseModel):
    id: int
    filename: str
    status: str
    target: str = "quarantine"
    total_rows: int
    processed_rows: int
    inserted_rows: int
    error_message: str = ""
    created_by: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None

    class Config:
        from_attributes = True


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


class AIStatusOut(BaseModel):
    enabled: bool
    provider: str
    model: str = ""
    available: bool
    mode: str = "heuristics"


class ExplainQuarantineIn(BaseModel):
    name: str = ""
    email: str = ""
    error: str = ""


class ExplainQuarantineOut(BaseModel):
    explanation: str
    source: str


class AISuggestStewardshipIn(BaseModel):
    """Assign stewards to selected pending tasks, or all pending when assign_all_pending is true."""

    ids: list[int] = []
    assign_all_pending: bool = False


class AICopilotActionLogOut(BaseModel):
    id: int
    action_key: str
    user_id: str
    status: str
    summary: str
    payload: dict = {}
    created_at: datetime

    class Config:
        from_attributes = True


class AICopilotActionLogsPageOut(BaseModel):
    items: list[AICopilotActionLogOut]
    total: int
    offset: int
    limit: int


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
    schema_fields: str = ""
    sla_hours: int = 24
    contract_version: str = "1.0"


class CatalogAssetCreate(CatalogAssetBase):
    pass


class CatalogAssetOut(CatalogAssetBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class LineageImpactOut(BaseModel):
    anchor_node_key: str | None = None
    field: str | None = None
    affected_node_keys: list[str]
    nodes: list[LineageNodeOut]
    edges: list[LineageEdgeOut]
    catalog_assets: list[CatalogAssetOut]
    summary: str
