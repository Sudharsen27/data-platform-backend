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


class AICopilotPageContextIn(BaseModel):
    page: str = ""
    pathname: str = ""
    page_label: str = ""
    asset_id: int | str | None = None
    asset_name: str = ""
    asset_key: str = ""
    node_key: str = ""
    node_label: str = ""
    rule_id: int | str | None = None
    rule_field: str = ""
    rule_text: str = ""
    stewardship_id: int | str | None = None
    task_name: str = ""
    task_issue: str = ""
    master_id: int | str | None = None
    record_name: str = ""
    record_email: str = ""


class AICopilotChatIn(BaseModel):
    question: str
    page_context: AICopilotPageContextIn | None = None


class AICopilotSourceOut(BaseModel):
    type: str
    id: str
    label: str


class AICopilotChatOut(BaseModel):
    answer: str
    sources: list[AICopilotSourceOut] = []


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


class LineageImpactAssetSummary(BaseModel):
    id: int | None = None
    asset_key: str = ""
    name: str = ""
    asset_type: str = "table"
    domain: str = ""
    lineage_node_key: str = ""
    layer: str = ""
    system: str = ""
    impact_level: str = "medium"


class LineageImpactRuleSummary(BaseModel):
    id: int
    field: str
    rule: str
    status: str


class LineageImpactMasterSummary(BaseModel):
    id: int
    name: str
    email: str = ""
    source_queue_id: int


class LineageImpactScoreOut(BaseModel):
    source_asset: str
    source_asset_id: int | None = None
    source_asset_key: str | None = None
    anchor_node_key: str | None = None
    field: str | None = None
    impact_score: int
    downstream_count: int
    upstream_count: int
    datasets_impacted: int
    rules_impacted: int
    reports_impacted: int
    master_data_impacted: int
    downstream_assets: list[LineageImpactAssetSummary]
    upstream_assets: list[LineageImpactAssetSummary]
    critical_dependencies: list[LineageImpactAssetSummary]
    impacted_rules: list[LineageImpactRuleSummary] = []
    impacted_reports: list[LineageImpactAssetSummary] = []
    impacted_master_data: list[LineageImpactMasterSummary] = []
    summary: str = ""


class LineageImpactAnalyzeIn(BaseModel):
    question: str
    asset_id: int | None = None
    node_key: str = ""
    field: str = ""


class LineageImpactItemOut(BaseModel):
    name: str
    type: str = "dataset"
    impact_level: str = "medium"
    system: str = ""
    layer: str = ""


class LineageImpactAnalyzeOut(BaseModel):
    analysis: str
    impacts: list[LineageImpactItemOut]
    impact_score: int = 0
    downstream_count: int = 0
    upstream_count: int = 0
    source_engine: str = "heuristics"
    impact_detail: LineageImpactScoreOut | None = None


class ClassificationAnalyzeFieldIn(BaseModel):
    field_name: str
    dataset_id: int | None = None
    dataset_name: str = ""
    description: str = ""
    tags: str = ""


class ClassificationFieldOut(BaseModel):
    field_name: str
    classification: str
    confidence: int
    reason: str
    recommendations: list[str] = []
    ai_explanation: str = ""
    source_engine: str = "heuristics"


class ClassificationAnalyzeDatasetIn(BaseModel):
    dataset_id: int


class ClassificationFieldSummary(BaseModel):
    field_name: str
    classification: str
    confidence: int
    reason: str
    recommendations: list[str] = []


class ClassificationDatasetOut(BaseModel):
    dataset_id: int
    dataset_key: str
    dataset_name: str
    dataset_classification: str
    registered_pii_tier: str = ""
    risk_score: int
    field_count: int
    pii_count: int
    sensitive_count: int
    financial_count: int = 0
    confidential_count: int = 0
    public_count: int = 0
    pii_fields: list[ClassificationFieldSummary]
    sensitive_fields: list[ClassificationFieldSummary]
    financial_fields: list[ClassificationFieldSummary] = []
    confidential_fields: list[ClassificationFieldSummary] = []
    public_fields: list[ClassificationFieldSummary] = []
    all_fields: list[ClassificationFieldSummary] = []
    recommendations: list[str]
    summary: str = ""
    ai_summary: str = ""
    source_engine: str = "heuristics"


class GlossaryGenerateFieldIn(BaseModel):
    field_name: str
    dataset_id: int | None = None
    dataset_name: str = ""
    field_type: str = ""
    description: str = ""
    tags: str = ""


class GlossaryFieldOut(BaseModel):
    field_name: str = ""
    title: str
    definition: str
    usage: str
    governance_notes: str
    examples: list[str] = []
    source_engine: str = "heuristics"


class GlossaryGenerateDatasetIn(BaseModel):
    dataset_id: int


class GlossaryFieldBriefOut(BaseModel):
    field_name: str
    title: str
    definition: str
    usage: str
    governance_notes: str
    examples: list[str] = []
    source_engine: str = "heuristics"


class GlossaryDatasetOut(BaseModel):
    dataset_id: int
    dataset_key: str = ""
    dataset_title: str
    dataset_definition: str
    business_usage: str
    governance_notes: str = ""
    field_glossaries: list[GlossaryFieldBriefOut] = []
    source_engine: str = "heuristics"


class GlossarySaveIn(BaseModel):
    catalog_asset_id: int
    field_name: str = ""
    title: str
    definition: str
    usage: str = ""
    governance_notes: str = ""
    examples: list[str] = []
    status: str = "approved"


class GlossaryUpdateIn(BaseModel):
    title: str | None = None
    definition: str | None = None
    usage: str | None = None
    governance_notes: str | None = None
    examples: list[str] | None = None
    status: str | None = None


class GlossaryEntryOut(BaseModel):
    id: int
    catalog_asset_id: int
    field_name: str
    title: str
    definition: str
    usage: str
    governance_notes: str
    examples: list[str] = []
    status: str
    source_engine: str
    created_by: str
    updated_by: str
    created_at: datetime
    updated_at: datetime


class GlossaryHistoryOut(BaseModel):
    id: int
    glossary_entry_id: int
    action: str
    old_value: str
    new_value: str
    acted_by: str
    acted_at: datetime


class GlossaryEntriesPageOut(BaseModel):
    items: list[GlossaryEntryOut]
    total: int


class DocumentationKeyFieldOut(BaseModel):
    field_name: str
    description: str = ""
    classification: str = "Unknown"


class DocumentationGenerateIn(BaseModel):
    dataset_id: int


class DocumentationOut(BaseModel):
    dataset_id: int
    dataset_key: str = ""
    title: str
    summary: str
    business_description: str = ""
    purpose: str
    key_fields: list[DocumentationKeyFieldOut] = []
    owner_recommendation: str = ""
    governance_notes: str
    classification_summary: str = ""
    quality_expectations: str = ""
    usage_guidelines: str
    compliance_considerations: str = ""
    source_engine: str = "heuristics"


class DocumentationSaveIn(BaseModel):
    catalog_asset_id: int
    title: str
    summary: str = ""
    business_description: str = ""
    purpose: str = ""
    key_fields: list[DocumentationKeyFieldOut] = []
    owner_recommendation: str = ""
    governance_notes: str = ""
    classification_summary: str = ""
    quality_expectations: str = ""
    usage_guidelines: str = ""
    compliance_considerations: str = ""
    status: str = "approved"


class DocumentationUpdateIn(BaseModel):
    title: str | None = None
    summary: str | None = None
    business_description: str | None = None
    purpose: str | None = None
    key_fields: list[DocumentationKeyFieldOut] | None = None
    owner_recommendation: str | None = None
    governance_notes: str | None = None
    classification_summary: str | None = None
    quality_expectations: str | None = None
    usage_guidelines: str | None = None
    compliance_considerations: str | None = None
    status: str | None = None


class DocumentationEntryOut(DocumentationOut):
    id: int
    catalog_asset_id: int
    status: str
    created_by: str
    updated_by: str
    created_at: datetime
    updated_at: datetime


class DocumentationHistoryOut(BaseModel):
    id: int
    documentation_id: int
    action: str
    old_value: str
    new_value: str
    acted_by: str
    acted_at: datetime


class DocumentationExportOut(BaseModel):
    format: str
    filename: str
    content: str
    content_type: str


class RuleRecommendIn(BaseModel):
    dataset_id: int


class RuleRecommendFieldIn(BaseModel):
    field_name: str
    dataset_id: int | None = None


class RecommendedRuleOut(BaseModel):
    field_name: str
    rule_type: str
    rule_text: str
    confidence: int
    business_reason: str = ""
    governance_importance: str = ""
    compliance_impact: str = ""
    source_engine: str = "heuristics"


class RuleRiskAnalysisOut(BaseModel):
    data_quality_risk_score: int
    data_quality_risk_level: str
    governance_risk_score: int
    governance_risk_level: str
    compliance_risk_score: int
    compliance_risk_level: str
    missing_rule_gaps: int = 0


class RuleRecommendFieldOut(BaseModel):
    dataset_id: int | None = None
    field_name: str
    classification: str = ""
    rules: list[RecommendedRuleOut]
    risk_analysis: RuleRiskAnalysisOut
    source_engine: str = "heuristics"


class RuleRecommendDatasetOut(BaseModel):
    dataset_id: int
    dataset_key: str = ""
    dataset_name: str
    recommended_rules: list[RecommendedRuleOut]
    risk_analysis: RuleRiskAnalysisOut
    source_engine: str = "heuristics"


class RuleRecommendationOut(BaseModel):
    id: int
    catalog_asset_id: int
    field_name: str
    rule_type: str
    rule_text: str
    confidence: int
    business_reason: str
    governance_importance: str
    compliance_impact: str
    status: str
    source_engine: str
    approved_rule_id: int | None = None
    created_by: str
    updated_by: str
    created_at: datetime
    updated_at: datetime


class RuleRecommendationUpdateIn(BaseModel):
    rule_text: str | None = None
    rule_type: str | None = None
    confidence: int | None = None
    business_reason: str | None = None
    governance_importance: str | None = None
    compliance_impact: str | None = None


class RuleRecommendationHistoryOut(BaseModel):
    id: int
    recommendation_id: int
    action: str
    old_value: str
    new_value: str
    acted_by: str
    acted_at: datetime


class RuleRecommendationsPageOut(BaseModel):
    items: list[RuleRecommendationOut]
    total: int


class StewardshipExplainIn(BaseModel):
    stewardship_id: int


class StewardshipRemediateIn(BaseModel):
    stewardship_id: int


class StewardshipRemediationOut(BaseModel):
    stewardship_id: int
    failure_explanation: str
    root_cause: str
    suggested_fix: str = ""
    business_impact: str = ""
    governance_impact: str = ""
    risk_score: int = 0
    risk_level: str = "Low Risk"
    suggested_actions: list[str] = []
    context: dict = {}
    source_engine: str = "heuristics"


class StewardshipExplainOut(BaseModel):
    stewardship_id: int
    explanation: str
    root_cause: str = ""
    source_engine: str = "heuristics"


class StewardshipRemediationEntryOut(StewardshipRemediationOut):
    id: int
    status: str
    assigned_owner: str = ""
    created_by: str
    updated_by: str
    created_at: datetime
    updated_at: datetime


class StewardshipRemediationAssignIn(BaseModel):
    owner_email: str


class StewardshipRemediationHistoryOut(BaseModel):
    id: int
    remediation_id: int
    action: str
    old_value: str
    new_value: str
    acted_by: str
    acted_at: datetime


class GovernanceDimensionDetailOut(BaseModel):
    key: str
    label: str
    score: int
    weight_percent: int = 0


class GovernanceDatasetSummaryOut(BaseModel):
    dataset_id: int
    dataset_name: str
    overall_score: int
    domain: str = ""
    risk_level: str = "Medium Risk"


class GovernanceDatasetAttentionOut(BaseModel):
    dataset_id: int
    dataset_name: str
    overall_score: int
    missing_governance_areas: list[str] = []


class GovernanceDomainScoreOut(BaseModel):
    domain: str
    dataset_count: int
    overall_score: int
    risk_score: int
    risk_level: str
    dimensions: dict[str, int]
    datasets: list[dict] = []


class GovernanceDatasetScoreOut(BaseModel):
    scope: str = "dataset"
    dataset_id: int
    dataset_key: str
    dataset_name: str
    domain: str = ""
    overall_score: int
    risk_score: int
    risk_level: str
    dimensions: dict[str, int]
    dimension_details: list[GovernanceDimensionDetailOut]
    missing_governance_areas: list[str] = []
    recommendations: list[str] = []
    field_count: int = 0
    generated_at: str = ""


class GovernancePlatformScoreOut(BaseModel):
    scope: str = "platform"
    overall_score: int
    risk_score: int
    risk_level: str
    dataset_count: int
    domain_count: int
    dimensions: dict[str, int]
    dimension_details: list[GovernanceDimensionDetailOut]
    missing_governance_areas: list[str] = []
    recommendations: list[str] = []
    domains: list[GovernanceDomainScoreOut] = []
    datasets: list[GovernanceDatasetSummaryOut] = []
    datasets_needing_attention: list[GovernanceDatasetAttentionOut] = []
    generated_at: str = ""
