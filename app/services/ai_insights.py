from app.models import StewardshipQueue, SyncJob


def build_ai_insights(db, analytics: dict | None = None):
    if analytics:
        quarantine_total = int(analytics.get("total_records") or 0)
        quarantine_with_error = int(analytics.get("failed_records") or 0)
    else:
        from app.models import QuarantineData

        quarantine_total = db.query(QuarantineData).count()
        quarantine_with_error = (
            db.query(QuarantineData)
            .filter(QuarantineData.error.isnot(None), QuarantineData.error != "")
            .count()
        )
    failed_jobs = db.query(SyncJob).filter(SyncJob.status == "failed").count()
    pending_stewardship = db.query(StewardshipQueue).filter(StewardshipQueue.status == "pending").count()

    issue_rate = 0.0
    if quarantine_total > 0:
        issue_rate = round((quarantine_with_error / quarantine_total) * 100, 1)

    return [
        {
            "title": "Anomaly Detection",
            "detail": f"Current quarantine issue rate is {issue_rate}% across {quarantine_total} records.",
            "priority": "high" if issue_rate >= 25 else "medium",
        },
        {
            "title": "Lineage Impact Prediction",
            "detail": "Upstream CRM source changes may impact staging, master, and analytics lineage nodes.",
            "priority": "medium",
        },
        {
            "title": "Operational Risk",
            "detail": f"{failed_jobs} failed jobs and {pending_stewardship} pending stewardship tasks need review.",
            "priority": "high" if failed_jobs > 0 else "medium",
        },
    ]
