from datetime import datetime, timezone
from threading import Lock

from sqlalchemy.orm import Session

from app.models import PipelineRun, QuarantineData, Rule

_pipeline_state = {
    "status": "idle",
    "last_run_at": None,
    "last_message": "Pipeline has not run yet.",
    "total_records": 0,
    "processed_records": 0,
    "progress_percent": 0,
    "run_count": 0,
    "success_count": 0,
    "failed_count": 0,
}
_state_lock = Lock()


def _is_empty(value):
    return str(value or "").strip() == ""


def _apply_rule_to_field(field_value, field_name: str, rule_text: str):
    normalized_rule = str(rule_text or "").lower()

    if (
        "cannot be null" in normalized_rule
        or "cannot be empty" in normalized_rule
        or "required" in normalized_rule
    ):
        if _is_empty(field_value):
            return f"{field_name} {normalized_rule}"

    if field_name == "email" and "contain @" in normalized_rule:
        if "@" not in str(field_value or ""):
            return "email must contain @"

    return ""


def _validate_row_with_rules(row: QuarantineData, rules):
    errors = []

    for rule in rules:
        field_name = str(rule.field or "").strip().lower()
        if not field_name:
            continue

        field_value = getattr(row, field_name, "")
        message = _apply_rule_to_field(field_value, field_name, rule.rule or "")
        if message:
            errors.append(message)

    return ", ".join(errors)


def _compute_match_status(name: str, email: str):
    clean_name = str(name or "").strip()
    clean_email = str(email or "").strip().lower()

    is_email_valid = "@" in clean_email and "." in clean_email.split("@")[-1]
    if is_email_valid:
        return "merged"

    if clean_email or clean_name:
        return "review"

    return "new"


def get_pipeline_state():
    with _state_lock:
        return dict(_pipeline_state)


def run_pipeline(db: Session):
    with _state_lock:
        if _pipeline_state["status"] == "running":
            raise RuntimeError("Pipeline is already running.")
        _pipeline_state["status"] = "running"
        _pipeline_state["last_message"] = "Pipeline execution in progress."
        _pipeline_state["total_records"] = 0
        _pipeline_state["processed_records"] = 0
        _pipeline_state["progress_percent"] = 0

    try:
        pipeline_run = PipelineRun(
            status="running",
            records_processed=0,
            start_time=datetime.utcnow(),
        )
        db.add(pipeline_run)
        db.commit()
        db.refresh(pipeline_run)

        rules = db.query(Rule).filter(Rule.status == "active").all()
        total_records = db.query(QuarantineData).count()
        batch_size = 5000
        processed_records = 0

        with _state_lock:
            _pipeline_state["total_records"] = total_records

        merged_count = 0
        review_count = 0
        new_count = 0

        for offset in range(0, total_records, batch_size):
            records = (
                db.query(QuarantineData)
                .order_by(QuarantineData.id.asc())
                .offset(offset)
                .limit(batch_size)
                .all()
            )

            for record in records:
                transformed_error = _validate_row_with_rules(record, rules)
                match_status = _compute_match_status(record.name, record.email)

                record.error = transformed_error
                record.match_status = match_status

                if match_status == "merged":
                    merged_count += 1
                elif match_status == "review":
                    review_count += 1
                else:
                    new_count += 1

            db.commit()
            processed_records += len(records)
            pipeline_run.records_processed = processed_records
            db.commit()

            with _state_lock:
                _pipeline_state["processed_records"] = processed_records
                _pipeline_state["progress_percent"] = (
                    int((processed_records / total_records) * 100)
                    if total_records > 0
                    else 100
                )

        now = datetime.now(timezone.utc).isoformat()
        summary = {
            "total_records": total_records,
            "merged": merged_count,
            "review": review_count,
            "new": new_count,
            "status": "success",
            "message": "Pipeline completed successfully.",
            "ran_at": now,
        }

        with _state_lock:
            _pipeline_state["status"] = "success"
            _pipeline_state["last_run_at"] = now
            _pipeline_state["last_message"] = summary["message"]
            _pipeline_state["processed_records"] = total_records
            _pipeline_state["progress_percent"] = 100
            _pipeline_state["run_count"] += 1
            _pipeline_state["success_count"] += 1

            summary["run_count"] = _pipeline_state["run_count"]
            summary["success_count"] = _pipeline_state["success_count"]
            summary["failed_count"] = _pipeline_state["failed_count"]

        pipeline_run.status = "success"
        pipeline_run.records_processed = total_records
        pipeline_run.end_time = datetime.utcnow()
        db.commit()

        return summary
    except Exception as error:
        db.rollback()
        if "pipeline_run" in locals() and pipeline_run.id:
            pipeline_run.status = "failed"
            pipeline_run.end_time = datetime.utcnow()
            db.add(pipeline_run)
            db.commit()
        with _state_lock:
            _pipeline_state["status"] = "failed"
            _pipeline_state["last_message"] = str(error)
            _pipeline_state["run_count"] += 1
            _pipeline_state["failed_count"] += 1
        raise
