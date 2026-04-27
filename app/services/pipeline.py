from datetime import datetime, timezone
from threading import Lock
import copy

from sqlalchemy.orm import Session

from app.models import PipelineRun, QuarantineData, Rule, StewardshipQueue


def _default_steps():
    return [
        {"key": "ingest", "label": "Ingest", "status": "pending", "count": 0},
        {"key": "validation", "label": "Validate", "status": "pending", "count": 0},
        {"key": "matching", "label": "Match", "status": "pending", "count": 0},
        {"key": "stewardship", "label": "Review", "status": "pending", "count": 0},
        {"key": "golden", "label": "Golden", "status": "pending", "count": 0},
    ]


def _set_step_status(steps, key: str, status: str):
    for step in steps:
        if step["key"] == key:
            step["status"] = status
            break


def _set_step_count(steps, key: str, count: int):
    for step in steps:
        if step["key"] == key:
            step["count"] = count
            break


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
    "steps": _default_steps(),
    "stage_counts": {
        "ingest": 0,
        "validation": 0,
        "matching": 0,
        "stewardship": 0,
        "golden": 0,
    },
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


def _compute_match_confidence(name: str, email: str, error: str):
    score = 40
    clean_name = str(name or "").strip()
    clean_email = str(email or "").strip().lower()
    clean_error = str(error or "").strip()

    if clean_name:
        score += 20

    if clean_email:
        score += 10

    is_email_valid = "@" in clean_email and "." in clean_email.split("@")[-1]
    if is_email_valid:
        score += 30
    elif clean_email:
        score += 10

    if clean_error:
        score -= 15

    return max(0, min(100, score))


def get_pipeline_state():
    with _state_lock:
        return copy.deepcopy(_pipeline_state)


def run_pipeline(db: Session):
    steps = _default_steps()
    with _state_lock:
        if _pipeline_state["status"] == "running":
            raise RuntimeError("Pipeline is already running.")
        _pipeline_state["status"] = "running"
        _pipeline_state["last_message"] = "Pipeline execution in progress."
        _pipeline_state["total_records"] = 0
        _pipeline_state["processed_records"] = 0
        _pipeline_state["progress_percent"] = 0
        _pipeline_state["steps"] = steps
        _pipeline_state["stage_counts"] = {
            "ingest": 0,
            "validation": 0,
            "matching": 0,
            "stewardship": 0,
            "golden": 0,
        }
        _set_step_status(_pipeline_state["steps"], "ingest", "running")

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
            _pipeline_state["stage_counts"]["ingest"] = total_records
            _set_step_count(_pipeline_state["steps"], "ingest", total_records)
            _set_step_status(_pipeline_state["steps"], "ingest", "completed")
            _set_step_status(_pipeline_state["steps"], "validation", "running")
            _set_step_status(_pipeline_state["steps"], "matching", "running")
            _set_step_status(_pipeline_state["steps"], "stewardship", "running")
            _set_step_status(_pipeline_state["steps"], "golden", "running")

        merged_count = 0
        review_count = 0
        new_count = 0
        stewardship_count = 0

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
                match_confidence = _compute_match_confidence(
                    record.name, record.email, transformed_error
                )

                record.error = transformed_error
                final_status = match_status

                if 50 <= match_confidence <= 80:
                    existing_item = (
                        db.query(StewardshipQueue)
                        .filter(StewardshipQueue.id == record.id)
                        .first()
                    )
                    if not existing_item:
                        db.add(
                            StewardshipQueue(
                                id=record.id,
                                name=record.name,
                                email=record.email,
                                issue=transformed_error or "Review needed for moderate confidence match.",
                                status="pending",
                            )
                        )
                    else:
                        existing_item.name = record.name
                        existing_item.email = record.email
                        existing_item.issue = (
                            transformed_error
                            or "Review needed for moderate confidence match."
                        )
                        existing_item.status = "pending"
                    stewardship_count += 1
                    final_status = "stewardship"

                record.match_status = final_status

                if final_status == "merged":
                    merged_count += 1
                elif final_status == "review":
                    review_count += 1
                elif final_status == "new":
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
                _pipeline_state["stage_counts"]["validation"] = processed_records
                _pipeline_state["stage_counts"]["matching"] = processed_records
                _pipeline_state["stage_counts"]["stewardship"] = stewardship_count
                _pipeline_state["stage_counts"]["golden"] = merged_count
                _set_step_count(_pipeline_state["steps"], "validation", processed_records)
                _set_step_count(_pipeline_state["steps"], "matching", processed_records)
                _set_step_count(_pipeline_state["steps"], "stewardship", stewardship_count)
                _set_step_count(_pipeline_state["steps"], "golden", merged_count)

        now = datetime.now(timezone.utc).isoformat()
        summary = {
            "total_records": total_records,
            "merged": merged_count,
            "review": review_count,
            "new": new_count,
            "stewardship": stewardship_count,
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
            _set_step_status(_pipeline_state["steps"], "validation", "completed")
            _set_step_status(_pipeline_state["steps"], "matching", "completed")
            _set_step_status(_pipeline_state["steps"], "stewardship", "completed")
            _set_step_status(_pipeline_state["steps"], "golden", "completed")

            summary["run_count"] = _pipeline_state["run_count"]
            summary["success_count"] = _pipeline_state["success_count"]
            summary["failed_count"] = _pipeline_state["failed_count"]
            summary["steps"] = copy.deepcopy(_pipeline_state["steps"])
            summary["stage_counts"] = dict(_pipeline_state["stage_counts"])

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
            for step in _pipeline_state["steps"]:
                if step["status"] == "running":
                    step["status"] = "pending"
        raise
