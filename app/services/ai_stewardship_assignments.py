"""Assign stewardship owners to pending queue items (load-balanced, issue-aware)."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.models import StewardshipQueue

_MAX_BULK_ASSIGNMENTS = 50
_MAX_SELECTED_ASSIGNMENTS = 500

# Demo stewards — use emails so the UI/export stay consistent with catalog owners.
_STEWARDS: list[dict[str, str]] = [
    {
        "id": "customer",
        "name": "Data Steward - Customer Domain",
        "email": "steward-customer@example.com",
    },
    {
        "id": "product",
        "name": "Data Steward - Product Domain",
        "email": "steward-product@example.com",
    },
    {
        "id": "admin",
        "name": "Data Governance Admin",
        "email": "governance-admin@example.com",
    },
]


def _pick_steward_for_issue(issue: str) -> dict[str, str]:
    low = (issue or "").lower()
    if any(token in low for token in ("product", "sku", "catalog")):
        return _STEWARDS[1]
    if any(
        token in low
        for token in ("customer", "crm", "email", "duplicate", "identity", "pii")
    ):
        return _STEWARDS[0]
    return _STEWARDS[2]


def assign_stewardship_owners(
    db: Session,
    *,
    task_ids: list[int] | None = None,
    assign_all_pending: bool = False,
) -> dict:
    """
    Assign owner_email on pending stewardship rows.
    Pass task_ids for checkbox selection; assign_all_pending for dashboard bulk (max 50).
    """
    if task_ids:
        unique_ids = list(dict.fromkeys(task_ids))[:_MAX_SELECTED_ASSIGNMENTS]
        pending_items = (
            db.query(StewardshipQueue)
            .filter(
                StewardshipQueue.id.in_(unique_ids),
                StewardshipQueue.status == "pending",
            )
            .order_by(StewardshipQueue.id.asc())
            .all()
        )
        mode_label = f"{len(unique_ids)} selected"
        not_pending = len(unique_ids) - len(pending_items)
    elif assign_all_pending:
        pending_items = (
            db.query(StewardshipQueue)
            .filter(StewardshipQueue.status == "pending")
            .order_by(StewardshipQueue.id.asc())
            .limit(_MAX_BULK_ASSIGNMENTS)
            .all()
        )
        mode_label = "all pending (bulk)"
        not_pending = 0
    else:
        return {
            "summary": "Select pending task(s) or use assign all pending.",
            "details": [],
            "applied_count": 0,
            "skipped_count": 0,
            "assignments": [],
            "error": "no_target",
        }

    if not pending_items:
        summary = "No pending stewardship tasks found."
        if task_ids and not_pending:
            summary = f"No pending tasks in selection ({not_pending} not pending or missing)."
        return {
            "summary": summary,
            "details": [],
            "applied_count": 0,
            "skipped_count": 0,
            "assignments": [],
        }

    load: dict[str, int] = {s["email"]: 0 for s in _STEWARDS}
    details: list[str] = []
    assignments: list[dict] = []
    applied_count = 0
    skipped_count = 0

    for item in pending_items:
        preferred = _pick_steward_for_issue(item.issue or "")
        sorted_stewards = sorted(
            _STEWARDS,
            key=lambda s: (
                0 if s["email"] == preferred["email"] else 1,
                load[s["email"]],
            ),
        )
        chosen = sorted_stewards[0]
        load[chosen["email"]] += 1
        confidence = max(0.55, 0.92 - (load[chosen["email"]] - 1) * 0.08)

        current_owner = (getattr(item, "owner_email", None) or "").strip()
        if current_owner == chosen["email"]:
            skipped_count += 1
            details.append(
                f"TASK-{item.id}: already assigned to {chosen['name']} ({chosen['email']})"
            )
            continue

        item.owner_email = chosen["email"]
        applied_count += 1
        assignments.append(
            {
                "task_id": item.id,
                "owner_email": chosen["email"],
                "owner_name": chosen["name"],
                "confidence": confidence,
                "issue": item.issue or "",
            }
        )
        details.append(
            f"TASK-{item.id}: assigned {chosen['name']} ({chosen['email']}, "
            f"confidence {confidence:.2f}, issue: {item.issue or 'general review'})"
        )

    summary = (
        f"Assigned owners to {applied_count} task(s) ({mode_label})."
        if applied_count
        else f"No assignments changed ({mode_label})."
    )
    if skipped_count and applied_count:
        summary += f" Skipped {skipped_count} already assigned."
    elif skipped_count and not applied_count:
        summary = (
            f"All {skipped_count} selected task(s) already have the suggested owner."
            if task_ids
            else f"All {skipped_count} pending task(s) already have the suggested owner."
        )
    if not_pending and applied_count:
        summary += f" ({not_pending} id(s) skipped — not pending)."

    return {
        "summary": summary,
        "details": details,
        "applied_count": applied_count,
        "skipped_count": skipped_count,
        "assignments": assignments,
    }
