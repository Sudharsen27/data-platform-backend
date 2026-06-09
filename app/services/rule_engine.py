"""Execute active data-quality rules against quarantine rows."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from app.models import QuarantineData, Rule


@dataclass
class RuleViolation:
    field: str
    message: str
    rule_id: int | None = None
    rule_text: str = ""


@dataclass
class RuleExecutionStats:
    active_rules: int = 0
    records_evaluated: int = 0
    records_with_violations: int = 0
    total_violations: int = 0
    rule_hits: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "active_rules": self.active_rules,
            "records_evaluated": self.records_evaluated,
            "records_with_violations": self.records_with_violations,
            "total_violations": self.total_violations,
            "rule_hits": self.rule_hits,
        }


def _is_empty(value) -> bool:
    return str(value or "").strip() == ""


def _field_value(row: QuarantineData | dict, field_name: str):
    if isinstance(row, dict):
        return row.get(field_name, "")
    return getattr(row, field_name, "")


def _apply_rule(
    field_value,
    field_name: str,
    rule_text: str,
    *,
    email_key: str,
    email_seen_count: int,
) -> str:
    normalized_rule = str(rule_text or "").lower()
    text_value = str(field_value or "")

    if (
        "cannot be null" in normalized_rule
        or "cannot be empty" in normalized_rule
        or "required" in normalized_rule
        or "must not be blank" in normalized_rule
    ):
        if _is_empty(field_value):
            return f"{field_name} is required"

    if field_name == "email":
        if "contain @" in normalized_rule or "valid domain" in normalized_rule:
            if "@" not in text_value:
                return "email must contain @"
            if "valid domain" in normalized_rule or "domain suffix" in normalized_rule:
                domain = text_value.split("@")[-1] if "@" in text_value else ""
                if not domain or "." not in domain:
                    return "email must contain @ and a valid domain suffix"

        if "unique" in normalized_rule and email_key:
            if email_seen_count > 0:
                return "email should be unique across source systems"

    if field_name == "name":
        if "alphabetic" in normalized_rule or "at least 2" in normalized_rule:
            letters = re.sub(r"[^a-zA-Z]", "", text_value)
            if len(letters) < 2:
                return "name should have at least 2 alphabetic characters"

    if "unique" in normalized_rule and field_name.endswith("_id"):
        if _is_empty(field_value):
            return f"{field_name} is required for uniqueness checks"

    if "only digits" in normalized_rule or "contain only digits" in normalized_rule:
        if not _is_empty(field_value) and not text_value.isdigit():
            return f"{field_name} must contain only digits"

    length_match = re.search(r"length must be (\d+)", normalized_rule)
    if length_match and not _is_empty(field_value):
        expected = int(length_match.group(1))
        if len(text_value) != expected:
            return f"{field_name} length must be {expected}"

    if "future date" in normalized_rule and not _is_empty(field_value):
        from datetime import date

        try:
            parsed = date.fromisoformat(text_value[:10])
            if parsed > date.today():
                return f"{field_name} cannot be a future date"
        except ValueError:
            return f"{field_name} must be a valid date"

    if "greater than or equal to zero" in normalized_rule and not _is_empty(field_value):
        try:
            if float(text_value) < 0:
                return f"{field_name} must be greater than or equal to zero"
        except ValueError:
            return f"{field_name} must be numeric"

    if "format" in normalized_rule and field_name in ("email", "name", "record"):
        if field_name == "email" and "@" not in text_value and not _is_empty(field_value):
            return f"{field_name} format is invalid"

    if "quarantine when" in normalized_rule or "flag quarantine" in normalized_rule:
        return rule_text[:120]

    return ""


def evaluate_row(
    row: QuarantineData | dict,
    rules: list[Rule],
    *,
    email_seen_before: int = 0,
) -> list[RuleViolation]:
    violations: list[RuleViolation] = []
    email_key = str(_field_value(row, "email") or "").strip().lower()
    active = [r for r in rules if str(r.status or "").lower() == "active"]

    for rule in active:
        field_name = str(rule.field or "").strip().lower()
        if not field_name or field_name == "record":
            field_name = "record"
            field_value = _field_value(row, "email") or _field_value(row, "name")
        else:
            field_value = _field_value(row, field_name)

        message = _apply_rule(
            field_value,
            field_name,
            rule.rule or "",
            email_key=email_key,
            email_seen_count=email_seen_before if field_name == "email" else 0,
        )
        if message:
            violations.append(
                RuleViolation(
                    field=field_name,
                    message=message,
                    rule_id=rule.id,
                    rule_text=rule.rule or "",
                )
            )

    return violations


def violations_to_error_string(violations: list[RuleViolation]) -> str:
    if not violations:
        return ""
    return ", ".join(v.message for v in violations)


def validate_row_with_rules(
    row: QuarantineData | dict,
    rules: list[Rule],
    *,
    email_seen_before: int = 0,
) -> str:
    return violations_to_error_string(
        evaluate_row(row, rules, email_seen_before=email_seen_before)
    )


def run_rule_execution(
    rows: list[QuarantineData],
    rules: list[Rule],
    *,
    seen_emails: dict[str, int] | None = None,
) -> tuple[list[str], RuleExecutionStats]:
    """
    Evaluate all rows; return parallel list of error strings and aggregate stats.
    Pass seen_emails across batches so uniqueness rules work on the full dataset.
    """
    active = [r for r in rules if str(r.status or "").lower() == "active"]
    stats = RuleExecutionStats(active_rules=len(active))
    hit_counter: Counter[int] = Counter()
    errors_out: list[str] = []
    email_counts = seen_emails if seen_emails is not None else {}

    for row in rows:
        email_key = (row.email or "").strip().lower()
        prior = email_counts.get(email_key, 0) if email_key else 0
        violations = evaluate_row(row, active, email_seen_before=prior)
        err = violations_to_error_string(violations)
        errors_out.append(err)

        stats.records_evaluated += 1
        if violations:
            stats.records_with_violations += 1
            stats.total_violations += len(violations)
            for v in violations:
                if v.rule_id is not None:
                    hit_counter[v.rule_id] += 1

        if email_key:
            email_counts[email_key] = prior + 1

    stats.rule_hits = [
        {
            "rule_id": rule.id,
            "field": rule.field,
            "rule": rule.rule,
            "hits": hit_counter.get(rule.id, 0),
        }
        for rule in active
        if hit_counter.get(rule.id, 0) > 0
    ]
    stats.rule_hits.sort(key=lambda item: item["hits"], reverse=True)
    return errors_out, stats
