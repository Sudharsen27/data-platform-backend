"""Suggest data-quality rules from quarantine error patterns."""

from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import QuarantineData

_MAX_ERROR_PATTERNS = 15
_MAX_RULE_SUGGESTIONS = 8
_FALLBACK_SUGGESTIONS: list[tuple[str, str, float, str, int]] = [
    ("email", "Email must contain @ and a valid domain suffix", 0.75, "default profile", 0),
    ("name", "Name should have at least 2 alphabetic characters", 0.7, "default profile", 0),
    ("email", "Email should be unique across source systems", 0.72, "default profile", 0),
]


def _confidence_from_count(count: int, total_with_errors: int) -> float:
    if total_with_errors <= 0:
        return 0.7
    share = count / total_with_errors
    return round(min(0.95, 0.55 + share * 0.4), 2)


def _map_error_to_rule(error: str, count: int, total_with_errors: int) -> tuple[str, str, float] | None:
    text = (error or "").strip()
    if not text:
        return None

    low = text.lower()
    confidence = _confidence_from_count(count, total_with_errors)

    if "invalid email" in low or (
        "email" in low and any(token in low for token in ("format", "domain", "@", "invalid"))
    ):
        return (
            "email",
            "Email must contain @ and a valid domain suffix",
            confidence,
        )

    if "duplicate" in low:
        field = "email" if "email" in low else "record"
        return (
            field,
            "Identifier should be unique across source systems (duplicate detected in quarantine)",
            confidence,
        )

    if "required" in low or "missing" in low:
        if "email" in low:
            return ("email", "Email is required and must not be blank", confidence)
        if "name" in low:
            return ("name", "Name is required and must not be blank", confidence)
        return ("record", "Required fields must be populated before publish to master data", confidence)

    if "name" in low and any(token in low for token in ("short", "alphabetic", "character", "length")):
        return ("name", "Name should have at least 2 alphabetic characters", confidence)

    if "format" in low or "pattern" in low:
        field = "email" if "email" in low else "name" if "name" in low else "record"
        return (field, f"Field must match expected format ({text[:100]})", confidence)

    if "seeded record" in low:
        return None

    field = "email" if "email" in low else "name" if "name" in low else "record"
    return (
        field,
        f"Quarantine when validation fails: {text[:120]}",
        max(0.6, confidence - 0.05),
    )


def build_rule_suggestions_from_quarantine(db: Session) -> list[dict]:
    """
    Return suggestions: field, rule, confidence, source_error, occurrence_count.
    Dedupes by (field, rule). Uses defaults when quarantine has no errors.
    """
    error_rows = (
        db.query(QuarantineData.error, func.count(QuarantineData.id).label("cnt"))
        .filter(QuarantineData.error.isnot(None), QuarantineData.error != "")
        .group_by(QuarantineData.error)
        .order_by(func.count(QuarantineData.id).desc())
        .limit(_MAX_ERROR_PATTERNS)
        .all()
    )

    if not error_rows:
        return [
            {
                "field": field,
                "rule": rule,
                "confidence": confidence,
                "source_error": source,
                "occurrence_count": count,
            }
            for field, rule, confidence, source, count in _FALLBACK_SUGGESTIONS
        ]

    total_with_errors = sum(int(row.cnt) for row in error_rows)
    seen: set[tuple[str, str]] = set()
    suggestions: list[dict] = []

    for row in error_rows:
        mapped = _map_error_to_rule(row.error, int(row.cnt), total_with_errors)
        if not mapped:
            continue
        field_name, rule_text, confidence = mapped
        key = (field_name, rule_text)
        if key in seen:
            continue
        seen.add(key)
        suggestions.append(
            {
                "field": field_name,
                "rule": rule_text,
                "confidence": confidence,
                "source_error": row.error,
                "occurrence_count": int(row.cnt),
            }
        )
        if len(suggestions) >= _MAX_RULE_SUGGESTIONS:
            break

    if not suggestions:
        return [
            {
                "field": field,
                "rule": rule,
                "confidence": confidence,
                "source_error": source,
                "occurrence_count": count,
            }
            for field, rule, confidence, source, count in _FALLBACK_SUGGESTIONS
        ]

    return suggestions
