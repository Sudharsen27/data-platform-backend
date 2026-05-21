from app.database import SessionLocal
from app.models import QuarantineData, Rule
from app.services.rule_engine import evaluate_row, run_rule_execution, validate_row_with_rules


def _active_rules():
    return [
        Rule(id=1, field="email", rule="Email cannot be null", status="active"),
        Rule(
            id=2,
            field="email",
            rule="Email must contain @ and a valid domain suffix",
            status="active",
        ),
        Rule(
            id=3,
            field="name",
            rule="Name should have at least 2 alphabetic characters",
            status="active",
        ),
        Rule(
            id=4,
            field="email",
            rule="Email should be unique across source systems",
            status="active",
        ),
    ]


def test_email_required_and_format():
    rules = _active_rules()
    violations = evaluate_row({"name": "Ann", "email": ""}, rules)
    messages = [v.message for v in violations]
    assert any("required" in m for m in messages)

    violations2 = evaluate_row({"name": "Ann", "email": "bad"}, rules)
    assert any("@" in v.message for v in violations2)


def test_duplicate_email_across_rows():
    rules = _active_rules()
    seen: dict[str, int] = {}
    rows = [
        QuarantineData(name="A", email="dup@test.com", error=""),
        QuarantineData(name="B", email="dup@test.com", error=""),
    ]
    errors, stats = run_rule_execution(rows, rules, seen_emails=seen)
    assert stats.records_with_violations >= 1
    assert any("unique" in e for e in errors if e)


def test_inactive_rules_skipped():
    db = SessionLocal()
    try:
        rules = [
            Rule(field="email", rule="Email cannot be null", status="inactive"),
        ]
        err = validate_row_with_rules({"name": "X", "email": ""}, rules)
        assert err == ""
    finally:
        db.close()
