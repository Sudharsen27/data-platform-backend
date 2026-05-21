from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.deps.auth import get_current_user, require_permission
from app.models import Rule, User
from app.schemas import RuleValidateIn, RuleValidateOut, RuleViolationOut
from app.services.rule_engine import evaluate_row, violations_to_error_string

router = APIRouter(prefix="/rules", tags=["rules"])


@router.post("/validate", response_model=RuleValidateOut)
def validate_sample_row(
    body: RuleValidateIn,
    db: Session = Depends(get_db),
    _: User = Depends(require_permission("rules:read")),
):
    rules = db.query(Rule).filter(Rule.status == "active").order_by(Rule.id.asc()).all()
    row = {"name": body.name, "email": body.email}
    violations = evaluate_row(row, rules, email_seen_before=0)
    return {
        "violations": [
            RuleViolationOut(
                field=v.field,
                message=v.message,
                rule_id=v.rule_id,
                rule_text=v.rule_text,
            )
            for v in violations
        ],
        "error": violations_to_error_string(violations),
        "active_rules": len(rules),
    }
