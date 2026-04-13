from typing import List

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.database import Base, SessionLocal, engine, get_db
from app.models import QuarantineData, Rule
from app.schemas import QuarantineOut, QuarantineUpdate, RuleCreate, RuleOut, RuleUpdate

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)


def seed_data(db: Session):
    if db.query(QuarantineData).count() == 0:
        db.add_all(
            [
                QuarantineData(name="John", email="", error="Email missing"),
                QuarantineData(name="Alice", email="alice@mail.com", error=""),
                QuarantineData(
                    name="Mark",
                    email="markmail.com",
                    error="Invalid email format",
                ),
            ]
        )

    if db.query(Rule).count() == 0:
        db.add_all(
            [
                Rule(field="email", rule="Email cannot be null", status="active"),
                Rule(field="name", rule="Name must be at least 2 chars", status="active"),
                Rule(field="phone", rule="Phone format must be valid", status="inactive"),
            ]
        )

    db.commit()


@app.on_event("startup")
def on_startup():
    db = SessionLocal()
    try:
        seed_data(db)
    finally:
        db.close()


@app.get("/")
def home():
    return {"message": "Backend running 🚀"}


@app.get("/dashboard")
def dashboard():
    return {
        "success_rate": 95,
        "failed_records": 120,
        "active_jobs": 5,
        "success_vs_failed": [
            {"name": "Success", "value": 2280},
            {"name": "Failed", "value": 120},
        ],
        "records_trend": [
            {"day": "Mon", "records": 280},
            {"day": "Tue", "records": 350},
            {"day": "Wed", "records": 300},
            {"day": "Thu", "records": 420},
            {"day": "Fri", "records": 480},
            {"day": "Sat", "records": 360},
            {"day": "Sun", "records": 210},
        ],
        "error_distribution": [
            {"type": "Email Missing", "count": 42},
            {"type": "Invalid Email", "count": 28},
            {"type": "Name Missing", "count": 19},
            {"type": "Phone Invalid", "count": 31},
        ],
    }


@app.get("/quarantine", response_model=List[QuarantineOut])
def get_quarantine(db: Session = Depends(get_db)):
    return db.query(QuarantineData).order_by(QuarantineData.id.asc()).all()


@app.post("/quarantine/update")
def update_quarantine(payload: QuarantineUpdate, db: Session = Depends(get_db)):
    record = db.query(QuarantineData).filter(QuarantineData.id == payload.id).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    record.name = payload.name
    record.email = payload.email
    record.error = payload.error
    db.commit()
    db.refresh(record)

    return {"message": "Record updated successfully", "record": record}


@app.get("/rules", response_model=List[RuleOut])
def get_rules(db: Session = Depends(get_db)):
    return db.query(Rule).order_by(Rule.id.asc()).all()


@app.post("/rules")
@app.post("/rules/add")
def add_rule(payload: RuleCreate, db: Session = Depends(get_db)):
    new_rule = Rule(field=payload.field, rule=payload.rule, status=payload.status)
    db.add(new_rule)
    db.commit()
    db.refresh(new_rule)
    return {"message": "Rule added successfully", "rule": new_rule}


@app.post("/rules/update")
def update_rule(payload: RuleUpdate, db: Session = Depends(get_db)):
    rule_item = db.query(Rule).filter(Rule.id == payload.id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    rule_item.field = payload.field
    rule_item.rule = payload.rule
    rule_item.status = payload.status
    db.commit()
    db.refresh(rule_item)
    return {"message": "Rule updated successfully", "rule": rule_item}


@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule_item = db.query(Rule).filter(Rule.id == rule_id).first()
    if not rule_item:
        raise HTTPException(status_code=404, detail="Rule not found")

    db.delete(rule_item)
    db.commit()
    return {"message": "Rule deleted successfully"}