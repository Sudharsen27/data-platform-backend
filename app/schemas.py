from pydantic import BaseModel


class QuarantineBase(BaseModel):
    name: str
    email: str
    error: str = ""


class QuarantineUpdate(QuarantineBase):
    id: int


class QuarantineOut(QuarantineBase):
    id: int

    class Config:
        from_attributes = True


class RuleBase(BaseModel):
    field: str
    rule: str
    status: str = "active"


class RuleCreate(RuleBase):
    pass


class RuleUpdate(RuleBase):
    id: int


class RuleOut(RuleBase):
    id: int

    class Config:
        from_attributes = True
