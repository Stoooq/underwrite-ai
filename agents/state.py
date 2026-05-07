from pydantic import BaseModel


class UnderwritingState(BaseModel):
    client_id: int
    features: dict[str, float]
    income_flag: bool | None = None
    consistency_flag: bool | None = None
    probability_of_default: float | None = None
    shap_summary: str | None = None
    decision: str | None = None
    report: str | None = None
