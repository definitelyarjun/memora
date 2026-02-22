"""Router — Module 5: Financial Impact & ROI Simulator.

POST /api/v1/analyze/financial-impact
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.session_store import session_store
from app.schemas.financial import FinancialReport
from app.services.financial import compute_financial_report

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Financial Impact"])


class FinancialImpactRequest(BaseModel):
    session_id: str


@router.post("/financial-impact", response_model=FinancialReport)
def analyze_financial_impact(request: FinancialImpactRequest) -> FinancialReport:
    """Compute CFO-level financial impact: Metrics 5, 12, and 7.

    Requires Module 4 (automation_report) to have been run.
    Enriched by Module 3 (benchmark_report) for Metric 7.
    Uses expenses.csv from Module 1 for accurate margin calculation.
    """
    entry = session_store.get(request.session_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Session not found.")

    try:
        report = compute_financial_report(request.session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    session_store.patch(request.session_id, financial_report=report)
    return report
