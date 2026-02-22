"""Router for Module 6 — Growth & Retention Benchmarking."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.session_store import session_store
from app.schemas.retention import RetentionReport
from app.services.retention import compute_retention_report

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Retention Benchmarking"])


class RetentionRequest(BaseModel):
    session_id: str


@router.post("/retention", response_model=RetentionReport)
def analyze_retention(request: RetentionRequest) -> RetentionReport:
    """Run Module 6 — Growth & Retention Benchmarking.

    Requires:
      - Session created by Module 1 with sales_inquiries.csv uploaded.

    Returns:
      - Metric 9 : Churn Reduction Potential
      - Metric 10: NRR Projection
      - Competitor benchmarks + growth radar data
    """
    session_id = request.session_id.strip()
    entry = session_store.get(session_id)
    if entry is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Session not found or expired.")

    try:
        report = compute_retention_report(session_id, entry)
    except ValueError as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=422, detail=str(exc))

    session_store.patch(session_id, retention_report=report)
    return report
