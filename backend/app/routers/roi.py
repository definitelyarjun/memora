"""API router for Module 6 — ROI Estimator.

Endpoint:
    POST /api/v1/analyze/roi
        - Accepts a session_id that already has automation_report (Module 4)
          and/or consolidation_report (Module 5).
        - Returns ROIReport with time-saved, cost-saved, annual savings.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.roi import ROIReport
from app.services.roi import compute_roi_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — ROI"])


@router.post("/roi", response_model=ROIReport)
def analyze_roi(
    session_id: str = Form(..., description="session_id from Module 1a"),
) -> ROIReport:
    """Estimate ROI from automation and consolidation recommendations.

    Prerequisites (at least one required):
      - Module 4 (POST /analyze/automation) — automation candidates
      - Module 5 (POST /analyze/consolidation) — migration steps

    Returns time saved, cost saved, annual savings, implementation costs,
    payback period, and 3-year net benefit.
    """
    entry = session_store.get(session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{session_id}' not found or expired. "
                "Run Module 1a first."
            ),
        )

    try:
        report = compute_roi_report(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Persist in session for downstream modules (Module 7)
    entry.roi_report = report
    return report
