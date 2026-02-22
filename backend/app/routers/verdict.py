"""API router for Module 7 — Strategic Verdict Generator.

Endpoint:
    POST /api/v1/analyze/verdict
        - Accepts a session_id that has at least one analysis module (2–6) run.
        - Returns StrategicVerdict aggregating all available module outputs.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.verdict import StrategicVerdict
from app.services.verdict import compute_strategic_verdict

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Strategic Verdict"])


@router.post("/verdict", response_model=StrategicVerdict)
def analyze_verdict(
    session_id: str = Form(..., description="session_id from Module 1a"),
) -> StrategicVerdict:
    """Generate the final strategic diagnostic aggregating all module outputs.

    Prerequisites (at least one required):
      - Module 2 (POST /analyze/quality) — data quality & AI readiness
      - Module 3 (POST /analyze/benchmark) — industry benchmarking
      - Module 4 (POST /analyze/automation) — automation opportunities
      - Module 5 (POST /analyze/consolidation) — data consolidation
      - Module 6 (POST /analyze/roi) — ROI estimator

    The more modules that have been run, the richer the verdict.
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
        report = compute_strategic_verdict(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    return report
