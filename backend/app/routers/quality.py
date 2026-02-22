"""API router for Module 2 — Data Quality & AI Readiness Analyzer.

Endpoint:
    POST /api/v1/analyze/quality
        - Accepts a session_id from Module 1
        - Computes quality scores from the stored DataFrame + DataIssue list
        - Writes the QualityReport back into the session store
        - Returns QualityReport
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.quality import QualityReport
from app.services.quality import compute_quality_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Quality"])


@router.post("/quality", response_model=QualityReport)
def analyze_quality(
    session_id: str = Form(..., description="session_id returned by /ingest/tabular"),
) -> QualityReport:
    """Score data quality and compute an AI Readiness score.

    Reads the raw DataFrame, DataIssue list, workflow_analysis, and
    company_metadata stored by Module 1.  All calculations are
    deterministic — no LLM call is made.

    The resulting QualityReport is written back into the session store
    so that Modules 4, 5, and 7 can access it by session_id.

    Scoring dimensions (6)
    ----------------------
    Data Quality (60 % combined):
      Completeness            20 %   fraction of non-null cells
      Deduplication           15 %   fraction of non-duplicate rows
      Consistency             15 %   penalised by naming + whitespace issues
      Structural Integrity    10 %   penalised by unparsed dates + mixed dtypes

    Operational Readiness (40 % combined):
      Process Digitisation    25 %   automated_steps / total_steps from workflow
      Tool Maturity           15 %   scored by tool sophistication tier

    Readiness levels: High ≥0.80 · Moderate ≥0.60 · Low ≥0.40 · Critical <0.40
    """
    entry = session_store.get(session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{session_id}' not found or has expired. "
                "Re-upload the file via /ingest/tabular to start a new session."
            ),
        )

    try:
        report = compute_quality_report(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Write report back into the session so downstream modules can read it
    session_store.patch(session_id, quality_report=report)

    logger.info(
        "Quality report computed for session %s — AI readiness: %.2f (%s)",
        session_id,
        report.ai_readiness_score,
        report.readiness_level,
    )

    return report
