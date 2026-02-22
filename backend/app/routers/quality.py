"""API router for Module 2 — Data Quality & DPDP Compliance Scanner.

FoundationIQ 3.0 (Startup Edition)

Endpoint:
    POST /api/v1/analyze/quality
        - Accepts a session_id from Module 1
        - Computes quality scores from stored DataFrames + DataIssue list
        - Runs DPDP PII compliance scan across all uploaded CSVs
        - Writes the QualityReport back into the session store
        - Returns QualityReport with Metric 2 + Metric 6
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.quality import QualityReport
from app.services.quality import compute_quality_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Quality & DPDP"])


@router.post("/quality", response_model=QualityReport)
def analyze_quality(
    session_id: str = Form(..., description="session_id returned by /ingest/startup"),
) -> QualityReport:
    """Score data quality and run DPDP compliance scan.

    Reads all stored DataFrames (org_chart, expenses, sales_inquiries),
    DataIssue list, workflow_analysis, and company_metadata from Module 1.
    All calculations are deterministic — no LLM call is made.

    The resulting QualityReport is written back into the session store
    so that Modules 4, 5, and 7 can access it by session_id.

    Metrics produced:
      Metric 2 — Data Quality Score (>0.85 = pass, no cleanup mandate)
      Metric 6 — DPDP Risk Level (PII column scan)

    Scoring dimensions (7):
      Completeness              25 %
      Deduplication             20 %
      Consistency               15 %
      Structural Integrity      10 %
      Process Digitisation      15 %
      Tool Maturity              5 %
      Data Coverage             10 %

    Quality levels: High ≥0.80 · Moderate ≥0.60 · Low ≥0.40 · Critical <0.40
    """
    entry = session_store.get(session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{session_id}' not found or has expired. "
                "Re-upload files via /ingest/startup to start a new session."
            ),
        )

    try:
        report = compute_quality_report(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Write report back into the session so downstream modules can read it
    session_store.patch(session_id, quality_report=report)

    logger.info(
        "Quality report computed for session %s — score: %.2f (%s), DPDP risk: %s",
        session_id,
        report.data_quality_score,
        report.readiness_level,
        report.dpdp_compliance.risk_level,
    )

    return report
