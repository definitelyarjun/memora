"""API router for Module 5 — Data Consolidation Recommendations.

Endpoint:
    POST /api/v1/analyze/consolidation
        - Accepts a session_id that already has company_metadata (Module 1a).
        - Optionally enriched by workflow_analysis (Module 1a) and
          quality_report (Module 2) for deeper analysis.
        - Returns ConsolidationReport.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.consolidation import ConsolidationReport
from app.services.consolidation import compute_consolidation_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Consolidation"])


@router.post("/consolidation", response_model=ConsolidationReport)
def analyze_consolidation(
    session_id: str = Form(..., description="session_id from Module 1a"),
) -> ConsolidationReport:
    """Recommend data consolidation strategy for scattered tools and sources.

    Prerequisites:
      1. Run Module 1a (POST /ingest/tabular) — required for company metadata
         and (ideally) workflow analysis.

    Optional enrichments:
      - Module 2 (quality report) — provides column quality context.
      - Module 4 (automation report) — cross-references automation opportunities.

    The engine analyses:
      - Data silos (every tool or medium used)
      - Manual data flows between silos
      - Redundant data storage
      - Then recommends a migration plan + unified schema.
    """
    entry = session_store.get(session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{session_id}' not found or has expired. "
                "Re-run Module 1a to start a new session."
            ),
        )

    try:
        report = compute_consolidation_report(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Write report back into session for downstream modules
    session_store.patch(session_id, consolidation_report=report)

    logger.info(
        "Consolidation report for session %s — %d silos, %d manual flows, score %.0f%%",
        session_id,
        report.total_silos,
        report.manual_flows,
        report.consolidation_score * 100,
    )

    return report
