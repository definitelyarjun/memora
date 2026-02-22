"""API router for Module 4 — Automation Opportunity Detector.

Endpoint:
    POST /api/v1/analyze/automation
        - Accepts a session_id that already has workflow_analysis (Module 1a)
          and quality_report (Module 2) in the session store
        - Classifies each workflow step as an automation candidate
        - Writes the AutomationReport back into the session store
        - Returns AutomationReport
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, HTTPException

from app.core.session_store import session_store
from app.schemas.automation import AutomationReport
from app.services.automation import compute_automation_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Automation"])


@router.post("/automation", response_model=AutomationReport)
def analyze_automation(
    session_id: str = Form(..., description="session_id with workflow + quality report"),
) -> AutomationReport:
    """Detect automation opportunities across the company's workflow.

    Prerequisites:
      1. Run Module 1a (POST /ingest/tabular) with a workflow description
      2. Run Module 2  (POST /analyze/quality)

    For each workflow step the engine determines:
      - Whether it's an automation candidate
      - What type of automation applies (RPA, Digital Form, API, AI/ML, Decision Engine)
      - Confidence score + human-readable reasoning
      - Implementation effort and priority

    All classification is deterministic (rule-based keyword matching).
    No LLM call is made.
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
        report = compute_automation_report(session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    # Write report back into session for downstream modules
    session_store.patch(session_id, automation_report=report)

    logger.info(
        "Automation report computed for session %s — %d/%d steps automatable",
        session_id,
        report.summary.automatable_steps,
        report.summary.total_steps,
    )

    return report
