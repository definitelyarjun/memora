"""API router for Module 4 — Organizational Role & Automation Auditor.

Endpoint:
    POST /api/v1/analyze/role-audit
        - Accepts a JSON body with session_id
        - Reads org_chart.csv from the session (uploaded in Module 1)
        - Maps every job title to automation potential (rules-based, no LLM)
        - Calculates Metric 3 (Role Automation %) and Metric 8 (RPE Lift)
        - Writes AutomationReport back into session as automation_report
        - Returns AutomationReport
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.session_store import session_store
from app.schemas.automation import AutomationReport
from app.services.automation import compute_automation_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Role Auditor"])


class RoleAuditRequest(BaseModel):
    session_id: str


@router.post("/role-audit", response_model=AutomationReport)
def role_audit(request: RoleAuditRequest) -> AutomationReport:
    """Audit every role in the org chart for automation potential.

    Prerequisites:
      - Run Module 1 (POST /ingest/startup) with org_chart.csv uploaded

    For each employee the engine:
      - Matches the job title to a curated automation-potential database
      - Assigns a vulnerability level (High / Medium / Low)
      - Calculates hours saved per week (Metric 3)

    Also computes:
      - RPE Lift: how much revenue per employee grows if the same team
        handles projected MRR without new hires (Metric 8)

    All classification is deterministic (rule-based). No LLM call is made.
    """
    entry = session_store.get(request.session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{request.session_id}' not found or has expired. "
                "Re-run Module 1 to start a new session."
            ),
        )

    try:
        report = compute_automation_report(request.session_id, entry)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    session_store.patch(request.session_id, automation_report=report)

    logger.info(
        "Role audit computed for session %s — %d employees, avg automation %.0f%%, RPE lift %.0f%%",
        request.session_id,
        report.total_employees,
        report.avg_automation_pct,
        report.rpe_metrics.rpe_lift_pct,
    )

    return report
