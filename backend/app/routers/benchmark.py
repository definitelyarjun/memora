"""API router for Module 3 — Workflow Bottleneck & Speed Analyzer.

FoundationIQ 3.0 (Startup Edition)

Endpoint:
    POST /api/v1/analyze/bottleneck
        - Accepts a JSON body with a session_id
        - Reads sales_inquiries_df from the session store (ingested by Module 1)
        - Calculates Turnaround Time (TAT) per inquiry using pandas
        - Flags bottlenecks (TAT > 48 hours)
        - Computes Metric 11 (TAT Improvement %) and Metric 4 (Hours Saved)
        - Generates a Mermaid flowchart
        - Stores BottleneckReport back into the session for Module 7
        - Returns BottleneckReport

Prerequisites:
    - A valid session_id from POST /api/v1/ingest/startup (Module 1)
    - sales_inquiries.csv must have been uploaded in that session
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.core.session_store import session_store
from app.schemas.benchmark import BottleneckReport
from app.services.benchmark import compute_bottleneck_report

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Bottleneck"])


class BottleneckRequest(BaseModel):
    session_id: str = Field(..., description="session_id from Module 1 /ingest/startup")


@router.post("/bottleneck", response_model=BottleneckReport)
def analyze_bottleneck(request: BottleneckRequest) -> BottleneckReport:
    """Analyze sales pipeline TAT to surface bottlenecks and automation savings.

    Stats computed (all deterministic Pandas — no LLM required)
    -----------------------------------------------------------
    - Average / median / min / max TAT in hours (closed inquiries only)
    - Bottleneck count and % (TAT > 48 hours)
    - Metric 11: avg TAT improvement % if automation drops TAT to 2h
    - Metric 4: total pipeline hours recoverable via automation
    - Per-inquiry TAT table with bottleneck flags
    - Mermaid TD flowchart showing bottleneck distribution

    The result is stored back in the session so Module 7 (Strategic Verdict)
    can incorporate it into the composite readiness score.
    """
    entry = session_store.get(request.session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{request.session_id}' not found or has expired. "
                "Re-upload via /ingest/startup to start a new session."
            ),
        )

    try:
        report = compute_bottleneck_report(
            session_id=request.session_id,
            entry=entry,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    # Store for Module 7 aggregation
    session_store.patch(request.session_id, benchmark_report=report)

    logger.info(
        "Bottleneck analysis complete for session %s — "
        "closed: %d/%d, bottlenecks: %d (%.0f%%), avg TAT: %.1fh, "
        "hours saved: %.0f",
        request.session_id,
        report.closed_inquiries,
        report.total_inquiries,
        report.bottleneck_count,
        report.bottleneck_pct,
        report.avg_tat_hours,
        report.total_hours_saved,
    )

    return report
