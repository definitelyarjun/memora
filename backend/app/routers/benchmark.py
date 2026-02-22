"""API router for Module 3 — Industry Benchmarking & Competitiveness Analyzer.

Endpoint:
    POST /api/v1/analyze/benchmark
        - Accepts a JSON body (BenchmarkRequest)
        - Pulls company metadata from the session store (Module 1 output)
        - Runs Pandas stats against the bundled market dataset
        - Sends structured data to Gemini for strategic pricing advice
        - Writes BenchmarkReport back into the session store
        - Returns BenchmarkReport

Note: This module is independently callable — it does NOT require
/analyze/quality to have run first. It only needs a valid session_id
from /ingest/tabular (for company_metadata context).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.core.session_store import session_store
from app.schemas.benchmark import BenchmarkReport, BenchmarkRequest
from app.services.benchmark import run_benchmark

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/analyze", tags=["Analysis — Benchmarking"])


@router.post("/benchmark", response_model=BenchmarkReport)
def analyze_benchmark(request: BenchmarkRequest) -> BenchmarkReport:
    """Benchmark a product/service against the market and generate pricing strategy.

    Stats layer (always runs — no LLM required)
    -------------------------------------------
    - Loads bundled market dataset for the requested category
    - Computes market average, median, min, max, std
    - Calculates your price percentile rank
    - Scores your feature overlap with competitors (Jaccard similarity)
    - Identifies the 5 closest competitors by price

    LLM strategy layer (requires GEMINI_API_KEY)
    ---------------------------------------------
    - Sends all stats + top competitors to Gemini
    - Returns: competitiveness score, strategic recommendation,
      suggested optimal price, key insights
    - If Gemini is unavailable → stats are still returned, LLM fields are null

    Supported categories: hotel, restaurant, electronics, apparel, saas, consulting

    The result is stored back in the session so Module 7 (Strategic Verdict)
    can aggregate it with all other module outputs.
    """
    entry = session_store.get(request.session_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Session '{request.session_id}' not found or has expired. "
                "Re-upload via /ingest/tabular to start a new session."
            ),
        )

    report = run_benchmark(request=request, entry=entry, session_id=request.session_id)

    # Write back into session for Module 7 aggregation
    session_store.patch(request.session_id, benchmark_report=report)

    logger.info(
        "Benchmark complete for session %s — category: %s, position: %s, "
        "competitiveness: %s",
        request.session_id,
        report.category,
        report.price_position,
        report.competitiveness_score,
    )

    return report
