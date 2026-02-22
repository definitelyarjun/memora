"""Pydantic models for Module 6 — Growth & Retention Benchmarking.

Metrics computed:
  Metric 9  — Churn Reduction Potential  (current → projected churn %)
  Metric 10 — Net Revenue Retention (NRR) Projection (target 106%+)
"""
from __future__ import annotations

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------

class CompetitorChurnBenchmark(BaseModel):
    """Single competitor / industry-tier data point from LLM."""
    company: str           # e.g. "Freshdesk (SaaS)"
    sector: str            # e.g. "SaaS"
    churn_pct: float       # monthly churn %
    nrr_pct: float | None  # NRR %, if available


class RadarDataPoint(BaseModel):
    """One axis of the growth radar chart (0–100 normalised score)."""
    axis: str               # axis label
    startup_value: float    # this startup's score 0–100
    industry_avg: float     # 2026 industry average 0–100
    top_tier: float         # top-decile benchmark 0–100


# ---------------------------------------------------------------------------
# Top-level report
# ---------------------------------------------------------------------------

class RetentionReport(BaseModel):
    """Full Growth & Retention Benchmarking report — Module 6."""

    session_id: str

    # ── Raw inquiry stats ────────────────────────────────────────────────
    total_inquiries: int
    closed_won_count: int
    repeat_customer_count: int   # Repeat_Customer_Flag == Yes (among Closed Won)
    new_customer_count: int      # Repeat_Customer_Flag == No  (among Closed Won)
    lost_count: int
    pending_count: int
    win_rate_pct: float          # closed_won / total × 100
    repeat_rate_pct: float       # repeat_won / closed_won × 100

    # ── Metric 9 — Churn Reduction ────────────────────────────────────────
    current_churn_pct: float        # estimated from sales data (100 − repeat_rate)
    projected_churn_pct: float      # after AI-driven personalisation + follow-ups
    churn_reduction_pct: float      # Metric 9: current − projected
    industry_avg_churn_pct: float   # 2026 LLM benchmark
    top_tier_churn_pct: float       # best-in-class benchmark

    # ── Metric 10 — NRR Projection ────────────────────────────────────────
    current_nrr_pct: float          # estimated NRR today
    projected_nrr_pct: float        # Metric 10: NRR post-automation
    nrr_benchmark_pct: float        # industry NRR benchmark (e.g. 106%)

    # ── Growth insights (from LLM) ────────────────────────────────────────
    growth_levers: list[str]
    sector_risks: list[str]
    competitor_benchmarks: list[CompetitorChurnBenchmark]

    # ── Radar chart data (5 axes) ─────────────────────────────────────────
    radar_data: list[RadarDataPoint]

    # ── Narrative ─────────────────────────────────────────────────────────
    headline: str
    executive_summary: str
    warnings: list[str]
