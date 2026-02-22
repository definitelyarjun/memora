"""Pydantic models for Module 3 — Workflow Bottleneck & Speed Analyzer.

FoundationIQ 3.0 (Startup Edition)

Metrics produced:
    Metric 11 — TAT Improvement %          (avg_tat_improvement_pct)
    Metric 4  — Bottleneck Reduction Potential  (total_hours_saved)
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class InquiryTAT(BaseModel):
    """Turnaround-time record for a single sales inquiry."""

    inquiry_id: str
    inquiry_date: str
    payment_date: str | None = None
    tat_hours: float | None = None     # None when payment_date is missing
    is_bottleneck: bool = False
    status: str = ""


class BottleneckReport(BaseModel):
    """Full Workflow Bottleneck & Speed Analysis report — Module 3.

    Only closed inquiries (those with a Payment_Date) contribute to TAT
    statistics.  Pending / Lost rows are counted but excluded from the
    avg / median / max / min calculations.
    """

    session_id: str
    source_file: str = "sales_inquiries.csv"

    # ── Dataset overview ─────────────────────────────────────────────────
    total_inquiries: int
    closed_inquiries: int       # rows that have a Payment_Date

    # ── TAT statistics (hours, closed inquiries only) ────────────────────
    avg_tat_hours: float
    median_tat_hours: float
    max_tat_hours: float
    min_tat_hours: float

    # ── Bottleneck analysis ──────────────────────────────────────────────
    bottleneck_threshold_hours: float = Field(
        48.0, description="TAT above this value is flagged as a bottleneck"
    )
    bottleneck_count: int       # inquiries whose TAT > threshold
    bottleneck_pct: float       # % of closed inquiries that are bottlenecks

    # ── Automation impact (Metric 11 + Metric 4) ─────────────────────────
    automation_target_hours: float = Field(
        2.0, description="Target TAT if manual handoff is replaced by API trigger"
    )
    avg_tat_improvement_pct: float = Field(
        ..., description="Metric 11 — % TAT reduction achievable with automation"
    )
    total_hours_saved: float = Field(
        ..., description="Metric 4 — total hours cut across all closed inquiries"
    )
    avg_hours_saved_per_inquiry: float

    # ── Per-inquiry breakdown ─────────────────────────────────────────────
    inquiry_tat_list: list[InquiryTAT] = Field(default_factory=list)

    # ── Visual output ─────────────────────────────────────────────────────
    mermaid_flowchart: str = ""

    # ── Narrative ─────────────────────────────────────────────────────────
    recommendations: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
