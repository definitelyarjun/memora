"""Pydantic models for Module 6 — ROI Estimator.

Computes realistic time-saved, cost-saved, and annual savings estimates
based on automation opportunities (Module 4) and consolidation migrations
(Module 5).  All estimates use conservative, defensible assumptions.

Key principles:
  - Every number traces back to a stated assumption (hours/week, wage rate, etc.)
  - Assumptions are exposed in the report so SME owners can verify or adjust
  - Cost estimates use median SME wages by industry, not inflated enterprise rates
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

class Assumption(BaseModel):
    """A stated assumption underlying the ROI calculation."""

    key: str = Field(..., description="Machine-readable key (e.g. 'hourly_wage')")
    label: str = Field(..., description="Human-readable label")
    value: str = Field(..., description="The assumed value (as display string)")
    source: str = Field("FoundationIQ default", description="Where the value comes from")


class AutomationROILine(BaseModel):
    """ROI projection for one automatable workflow step."""

    step_number: int
    description: str
    automation_type: str
    current_hours_per_week: float = Field(
        ..., ge=0, description="Estimated hours currently spent per week on this step",
    )
    hours_saved_per_week: float = Field(
        ..., ge=0, description="Hours saved if automated (factoring in automation efficiency)",
    )
    annual_hours_saved: float = Field(..., ge=0)
    annual_cost_saved: float = Field(
        ..., ge=0, description="annual_hours_saved × blended hourly rate",
    )
    implementation_cost: float = Field(
        ..., ge=0, description="One-time cost to implement this automation",
    )
    payback_months: float | None = Field(
        None, ge=0, description="Months to recoup implementation cost. None if cost is 0.",
    )
    effort: str
    priority: str


class ConsolidationROILine(BaseModel):
    """ROI projection for one consolidation migration step."""

    migration_priority: int
    action: str
    from_tool: str
    to_tool: str
    current_overhead_hours_per_week: float = Field(
        ..., ge=0,
        description="Hours wasted per week on duplication, reconciliation, re-entry",
    )
    hours_saved_per_week: float = Field(..., ge=0)
    annual_hours_saved: float = Field(..., ge=0)
    annual_cost_saved: float = Field(..., ge=0)
    implementation_cost: float = Field(..., ge=0)
    payback_months: float | None = None
    effort: str


class ROISummary(BaseModel):
    """Aggregate ROI figures."""

    total_current_hours_per_week: float = Field(
        ..., ge=0, description="Total manual hours/week across all wasteful steps",
    )
    total_hours_saved_per_week: float = Field(..., ge=0)
    total_annual_hours_saved: float = Field(..., ge=0)
    total_annual_cost_saved: float = Field(..., ge=0)
    total_implementation_cost: float = Field(..., ge=0)
    net_first_year_benefit: float = Field(
        ..., description="annual_cost_saved − implementation_cost (can be negative)",
    )
    three_year_net_benefit: float = Field(
        ..., description="(annual_cost_saved × 3) − implementation_cost",
    )
    overall_payback_months: float | None = Field(
        None,
        description="Months to break even overall. None if implementation cost is 0.",
    )
    roi_percentage: float = Field(
        ..., description="(annual_cost_saved / implementation_cost × 100). 0 if no cost.",
    )


# ---------------------------------------------------------------------------
# Top-level report
# ---------------------------------------------------------------------------

class ROIReport(BaseModel):
    """Full ROI analysis combining automation and consolidation savings."""

    session_id: str

    # Inputs used
    assumptions: list[Assumption] = Field(default_factory=list)

    # Line items
    automation_lines: list[AutomationROILine] = Field(default_factory=list)
    consolidation_lines: list[ConsolidationROILine] = Field(default_factory=list)

    # Aggregates
    summary: ROISummary

    # Narrative
    executive_summary: str = Field(
        ..., description="2-3 sentence plain-English ROI summary",
    )
    top_recommendations: list[str] = Field(
        default_factory=list,
        description="Ranked list of highest-ROI actions to take",
    )
