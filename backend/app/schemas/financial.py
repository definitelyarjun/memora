"""Pydantic models for Module 5 — Financial Impact & ROI Simulator.

The CFO-level mathematical proof of why the startup needs to adopt AI now.

Metrics produced:
    Metric 5  — Net Monthly Savings (INR)
    Metric 12 — Operating Margin Lift (percentage point improvement)
    Metric 7  — Opportunity Cost of Delay (INR per month of inaction)
"""

from __future__ import annotations

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

class EmployeeSavingsLine(BaseModel):
    """Per-employee monthly savings from role automation."""

    employee_id: str
    name: str
    job_title: str
    monthly_salary_inr: float
    hours_per_week: float
    hours_saved_per_week: float = Field(
        ..., description="From Module 4 role audit"
    )
    loaded_hourly_rate_inr: float = Field(
        ..., description="Salary × 1.25 benefits overhead ÷ monthly hours"
    )
    monthly_hours_saved: float = Field(
        ..., description="hours_saved_per_week × 4.33"
    )
    gross_monthly_savings_inr: float = Field(
        ..., description="monthly_hours_saved × loaded_hourly_rate"
    )


class AIToolRecommendation(BaseModel):
    """An AI/automation tool recommended to unlock a role's savings."""

    tool_name: str
    purpose: str = Field(..., description="What this tool automates")
    monthly_cost_inr: float = Field(
        ..., ge=0, description="₹0 if already in tech stack"
    )
    replaces: str = Field(..., description="What it replaces (manual task or tool)")
    already_in_stack: bool = Field(
        ..., description="True if the startup already uses this tool"
    )
    for_role_category: str = Field(
        ..., description="The role type this tool targets"
    )


class BeforeAfterRow(BaseModel):
    """One row in the Before vs After dashboard."""

    metric: str
    before_value: str
    after_value: str
    delta: str
    icon: str


# ---------------------------------------------------------------------------
# Main report
# ---------------------------------------------------------------------------

class FinancialReport(BaseModel):
    """Complete Financial Impact & ROI Simulator report — Module 5."""

    session_id: str

    # ── Context ──────────────────────────────────────────────────────────
    current_mrr: float = Field(..., description="Latest month MRR from startup profile")
    total_payroll_monthly_inr: float = Field(
        ..., description="Sum of all employee salaries from Module 4"
    )
    total_recurring_expenses_inr: float = Field(
        ..., description="Average monthly recurring costs from expenses.csv"
    )
    total_monthly_costs_inr: float = Field(
        ..., description="Payroll + recurring expenses = full cost base"
    )
    headcount: int

    # ── Metric 5 — Net Monthly Savings ───────────────────────────────────
    gross_monthly_savings_inr: float = Field(
        ..., description="Sum of loaded-rate × hours-saved across automatable roles"
    )
    new_ai_tools_monthly_cost_inr: float = Field(
        ..., description="Monthly cost of new AI tools not already in tech stack"
    )
    net_monthly_savings_inr: float = Field(
        ..., description="Metric 5 — gross savings minus new tool costs"
    )
    net_annual_savings_inr: float = Field(
        ..., description="net_monthly × 12"
    )

    # ── Metric 12 — Operating Margin Lift ────────────────────────────────
    current_operating_margin_pct: float = Field(
        ..., description="(MRR − total costs) ÷ MRR × 100"
    )
    projected_operating_margin_pct: float = Field(
        ..., description="Margin after savings and new tool costs"
    )
    gross_margin_lift_pct: float = Field(
        ..., description="Metric 12 — projected − current (percentage points)"
    )

    # ── Metric 7 — Opportunity Cost of Delay ─────────────────────────────
    opportunity_cost_per_month_inr: float = Field(
        ..., description="Metric 7 — INR left on table for each month of inaction"
    )
    opportunity_cost_per_year_inr: float
    mrr_at_risk_monthly_inr: float = Field(
        ..., description="MRR at risk from TAT bottlenecks (Module 3 data)"
    )

    # ── Payback (from Module 6 if available, else estimated) ─────────────
    months_to_break_even: float | None = None

    # ── Detail ───────────────────────────────────────────────────────────
    employee_savings: list[EmployeeSavingsLine]
    ai_tool_recommendations: list[AIToolRecommendation]
    before_after: list[BeforeAfterRow]

    # ── Narrative ─────────────────────────────────────────────────────────
    headline: str
    executive_summary: str
    warnings: list[str] = Field(default_factory=list)
