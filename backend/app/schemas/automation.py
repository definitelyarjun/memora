"""Pydantic models for Module 4 — Organizational Role & Automation Auditor.

Analyses org_chart.csv to map every employee's role to its automation
potential, then calculates how much more revenue the same team could
handle once admin/repetitive tasks are automated.

Key Metrics
-----------
  Metric 3  Role Automation Potential (% of each role that is automatable)
  Metric 8  Revenue Per Employee (RPE) Lift — Current MRR ÷ Headcount
             vs Projected MRR ÷ Headcount after growth (automation enables
             the *same* team to handle far more revenue without new hires)

Vulnerability Levels
--------------------
  High    ≥ 60%  of tasks automatable — role needs significant upskilling
  Medium  30–59% automatable — moderate upskilling recommended
  Low     < 30%  automatable — mostly strategic/human-judgment tasks
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RoleAnalysis(BaseModel):
    """Automation audit result for a single employee / role."""

    employee_id: str
    name: str
    job_title: str
    department: str
    monthly_salary_inr: float
    hours_per_week: float

    # --- Metric 3: Role Automation Potential ---
    automation_pct: float = Field(
        ...,
        description="Percentage of this role's tasks that are automatable (0–100)",
        ge=0.0,
        le=100.0,
    )
    automatable_tasks: list[str] = Field(
        default_factory=list,
        description="Specific task types within this role that automation can handle",
    )
    vulnerability_level: str = Field(
        ...,
        description="High (≥60%) | Medium (30–59%) | Low (<30%)",
    )
    upskilling_rec: str = Field(
        ...,
        description="What this person should focus on once admin tasks are automated",
    )
    hours_saved_per_week: float = Field(
        ...,
        description="Estimated hours per week freed up by automation for this role",
    )


class RPEMetrics(BaseModel):
    """Revenue Per Employee metrics for Metric 8."""

    current_mrr: float = Field(
        ...,
        description="Latest month's MRR from startup_profile (INR)",
    )
    headcount: int = Field(
        ...,
        description="Total employees from org_chart.csv",
    )
    current_rpe_monthly: float = Field(
        ...,
        description="Current MRR ÷ headcount (INR per employee per month)",
    )
    projected_mrr: float = Field(
        ...,
        description=(
            "Projected MRR after patience_months of growth at monthly_growth_goal_pct "
            "— automation enables same team to handle this revenue"
        ),
    )
    projected_rpe_monthly: float = Field(
        ...,
        description="Projected MRR ÷ same headcount (INR per employee per month)",
    )
    rpe_lift_pct: float = Field(
        ...,
        description="Metric 8: ((projected_rpe - current_rpe) / current_rpe) × 100",
    )
    rpe_lift_inr: float = Field(
        ...,
        description="Absolute RPE lift in INR per employee per month",
    )
    growth_months_used: int = Field(
        ...,
        description="Number of months of growth modelled (from patience_months)",
    )
    monthly_growth_rate_pct: float = Field(
        ...,
        description="Monthly growth rate used for projection",
    )


class AutomationReport(BaseModel):
    """Full role & automation audit for a session."""

    session_id: str
    source_file: str = "org_chart.csv"
    total_employees: int

    # --- Per-role results ---
    roles: list[RoleAnalysis] = Field(default_factory=list)

    # --- Aggregate: Metric 3 ---
    avg_automation_pct: float = Field(
        ...,
        description="Mean automation potential across all roles (0–100)",
    )
    high_vulnerability_count: int = Field(
        ...,
        description="Roles with automation_pct ≥ 60%",
    )
    medium_vulnerability_count: int = Field(
        ...,
        description="Roles with 30% ≤ automation_pct < 60%",
    )
    low_vulnerability_count: int = Field(
        ...,
        description="Roles with automation_pct < 30%",
    )
    top_automatable_role: str = Field(
        ...,
        description="Job title with highest automation potential",
    )
    top_automatable_pct: float = Field(
        ...,
        description="Automation % of the most-automatable role",
    )
    total_hours_saved_per_week: float = Field(
        ...,
        description="Sum of hours_saved_per_week across all employees",
    )

    # --- Aggregate: Metric 8 ---
    rpe_metrics: RPEMetrics

    # --- Verdict.py compatibility ---
    # automation_coverage = avg_automation_pct / 100 (a 0-1 score used by
    # the overall readiness composite in verdict.py)
    automation_coverage: float = Field(
        ...,
        description="avg_automation_pct / 100 — used as a 0-1 signal for the overall score",
        ge=0.0,
        le=1.0,
    )

    # --- Guidance ---
    recommendations: list[str] = Field(default_factory=list)
    mermaid_chart: str = Field(
        default="",
        description="Mermaid flowchart showing roles grouped by vulnerability level",
    )
    warnings: list[str] = Field(default_factory=list)
