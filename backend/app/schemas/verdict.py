"""Pydantic models for Module 7 — Strategic Verdict Generator.

Aggregates outputs from all preceding modules into one executive-level
diagnostic report with a single overall readiness verdict, module-by-module
scorecard, prioritised action plan, and risk summary.

No new analysis is performed — this module is a *synthesis* layer.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


OverallVerdict = Literal[
    "AI-Ready",
    "Partially Ready",
    "Significant Gaps",
    "Not Ready",
]


class ModuleScorecard(BaseModel):
    """Summary of a single module's key findings."""

    module: str = Field(..., description="Module label (e.g. 'Data Quality')")
    module_number: str = Field(..., description="e.g. '2', '4', '5'")
    ran: bool = Field(..., description="Whether this module was executed")
    headline: str = Field(
        ...,
        description="One-line finding (e.g. '62% AI-ready — Moderate')",
    )
    score: float | None = Field(
        None,
        description="Primary 0–1 score (if applicable)",
        ge=0.0,
        le=1.0,
    )
    status: Literal["Strong", "Adequate", "Weak", "Critical", "Not Run"] = Field(
        ...,
        description="Quick health indicator",
    )
    details: list[str] = Field(
        default_factory=list,
        description="2-4 bullet points with key facts from this module",
    )


class RiskItem(BaseModel):
    """A specific risk or gap identified across all modules."""

    severity: Literal["Critical", "High", "Medium", "Low"] = Field(
        ..., description="Risk severity level",
    )
    area: str = Field(..., description="Which module/topic this relates to")
    description: str = Field(..., description="What the risk is")
    mitigation: str = Field(..., description="Recommended action to reduce this risk")


class ActionItem(BaseModel):
    """A prioritised action from the combined analysis."""

    priority: int = Field(..., ge=1, description="Execution order (1 = first)")
    action: str = Field(..., description="Concise imperative statement")
    source_module: str = Field(..., description="Which module generated this insight")
    impact: str = Field(..., description="What this action improves and by how much")
    effort: Literal["Low", "Medium", "High"] = Field(
        ..., description="Relative implementation effort",
    )
    timeframe: str = Field(
        ...,
        description="When to do this (e.g. 'Week 1-2', 'Month 1', 'Quarter 2')",
    )


class StrategicVerdict(BaseModel):
    """The final aggregated diagnostic report for the SME."""

    session_id: str

    # --- Overall verdict ---
    overall_readiness_score: float = Field(
        ...,
        description="Weighted composite 0.0–1.0 across all modules",
        ge=0.0,
        le=1.0,
    )
    verdict: OverallVerdict = Field(
        ...,
        description=(
            "AI-Ready ≥0.75 · Partially Ready ≥0.55 · "
            "Significant Gaps ≥0.35 · Not Ready <0.35"
        ),
    )
    verdict_summary: str = Field(
        ...,
        description="2-3 sentence executive summary of the overall diagnostic",
    )

    # --- Module scorecard ---
    scorecard: list[ModuleScorecard] = Field(
        default_factory=list,
        description="One entry per module showing whether it ran and key findings",
    )

    # --- Strengths & weaknesses ---
    strengths: list[str] = Field(
        default_factory=list,
        description="Top things the business is doing well",
    )
    weaknesses: list[str] = Field(
        default_factory=list,
        description="Top things that need improvement",
    )

    # --- Risk register ---
    risks: list[RiskItem] = Field(
        default_factory=list,
        description="Prioritised risk items across all modules",
    )

    # --- Action plan ---
    action_plan: list[ActionItem] = Field(
        default_factory=list,
        description="Ordered implementation roadmap",
    )

    # --- Key metrics summary ---
    key_metrics: dict[str, str] = Field(
        default_factory=dict,
        description="At-a-glance metric name→value pairs for the dashboard",
    )

    # --- Narrative ---
    executive_report: str = Field(
        ...,
        description="Full multi-paragraph executive report in Markdown",
    )
