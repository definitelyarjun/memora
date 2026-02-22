"""Pydantic models for Module 4 — Automation Opportunity Detector.

Each workflow step is evaluated against a rule-based classifier that
determines whether it is an automation candidate, what type of automation
applies, and how confident the assessment is.

Automation types
----------------
  RPA            Robotic Process Automation — repetitive, rule-based, structured data
  Digital Form   Replace paper/verbal handoff with a digital form or app
  API Integration Connect existing tools via APIs to eliminate manual transfer
  AI/ML          Requires machine-learning model (needs high data readiness)
  Decision Engine Rule-based decision that can be codified (if/else logic)
  Not Recommended Step is inherently human or data readiness is too low

Confidence scoring
------------------
  High     ≥ 0.80  — strong signal from keywords, step type, and data readiness
  Medium   ≥ 0.50  — likely automatable but some ambiguity
  Low      < 0.50  — weak signal, needs human review
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


AutomationType = Literal[
    "RPA",
    "Digital Form",
    "API Integration",
    "AI/ML",
    "Decision Engine",
    "Not Recommended",
]

ConfidenceLevel = Literal["High", "Medium", "Low"]


class AutomationCandidate(BaseModel):
    """Automation analysis for a single workflow step."""

    step_number: int
    description: str
    actor: str
    current_step_type: Literal["Manual", "Automated", "Decision", "Unknown"]
    tool_used: str | None = None

    # --- Classification output ---
    is_candidate: bool = Field(
        ...,
        description="Whether this step is a viable automation candidate",
    )
    automation_type: AutomationType = Field(
        ...,
        description="Recommended automation approach for this step",
    )
    confidence: float = Field(
        ...,
        description="Confidence score 0.0–1.0 for automation recommendation",
        ge=0.0,
        le=1.0,
    )
    confidence_level: ConfidenceLevel
    reasoning: str = Field(
        ...,
        description="Human-readable explanation of why this step was classified this way",
    )
    estimated_effort: Literal["Low", "Medium", "High"] = Field(
        ...,
        description="Relative effort to implement this automation",
    )
    priority: Literal["Critical", "High", "Medium", "Low", "Skip"] = Field(
        ...,
        description="Implementation priority based on impact × feasibility",
    )


class AutomationSummary(BaseModel):
    """Aggregate statistics for the automation report."""

    total_steps: int
    automatable_steps: int
    already_automated: int
    not_recommended: int
    automation_coverage: float = Field(
        ...,
        description=(
            "Fraction of steps that are either already automated or "
            "identified as automation candidates. 1.0 = full coverage."
        ),
        ge=0.0,
        le=1.0,
    )
    avg_confidence: float = Field(
        ...,
        description="Mean confidence across all candidates (excludes non-candidates)",
        ge=0.0,
        le=1.0,
    )
    by_type: dict[str, int] = Field(
        default_factory=dict,
        description="Count of candidates grouped by AutomationType",
    )
    by_priority: dict[str, int] = Field(
        default_factory=dict,
        description="Count of candidates grouped by priority level",
    )


class AutomationReport(BaseModel):
    """Full automation opportunity report for a session."""

    session_id: str
    ai_readiness_score: float = Field(
        ...,
        description="Carried forward from Module 2 quality report",
    )
    readiness_level: str

    # --- Per-step analysis -------------------------------------------------
    candidates: list[AutomationCandidate] = Field(default_factory=list)

    # --- Aggregate ---------------------------------------------------------
    summary: AutomationSummary

    # --- Actionable guidance -----------------------------------------------
    top_recommendations: list[str] = Field(
        default_factory=list,
        description="Up to 5 prioritised automation recommendations",
    )
    quick_wins: list[str] = Field(
        default_factory=list,
        description="Steps that can be automated with minimal effort",
    )
