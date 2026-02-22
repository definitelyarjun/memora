"""Pydantic models for Module 5 — Data Consolidation Recommendation Engine.

Analyses scattered tools and data sources across a company's operations and
produces concrete, actionable recommendations for unification.

Key concepts:
  DataSilo       — an isolated tool/medium where data lives (e.g. "Excel", "Paper ledger")
  DataFlow       — a detected manual hand-off of data between two silos
  RedundancyFlag — two silos that store overlapping information
  MigrationStep  — a specific, feasible action to consolidate
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------

ToolTier = Literal["Enterprise", "Productivity", "Informal"]


class DataSilo(BaseModel):
    """An isolated data store or tool identified in the company's operations."""

    name: str = Field(..., description="Tool or medium name (e.g. 'Excel', 'Paper ledger')")
    tier: ToolTier = Field(
        ...,
        description="Enterprise (ERP/CRM/POS), Productivity (Excel/Sheets), or Informal (paper/WhatsApp)",
    )
    data_types: list[str] = Field(
        default_factory=list,
        description="Kinds of data stored here (e.g. 'Sales', 'Invoices', 'Payroll')",
    )
    used_by: list[str] = Field(
        default_factory=list,
        description="Roles/actors who touch this tool in the workflow",
    )
    workflow_steps: list[int] = Field(
        default_factory=list,
        description="Step numbers where this silo appears",
    )
    weaknesses: list[str] = Field(
        default_factory=list,
        description="Problems with this silo (e.g. 'No backup', 'Not searchable')",
    )


class DataFlow(BaseModel):
    """A detected manual data transfer between two silos."""

    from_silo: str
    to_silo: str
    method: str = Field(
        ...,
        description="How data moves: 'Manual re-entry', 'Copy-paste', 'Verbal', 'Paper hand-off'",
    )
    step_number: int | None = None
    description: str = Field(..., description="What data is being moved and why")
    risk: str = Field(
        ...,
        description="Error risk: 'High' (verbal/manual), 'Medium' (copy-paste), 'Low' (automated)",
    )


class RedundancyFlag(BaseModel):
    """Two silos holding overlapping data — a consolidation opportunity."""

    silo_a: str
    silo_b: str
    overlapping_data: str = Field(
        ...,
        description="What data is duplicated (e.g. 'Daily sales totals')",
    )
    recommendation: str = Field(
        ...,
        description="Which silo should be the single source of truth",
    )


class MigrationStep(BaseModel):
    """A specific, feasible action to consolidate data infrastructure."""

    priority: int = Field(
        ...,
        description="Execution order: 1 = first, higher = later",
        ge=1,
    )
    action: str = Field(
        ...,
        description="Concise imperative statement (e.g. 'Replace paper ledger with Tally')",
    )
    from_tool: str = Field(..., description="Current tool or medium being replaced")
    to_tool: str = Field(..., description="Recommended replacement tool")
    rationale: str = Field(
        ...,
        description="Why this migration is recommended and what it solves",
    )
    effort: Literal["Low", "Medium", "High"] = Field(
        ...,
        description="Implementation effort: Low (days), Medium (weeks), High (months)",
    )
    affected_roles: list[str] = Field(
        default_factory=list,
        description="Roles that need retraining",
    )
    data_at_risk: str = Field(
        "",
        description="Data that needs careful migration (if any)",
    )


class UnifiedSchemaColumn(BaseModel):
    """A column in the recommended unified data schema."""

    name: str
    source: str = Field(
        ...,
        description="Where this data currently lives (tool or column name)",
    )
    dtype: str = Field(
        ...,
        description="Recommended data type (e.g. 'datetime', 'decimal', 'text')",
    )
    notes: str = ""


class UnifiedSchemaRecommendation(BaseModel):
    """Recommended unified schema to replace scattered data sources."""

    table_name: str = Field(
        ...,
        description="Logical table name (e.g. 'daily_transactions', 'inventory_log')",
    )
    purpose: str
    columns: list[UnifiedSchemaColumn] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Top-level report
# ---------------------------------------------------------------------------

class ConsolidationReport(BaseModel):
    """Full data consolidation analysis for a session."""

    session_id: str

    # Discovery
    silos: list[DataSilo] = Field(default_factory=list)
    data_flows: list[DataFlow] = Field(default_factory=list)
    redundancies: list[RedundancyFlag] = Field(default_factory=list)

    # Schema
    unified_schemas: list[UnifiedSchemaRecommendation] = Field(default_factory=list)

    # Action plan
    migration_steps: list[MigrationStep] = Field(default_factory=list)

    # Summary stats
    total_silos: int = 0
    informal_silos: int = Field(
        0,
        description="Number of Informal-tier silos (paper, WhatsApp, etc.)",
    )
    manual_flows: int = Field(
        0,
        description="Number of manual data transfer points",
    )
    consolidation_score: float = Field(
        ...,
        description=(
            "0.0–1.0 score: how well-consolidated the current setup is. "
            "1.0 = fully unified, 0.0 = completely fragmented."
        ),
        ge=0.0,
        le=1.0,
    )

    # Narrative
    executive_summary: str = Field(
        ...,
        description="2-3 sentence plain-English summary of findings",
    )
    top_recommendations: list[str] = Field(
        default_factory=list,
        description="Ordered list of the most impactful consolidation actions",
    )
