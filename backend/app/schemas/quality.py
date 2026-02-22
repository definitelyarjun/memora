"""Pydantic models for Module 2 — Data Quality & AI Readiness Analyzer."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


ReadinessLevel = Literal["High", "Moderate", "Low", "Critical"]


class ColumnQuality(BaseModel):
    """Quality breakdown for a single column."""

    name: str
    dtype: str
    completeness: float = Field(
        ...,
        description="Fraction of non-null values (1.0 = fully complete)",
        ge=0.0,
        le=1.0,
    )
    null_count: int
    issue_types: list[str] = Field(
        default_factory=list,
        description="Issue type labels detected for this column",
    )


class QualityReport(BaseModel):
    """Full data quality and AI readiness report for a session."""

    session_id: str

    # --- Raw counts -------------------------------------------------------
    row_count: int
    column_count: int
    total_cells: int
    missing_cells: int
    duplicate_rows: int

    # --- Data dimension scores (0.0 – 1.0) --------------------------------
    completeness_score: float = Field(
        ...,
        description="1 − (missing_cells / total_cells)",
        ge=0.0,
        le=1.0,
    )
    deduplication_score: float = Field(
        ...,
        description="1 − (duplicate_rows / total_rows)",
        ge=0.0,
        le=1.0,
    )
    consistency_score: float = Field(
        ...,
        description=(
            "Penalises inconsistent column names, whitespace padding, "
            "and mixed data types"
        ),
        ge=0.0,
        le=1.0,
    )
    structural_integrity_score: float = Field(
        ...,
        description="Penalises unparsed date columns and mixed dtypes",
        ge=0.0,
        le=1.0,
    )

    # --- Operational dimension scores (0.0 – 1.0) -------------------------
    process_digitisation_score: float = Field(
        ...,
        description=(
            "Fraction of workflow steps that are Automated. "
            "0.0 = all manual, 1.0 = fully digital."
        ),
        ge=0.0,
        le=1.0,
    )
    tool_maturity_score: float = Field(
        ...,
        description=(
            "Weighted score based on tool sophistication. "
            "Paper/WhatsApp = low, POS/ERP/CRM = high."
        ),
        ge=0.0,
        le=1.0,
    )
    data_coverage_score: float = Field(
        ...,
        description=(
            "Score based on which document types were provided. "
            "Sales only = 0.40, +invoices = 0.65, +payroll = 0.85, +inventory = 1.0."
        ),
        ge=0.0,
        le=1.0,
    )

    # --- Operational metadata ---------------------------------------------
    total_workflow_steps: int = Field(
        0, description="Total steps extracted from workflow analysis"
    )
    automated_steps: int = Field(
        0, description="Steps classified as Automated"
    )
    manual_steps: int = Field(
        0, description="Steps classified as Manual"
    )
    tools_detected: list[str] = Field(
        default_factory=list,
        description="Tools found in company metadata",
    )
    documents_provided: list[str] = Field(
        default_factory=list,
        description="Document types uploaded: sales, invoices, payroll, inventory",
    )

    # --- Composite --------------------------------------------------------
    ai_readiness_score: float = Field(
        ...,
        description=(
            "Weighted composite: completeness(17%) + deduplication(12%) "
            "+ consistency(11%) + structural_integrity(8%) "
            "+ process_digitisation(25%) + tool_maturity(12%) + data_coverage(15%)"
        ),
        ge=0.0,
        le=1.0,
    )
    readiness_level: ReadinessLevel = Field(
        ...,
        description="High ≥0.80 · Moderate ≥0.60 · Low ≥0.40 · Critical <0.40",
    )

    # --- Per-column breakdown ---------------------------------------------
    column_quality: list[ColumnQuality] = Field(default_factory=list)

    # --- Actionable guidance ----------------------------------------------
    top_recommendations: list[str] = Field(
        default_factory=list,
        description="Up to 7 prioritised recommendations for improving readiness",
    )
