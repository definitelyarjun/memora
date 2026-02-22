"""Pydantic models for Module 2 — Data Quality & DPDP Compliance Scanner.

FoundationIQ 3.0 (Startup Edition)

Metrics calculated:
  Metric 2: Data Quality Score (>85% required to proceed without cleanup mandate)
  Metric 6: DPDP Risk Level (flags columns that must be anonymised)
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


ReadinessLevel = Literal["High", "Moderate", "Low", "Critical"]
DPDPRiskLevel = Literal["Low", "Medium", "High", "Critical"]


# ---------------------------------------------------------------------------
# PII finding
# ---------------------------------------------------------------------------

class PIIFinding(BaseModel):
    """A single PII detection in a column."""

    column: str
    pii_type: str = Field(
        ...,
        description="Type of PII detected: email, phone, credit_card, aadhaar, pan, ip_address",
    )
    sample_count: int = Field(
        ..., description="Number of values matching the PII pattern"
    )
    total_values: int = Field(
        ..., description="Total non-null values in the column"
    )
    exposure_pct: float = Field(
        ..., description="Percentage of column values that are PII (0-100)"
    )
    risk_level: Literal["Low", "Medium", "High"] = Field(
        ..., description="Risk: High >50%, Medium 10-50%, Low <10%"
    )
    recommendation: str = Field(
        ..., description="Specific anonymisation / masking recommendation"
    )


# ---------------------------------------------------------------------------
# Per-column quality
# ---------------------------------------------------------------------------

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
    pii_types: list[str] = Field(
        default_factory=list,
        description="PII types found in this column (e.g. email, phone)",
    )


# ---------------------------------------------------------------------------
# DPDP Compliance section
# ---------------------------------------------------------------------------

class DPDPComplianceReport(BaseModel):
    """DPDP (Digital Personal Data Protection) compliance findings."""

    risk_level: DPDPRiskLevel = Field(
        ...,
        description="Overall DPDP risk: Critical = PII in >3 columns, High = 2-3, Medium = 1, Low = 0",
    )
    total_pii_columns: int = Field(
        0, description="Number of columns containing PII"
    )
    total_pii_values: int = Field(
        0, description="Total PII values detected across all columns"
    )
    pii_findings: list[PIIFinding] = Field(
        default_factory=list,
        description="Detailed PII findings per column",
    )
    compliance_warnings: list[str] = Field(
        default_factory=list,
        description="Actionable DPDP compliance warnings",
    )
    llm_api_safe: bool = Field(
        True,
        description="False if any High-risk PII columns exist — data must be anonymised before LLM API calls",
    )


# ---------------------------------------------------------------------------
# Full quality report
# ---------------------------------------------------------------------------

class QualityReport(BaseModel):
    """Full data quality + DPDP compliance report for a session.

    Metric 2: data_quality_score (>0.85 = pass)
    Metric 6: dpdp_compliance.risk_level
    """

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
        description="1 − (missing_cells / total_cells), adjusted for volume",
        ge=0.0,
        le=1.0,
    )
    deduplication_score: float = Field(
        ...,
        description="1 − (duplicate_rows / total_rows), includes fuzzy matching",
        ge=0.0,
        le=1.0,
    )
    consistency_score: float = Field(
        ...,
        description="Penalises inconsistent column names, whitespace padding",
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
        description="Fraction of automated workflow steps (0.0 = all manual)",
        ge=0.0,
        le=1.0,
    )
    tool_maturity_score: float = Field(
        ...,
        description="Weighted score based on tool sophistication tier",
        ge=0.0,
        le=1.0,
    )
    data_coverage_score: float = Field(
        ...,
        description="Score based on which CSV files were uploaded",
        ge=0.0,
        le=1.0,
    )

    # --- Operational metadata ---------------------------------------------
    total_workflow_steps: int = Field(0)
    automated_steps: int = Field(0)
    manual_steps: int = Field(0)
    tools_detected: list[str] = Field(default_factory=list)
    documents_provided: list[str] = Field(default_factory=list)

    # --- Metric 2: Data Quality Score (composite) -------------------------
    data_quality_score: float = Field(
        ...,
        description=(
            "Weighted composite: completeness(25%) + deduplication(20%) "
            "+ consistency(15%) + structural_integrity(10%) "
            "+ process_digitisation(15%) + tool_maturity(5%) + data_coverage(10%). "
            ">0.85 = no cleanup mandate."
        ),
        ge=0.0,
        le=1.0,
    )
    # Legacy alias
    ai_readiness_score: float = Field(
        ..., description="Alias for data_quality_score (backward compat)", ge=0.0, le=1.0
    )
    quality_pass: bool = Field(
        ..., description="True if data_quality_score >= 0.85"
    )
    readiness_level: ReadinessLevel = Field(
        ...,
        description="High ≥0.80 · Moderate ≥0.60 · Low ≥0.40 · Critical <0.40",
    )

    # --- Metric 6: DPDP Compliance ----------------------------------------
    dpdp_compliance: DPDPComplianceReport = Field(
        ..., description="DPDP compliance scan results including PII findings"
    )

    # --- Per-column breakdown ---------------------------------------------
    column_quality: list[ColumnQuality] = Field(default_factory=list)

    # --- Actionable guidance ----------------------------------------------
    top_recommendations: list[str] = Field(
        default_factory=list,
        description="Up to 10 prioritised recommendations for improving quality & compliance",
    )
