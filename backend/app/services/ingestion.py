"""Core processing logic for the Data Ingestion module (Module 1).

Responsibilities:
  - Validate uploaded file (extension, size)
  - Load raw data into a Pandas DataFrame — NO modifications
  - Detect and flag data quality issues (missing values, duplicates, etc.)
  - Normalize workflow text (whitespace only — content untouched)

The raw DataFrame is preserved exactly as uploaded.
All detected problems are surfaced as DataIssue flags for Module 2 to act on.
"""

from __future__ import annotations

import io
import re
from pathlib import PurePosixPath

import pandas as pd

from app.core.config import settings
from app.schemas.ingestion import DataIssue, IssueType


class IngestionError(Exception):
    """Raised when ingestion validation / processing fails."""


# ---------------------------------------------------------------------------
# File validation
# ---------------------------------------------------------------------------

def validate_file(filename: str, size_bytes: int) -> str:
    """Return the lowercased file extension if valid, else raise."""
    ext = PurePosixPath(filename).suffix.lower()
    if ext not in settings.allowed_tabular_extensions:
        raise IngestionError(
            f"Unsupported file type '{ext}'. "
            f"Allowed: {', '.join(settings.allowed_tabular_extensions)}"
        )
    max_bytes = settings.max_upload_size_mb * 1024 * 1024
    if size_bytes > max_bytes:
        raise IngestionError(
            f"File too large ({size_bytes / 1024 / 1024:.1f} MB). "
            f"Maximum allowed: {settings.max_upload_size_mb} MB."
        )
    return ext


# ---------------------------------------------------------------------------
# DataFrame loading (raw, no modifications)
# ---------------------------------------------------------------------------

def load_dataframe(content: bytes, ext: str) -> pd.DataFrame:
    """Read raw bytes into a Pandas DataFrame. Nothing is changed."""
    buf = io.BytesIO(content)
    if ext == ".csv":
        df = pd.read_csv(buf)
    elif ext in (".xlsx", ".xls"):
        df = pd.read_excel(buf, engine="openpyxl")
    else:
        raise IngestionError(f"No reader available for extension '{ext}'.")
    return df


# ---------------------------------------------------------------------------
# Issue detection (observe only — nothing is mutated)
# ---------------------------------------------------------------------------

_DATE_KEYWORDS = {"date", "time", "created", "updated", "timestamp", "dt"}
_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def detect_issues(df: pd.DataFrame) -> list[DataIssue]:
    """Scan the raw DataFrame and return a list of detected data issues.

    The DataFrame is never modified. Issues are purely observational flags
    that Module 2 (Data Quality Analyzer) will score and report on.
    """
    issues: list[DataIssue] = []
    total_rows = len(df)

    # 1. Inconsistent column names (not snake_case) -------------------------
    bad_cols = [str(c) for c in df.columns if not _SNAKE_CASE_RE.match(str(c))]
    if bad_cols:
        issues.append(DataIssue(
            issue_type=IssueType.INCONSISTENT_COLUMN_NAMES,
            column=None,
            description=(
                f"{len(bad_cols)} column(s) have inconsistent naming "
                f"(spaces, mixed case, or special characters): "
                + ", ".join(f"'{c}'" for c in bad_cols)
            ),
            affected_count=len(bad_cols),
            severity="medium",
        ))

    # 2. Missing values per column -----------------------------------------
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        if null_count > 0:
            pct = null_count / total_rows * 100
            severity = "high" if pct >= 50 else ("medium" if pct >= 20 else "low")
            issues.append(DataIssue(
                issue_type=IssueType.MISSING_VALUES,
                column=str(col),
                description=(
                    f"Column '{col}' has {null_count} missing value(s) "
                    f"({pct:.1f}% of rows)."
                ),
                affected_count=null_count,
                severity=severity,  # type: ignore[arg-type]
            ))

    # 3. Duplicate rows ----------------------------------------------------
    dup_count = int(df.duplicated().sum())
    if dup_count > 0:
        issues.append(DataIssue(
            issue_type=IssueType.DUPLICATE_ROWS,
            column=None,
            description=(
                f"{dup_count} fully duplicate row(s) detected "
                f"({dup_count / total_rows * 100:.1f}% of dataset)."
            ),
            affected_count=dup_count,
            severity="high" if dup_count / total_rows > 0.1 else "medium",
        ))

    # 4. Date-like columns stored as strings (unparsed) --------------------
    for col in df.select_dtypes(include=["object"]).columns:
        col_lower = str(col).lower()
        if any(kw in col_lower for kw in _DATE_KEYWORDS):
            issues.append(DataIssue(
                issue_type=IssueType.UNPARSED_DATES,
                column=str(col),
                description=(
                    f"Column '{col}' appears to contain dates but is stored as "
                    f"plain text (dtype: object). Date parsing not yet applied."
                ),
                affected_count=None,
                severity="medium",
            ))

    # 5. Whitespace padding in string columns ------------------------------
    for col in df.select_dtypes(include=["object"]).columns:
        padded = int(df[col].dropna().apply(
            lambda x: isinstance(x, str) and (x != x.strip())
        ).sum())
        if padded > 0:
            issues.append(DataIssue(
                issue_type=IssueType.WHITESPACE_IN_STRINGS,
                column=str(col),
                description=(
                    f"Column '{col}' has {padded} value(s) with leading or "
                    f"trailing whitespace."
                ),
                affected_count=padded,
                severity="low",
            ))

    return issues


# ---------------------------------------------------------------------------
# Workflow text normalisation (whitespace only — words untouched)
# ---------------------------------------------------------------------------

def normalize_workflow_text(text: str) -> str:
    """Trim and collapse excessive blank lines. Content is not altered."""
    text = text.strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


# ---------------------------------------------------------------------------
# High-level orchestrator
# ---------------------------------------------------------------------------

def process_ingestion(
    file_content: bytes,
    filename: str,
    workflow_text: str,
    company_metadata: dict,
) -> tuple[pd.DataFrame, str, dict, list]:
    """Run the full ingestion pipeline.

    Returns:
        (raw_dataframe, normalised_workflow_text, company_metadata, data_issues)

    The raw_dataframe is completely unmodified — no columns renamed,
    no rows dropped, no types coerced.
    """
    ext = validate_file(filename, len(file_content))
    df = load_dataframe(file_content, ext)
    issues = detect_issues(df)
    clean_text = normalize_workflow_text(workflow_text)
    return df, clean_text, company_metadata, issues
