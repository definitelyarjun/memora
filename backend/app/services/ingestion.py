"""Core processing logic for Module 1 — Startup Ingestion & Profiling.

FoundationIQ 3.0 (Startup Edition)

Responsibilities:
  - Validate uploaded CSV files (extension, size)
  - Load raw data into Pandas DataFrames — NO modifications
  - Validate expected columns for each of the 3 CSV types:
      org_chart.csv      → role, department, salary (+ optional)
      expenses.csv       → category, amount, month (+ optional)
      sales_inquiries.csv→ inquiry_date, payment_date, repeat_customer (+ optional)
  - Detect and flag data quality issues (missing values, duplicates, etc.)
  - Return raw DataFrames + issues for downstream modules

The raw DataFrames are preserved exactly as uploaded.
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
# Expected columns per CSV type (lowercase for matching)
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "org_chart": ["job_title", "department", "monthly_salary_inr"],
    "expenses": ["category", "amount_inr", "date"],
    "sales_inquiries": ["inquiry_date", "payment_date", "repeat_customer_flag"],
}


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
# Expected-column validation
# ---------------------------------------------------------------------------

def check_expected_columns(
    df: pd.DataFrame, file_type: str
) -> list[DataIssue]:
    """Check if a DataFrame contains the expected columns for its file type.

    Uses case-insensitive + whitespace-stripped matching.
    Returns a list of DataIssue items for any missing columns.
    """
    expected = EXPECTED_COLUMNS.get(file_type, [])
    if not expected:
        return []

    actual_lower = {str(c).strip().lower().replace(" ", "_") for c in df.columns}
    missing = [col for col in expected if col not in actual_lower]

    issues: list[DataIssue] = []
    if missing:
        issues.append(DataIssue(
            issue_type=IssueType.MISSING_EXPECTED_COLUMNS,
            column=None,
            description=(
                f"File type '{file_type}' is missing expected column(s): "
                + ", ".join(f"'{c}'" for c in missing)
                + f".  Found columns: {', '.join(str(c) for c in df.columns)}"
            ),
            affected_count=len(missing),
            severity="high",
        ))
    return issues


# ---------------------------------------------------------------------------
# Issue detection (observe only — nothing is mutated)
# ---------------------------------------------------------------------------

_DATE_KEYWORDS = {"date", "time", "created", "updated", "timestamp", "dt"}
_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_TITLE_CASE_RE = re.compile(r"^[A-Z][A-Za-z0-9]*(_[A-Z][A-Za-z0-9]*)*$")
_SPACE_OR_SPECIAL_RE = re.compile(r"[ !@#$%^&*()+={};:'\"<>,?/\\|`~]")


def _col_style(col: str) -> str:
    """Classify a column name's style for mixed-convention detection."""
    if _SNAKE_CASE_RE.match(col):
        return "snake"
    if _TITLE_CASE_RE.match(col):
        return "title"
    if col == col.upper():
        return "upper"
    if col == col.lower():
        return "lower"
    return "mixed"


def detect_issues(df: pd.DataFrame) -> list[DataIssue]:
    """Scan the raw DataFrame and return a list of detected data issues.

    The DataFrame is never modified. Issues are purely observational flags
    that Module 2 (Data Quality Analyzer) will score and report on.
    """
    issues: list[DataIssue] = []
    total_rows = len(df)

    # 1. Inconsistent column names ----------------------------------------
    # Flag columns with spaces / special characters (always a problem),
    # OR report if columns within the same file use multiple naming styles.
    space_cols = [str(c) for c in df.columns if _SPACE_OR_SPECIAL_RE.search(str(c))]
    if space_cols:
        issues.append(DataIssue(
            issue_type=IssueType.INCONSISTENT_COLUMN_NAMES,
            column=None,
            description=(
                f"{len(space_cols)} column(s) contain spaces or special "
                f"characters: " + ", ".join(f"'{c}'" for c in space_cols)
            ),
            affected_count=len(space_cols),
            severity="medium",
        ))
    else:
        styles = {_col_style(str(c)) for c in df.columns}
        if len(styles) > 1:
            bad_cols = [
                str(c) for c in df.columns
                if _col_style(str(c)) != max(styles, key=lambda s: sum(
                    1 for cc in df.columns if _col_style(str(cc)) == s
                ))
            ]
            issues.append(DataIssue(
                issue_type=IssueType.INCONSISTENT_COLUMN_NAMES,
                column=None,
                description=(
                    f"Column naming uses mixed conventions ({', '.join(sorted(styles))}). "
                    f"Inconsistent column(s): "
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
# Single-file processing helper
# ---------------------------------------------------------------------------

def process_single_csv(
    file_content: bytes,
    filename: str,
    file_type: str,
) -> tuple[pd.DataFrame, list[DataIssue]]:
    """Validate, load, and detect issues in a single CSV/Excel file.

    Args:
        file_content: Raw file bytes.
        filename: Original filename (used for extension detection).
        file_type: One of 'org_chart', 'expenses', 'sales_inquiries'.

    Returns:
        (raw_dataframe, data_issues)
    """
    ext = validate_file(filename, len(file_content))
    df = load_dataframe(file_content, ext)
    issues = check_expected_columns(df, file_type)
    issues.extend(detect_issues(df))
    return df, issues


# ---------------------------------------------------------------------------
# High-level orchestrator (backward compat wrapper)
# ---------------------------------------------------------------------------

def process_ingestion(
    file_content: bytes,
    filename: str,
    workflow_text: str = "",
    company_metadata: dict | None = None,
) -> tuple[pd.DataFrame, str, dict, list]:
    """Legacy ingestion pipeline — kept for backward compatibility.

    Returns:
        (raw_dataframe, workflow_text, company_metadata, data_issues)
    """
    ext = validate_file(filename, len(file_content))
    df = load_dataframe(file_content, ext)
    issues = detect_issues(df)
    return df, workflow_text.strip(), company_metadata or {}, issues
