"""Module 2 — Data Quality & AI Readiness scoring logic.

All calculations are deterministic and LLM-free.
Inputs come entirely from what Module 1 already stored in the session:
  - raw_dataframe       → completeness, deduplication
  - data_issues         → consistency, structural integrity
  - workflow_analysis   → process digitisation ratio
  - company_metadata    → tool maturity

Scoring dimensions and weights
--------------------------------
  Completeness              17 %   1 − (null_cells / total_cells)
  Deduplication             12 %   1 − (dup_rows / total_rows)
  Consistency               11 %   penalty for bad names, whitespace, mixed types
  Structural Integrity       8 %   penalty for unparsed dates and mixed dtypes
  Process Digitisation      25 %   automated_steps / total_steps from workflow
  Tool Maturity             12 %   weighted score by tool sophistication tier
  Data Coverage             15 %   which document types were uploaded

AI Readiness levels
-------------------
  High       ≥ 0.80
  Moderate   ≥ 0.60
  Low        ≥ 0.40
  Critical   < 0.40
"""

from __future__ import annotations

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.ingestion import IssueType
from app.schemas.quality import ColumnQuality, QualityReport, ReadinessLevel


# ---------------------------------------------------------------------------
# Weights (must sum to 1.0)
# ---------------------------------------------------------------------------

_W_COMPLETENESS = 0.17
_W_DEDUPLICATION = 0.12
_W_CONSISTENCY = 0.11
_W_STRUCTURAL = 0.08
_W_PROCESS_DIGITISATION = 0.25
_W_TOOL_MATURITY = 0.12
_W_DATA_COVERAGE = 0.15


# ---------------------------------------------------------------------------
# Tool classification tiers  (score 0.0 – 1.0 each)
# ---------------------------------------------------------------------------

# Tier 3 — high sophistication: full enterprise / domain-specific digital
_TIER3_KEYWORDS: set[str] = {
    "erp", "sap", "oracle", "zoho", "salesforce", "hubspot", "crm",
    "pos", "square", "toast", "lightspeed", "shopify", "woocommerce",
    "quickbooks", "xero", "freshbooks", "netsuite", "odoo",
    "power bi", "tableau", "looker", "metabase",
    "jira", "asana", "monday", "notion", "trello",
    "slack", "teams", "zendesk", "freshdesk",
    "aws", "azure", "gcp", "firebase",
    "stripe", "razorpay", "paypal",
}

# Tier 2 — medium: generic productivity software, some digital process
_TIER2_KEYWORDS: set[str] = {
    "excel", "google sheets", "airtable", "tally",
    "google forms", "typeform", "surveymonkey",
    "google drive", "dropbox", "onedrive",
    "canva", "mailchimp", "gmail", "outlook",
    "google pay", "phonepe", "paytm", "upi",
    "zoom", "google meet",
}

# Tier 1 — low: informal / non-digital  (everything else defaults here)
_TIER1_KEYWORDS: set[str] = {
    "whatsapp", "sms", "phone", "paper", "pen", "calculator",
    "diary", "logbook", "ledger", "notebook",
}

_TIER_SCORES = {3: 1.0, 2: 0.5, 1: 0.15}


# ---------------------------------------------------------------------------
# Volume thresholds for row-count scoring within completeness
# ---------------------------------------------------------------------------
# Each tuple: (exclusive upper bound, score for rows below that bound)
# A 3-row test CSV should not score the same as a 10 000-row production dataset.

_VOLUME_THRESHOLDS: list[tuple[int, float]] = [
    (20,    0.05),   # < 20 rows   — not enough to learn from
    (100,   0.25),   # 20–99       — very small sample
    (300,   0.50),   # 100–299     — limited history
    (1000,  0.70),   # 300–999     — reasonable dataset
    (5000,  0.85),   # 1 000–4 999 — good dataset
]                    # ≥ 5 000 rows → 1.0  (production-grade)


def _volume_score(row_count: int) -> float:
    """Map a row count to a 0–1 volume quality score."""
    for threshold, score in _VOLUME_THRESHOLDS:
        if row_count < threshold:
            return score
    return 1.0


# ---------------------------------------------------------------------------
# Internal helpers — data dimensions
# ---------------------------------------------------------------------------

def _completeness(df: pd.DataFrame) -> tuple[float, int, int]:
    """Return (score, missing_cells, total_cells).

    Score blends two sub-components:
      - Cell density (65 %): 1 − null_cells / total_cells
      - Row volume  (35 %): scored by _volume_score() against SME data norms

    A 25-row dataset can never score the same as a 25 000-row dataset, even if
    both are 100 % complete cell-wise.
    """
    total = df.shape[0] * df.shape[1]
    missing = int(df.isna().sum().sum())
    cell_score = 1.0 - (missing / total) if total else 1.0
    vol_score = _volume_score(df.shape[0])
    score = round(cell_score * 0.65 + vol_score * 0.35, 4)
    return score, missing, total


def _deduplication(df: pd.DataFrame) -> tuple[float, int, int]:
    """Return (score, exact_duplicate_count, fuzzy_duplicate_count).

    Two passes:
      1. Exact duplicates  — identical rows as-is.
      2. Fuzzy duplicates  — rows that become identical after normalisation:
             strings  → lowercase + strip whitespace
             floats   → rounded to 2 decimal places

    Exact dupes penalised 100 %; fuzzy dupes penalised 70 % (less certain).
    The ``duplicate_rows`` value exposed in the report shows exact dupes only
    (for transparency); the score uses both.
    """
    n = len(df)
    if n == 0:
        return 1.0, 0, 0

    exact_dups = int(df.duplicated().sum())

    # Normalised copy for fuzzy pass
    norm_df = df.copy()
    for col in norm_df.columns:
        if norm_df[col].dtype == object:
            norm_df[col] = norm_df[col].astype(str).str.lower().str.strip()
        elif pd.api.types.is_float_dtype(norm_df[col]):
            norm_df[col] = norm_df[col].round(2)

    fuzzy_total = int(norm_df.duplicated().sum())
    fuzzy_extra = max(0, fuzzy_total - exact_dups)

    effective_dups = exact_dups + fuzzy_extra * 0.70
    score = 1.0 - (effective_dups / n)
    return round(max(0.0, score), 4), exact_dups, fuzzy_extra


def _consistency(data_issues: list, col_count: int) -> float:
    """Score: 1.0 penalised by naming + whitespace issues."""
    if col_count == 0:
        return 1.0

    penalty = 0.0
    for issue in data_issues:
        if issue.issue_type == IssueType.INCONSISTENT_COLUMN_NAMES:
            affected = issue.affected_count or 0
            penalty += (affected / col_count) * 0.40

        elif issue.issue_type == IssueType.WHITESPACE_IN_STRINGS:
            penalty += (1 / col_count) * 0.30

    return round(max(0.0, 1.0 - penalty), 4)


def _structural_integrity(data_issues: list, col_count: int) -> float:
    """Score: 1.0 penalised by unparsed dates and mixed dtype issues."""
    if col_count == 0:
        return 1.0

    penalty = 0.0
    for issue in data_issues:
        if issue.issue_type == IssueType.UNPARSED_DATES:
            penalty += (1 / col_count) * 0.50

        elif issue.issue_type == IssueType.MIXED_DTYPES:
            penalty += (1 / col_count) * 0.30

    return round(max(0.0, 1.0 - penalty), 4)


# ---------------------------------------------------------------------------
# Internal helpers — operational dimensions (NEW)
# ---------------------------------------------------------------------------

def _process_digitisation(workflow_analysis) -> tuple[float, int, int, int]:
    """Score based on automated vs manual workflow steps.

    Returns (score, total_steps, automated_count, manual_count).
    If no workflow analysis exists, returns a pessimistic default (0.10).
    """
    if workflow_analysis is None:
        return 0.10, 0, 0, 0

    steps = getattr(workflow_analysis, "steps", [])
    if not steps:
        return 0.10, 0, 0, 0

    total = len(steps)
    automated = sum(1 for s in steps if getattr(s, "step_type", "") == "Automated")
    manual = sum(1 for s in steps if getattr(s, "step_type", "") == "Manual")

    # Score = automated fraction, with a small bonus for Decision steps
    # (decisions imply structured process even if not fully automated)
    decision = sum(1 for s in steps if getattr(s, "step_type", "") == "Decision")
    effective_automated = automated + (decision * 0.3)

    score = effective_automated / total if total else 0.0
    return round(min(1.0, score), 4), total, automated, manual


def _classify_tool(tool_name: str) -> int:
    """Return tier (3=high, 2=medium, 1=low) for a single tool name.

    Uses exact word matching to avoid substring collisions
    (e.g. 'sap' inside 'whatsapp').
    """
    t = tool_name.lower().strip()
    words = set(t.replace("-", " ").replace("_", " ").split())

    for keyword in _TIER3_KEYWORDS:
        kw_words = set(keyword.split())
        if kw_words.issubset(words) or keyword == t:
            return 3
    for keyword in _TIER2_KEYWORDS:
        kw_words = set(keyword.split())
        if kw_words.issubset(words) or keyword == t:
            return 2
    for keyword in _TIER1_KEYWORDS:
        kw_words = set(keyword.split())
        if kw_words.issubset(words) or keyword == t:
            return 1
    # Unknown tool defaults to tier 2 (benefit of the doubt — at least it's digital)
    return 2


def _tool_maturity(tools_used: list[str]) -> tuple[float, list[str]]:
    """Score based on the sophistication of tools the company uses.

    Returns (score, tools_list).
    If no tools are listed, return a pessimistic default (0.05).
    """
    if not tools_used:
        return 0.05, []

    tier_scores = [_TIER_SCORES[_classify_tool(t)] for t in tools_used]

    # Weighted: best tool counts most (50%), average of rest is 50%
    # This rewards having at least ONE good tool even if others are basic
    sorted_scores = sorted(tier_scores, reverse=True)
    best = sorted_scores[0]
    if len(sorted_scores) > 1:
        avg_rest = sum(sorted_scores[1:]) / len(sorted_scores[1:])
        score = best * 0.50 + avg_rest * 0.50
    else:
        score = best

    return round(score, 4), tools_used


# ---------------------------------------------------------------------------
# Data coverage score (NEW)
# ---------------------------------------------------------------------------

# Points for each document type  (must sum to 1.0)
_COVERAGE_POINTS: dict[str, float] = {
    "sales":     0.40,   # Always present
    "invoices":  0.25,   # Supplier/procurement records
    "payroll":   0.20,   # Staff/HR structure
    "inventory": 0.15,   # Stock management
}


def _data_coverage(documents_provided: list[str], supplementary_doc_stats: dict) -> float:
    """Score based on which document types were uploaded AND their content quality.

    Base points per doc type (sales=0.40, invoices=0.25, payroll=0.20, inventory=0.15).

    For supplementary docs (invoices/payroll/inventory), file content is validated:
      No stats provided       → full base points  (backward-compatible / test fallback)
      PDF (unstructured)      → 80 % of base points
      CSV/Excel ≥ 50 rows     → 100 % of base points
      CSV/Excel 10–49 rows    → 60 % of base points
      CSV/Excel < 10 rows     → 20 % of base points  (nearly empty)
      Unreadable / corrupt    → 30 % of base points

    An empty CSV uploaded just to game the score is worth almost nothing.
    """
    if not documents_provided:
        return 0.40  # assume sales at minimum

    score = 0.0
    for doc in documents_provided:
        doc_key = doc.lower()
        base = _COVERAGE_POINTS.get(doc_key, 0.0)

        if doc_key == "sales":
            score += base          # main file — volume already penalised via completeness
            continue

        stats = supplementary_doc_stats.get(doc_key)
        if stats is None:
            score += base          # no stats → full credit (backward-compat / tests)
        elif not stats.get("readable", True):
            score += base * 0.30   # unreadable / corrupt file
        elif stats.get("is_pdf"):
            score += base * 0.80   # PDF — can't count rows, give benefit of doubt
        else:
            rows = stats.get("row_count", 0)
            if rows >= 50:
                score += base          # full credit
            elif rows >= 10:
                score += base * 0.60   # partial — some data but thin
            else:
                score += base * 0.20   # nearly empty

    return round(min(1.0, score), 4)


# ---------------------------------------------------------------------------
# Readiness level
# ---------------------------------------------------------------------------

def _readiness_level(score: float) -> ReadinessLevel:
    if score >= 0.80:
        return "High"
    if score >= 0.60:
        return "Moderate"
    if score >= 0.40:
        return "Low"
    return "Critical"


# ---------------------------------------------------------------------------
# Per-column quality
# ---------------------------------------------------------------------------

def _column_quality(df: pd.DataFrame, data_issues: list) -> list[ColumnQuality]:
    """Per-column quality breakdown."""
    total_rows = len(df)

    col_issue_map: dict[str, set[str]] = {}
    for issue in data_issues:
        if issue.column:
            col_issue_map.setdefault(issue.column, set()).add(issue.issue_type.value)

    results = []
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        completeness = round(1.0 - (null_count / total_rows), 4) if total_rows else 1.0
        results.append(ColumnQuality(
            name=str(col),
            dtype=str(df[col].dtype),
            completeness=completeness,
            null_count=null_count,
            issue_types=sorted(col_issue_map.get(str(col), set())),
        ))
    return results


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _recommendations(
    completeness: float,
    deduplication: float,
    consistency: float,
    structural: float,
    process_dig: float,
    tool_mat: float,
    data_cov: float,
    df: pd.DataFrame,
    data_issues: list,
    total_steps: int,
    manual_steps: int,
    tools_used: list[str],
    documents_provided: list[str],
) -> list[str]:
    """Generate up to 7 prioritised, actionable recommendations."""
    recs: list[str] = []

    # --- Process digitisation (highest weight, address first) ---
    if process_dig < 0.30 and total_steps > 0:
        recs.append(
            f"{manual_steps} of {total_steps} workflow steps are manual. "
            "Prioritise automating high-frequency tasks like order taking, "
            "billing, and daily reporting with a POS or integrated system."
        )
    elif process_dig < 0.60 and total_steps > 0:
        recs.append(
            f"Only {int(process_dig*100)}% of workflow steps are automated. "
            "Bridge the gap by digitising remaining manual handoffs — "
            "e.g. replace paper checklists with digital forms."
        )

    # --- Data coverage ---
    missing_docs = [d for d in ["invoices", "payroll", "inventory"] if d not in documents_provided]
    if missing_docs:
        doc_labels = {"invoices": "supplier invoice records", "payroll": "payroll/staff sheet", "inventory": "inventory log"}
        missing_str = ", ".join(doc_labels[d] for d in missing_docs)
        recs.append(
            f"Missing data sources: {missing_str}. "
            "Uploading these improves accuracy of the AI readiness score and unlocks "
            "richer automation and consolidation recommendations."
        )

    # --- Tool maturity ---
    if tool_mat < 0.40:
        low_tools = [t for t in tools_used if _classify_tool(t) == 1]
        if low_tools:
            recs.append(
                f"Current toolkit relies on low-sophistication tools ({', '.join(low_tools)}). "
                "Adopt at least one integrated platform (POS, ERP, or CRM) to centralise "
                "data and reduce manual transcription errors."
            )
        else:
            recs.append(
                "No digital tools detected. Start with a basic cloud-based "
                "system (Google Sheets, a POS, or inventory app) to create a "
                "machine-readable data trail that AI can work with."
            )
    elif tool_mat < 0.60:
        recs.append(
            "Tools in use are mostly generic productivity software. "
            "Consider upgrading to domain-specific platforms (POS, inventory management, "
            "or accounting software with API access) for better data integration."
        )

    # --- Completeness ---
    if completeness < 0.80:
        missing_cols = (
            df.isna().sum()
            .where(lambda s: s > 0)
            .dropna()
            .sort_values(ascending=False)
        )
        top_cols = ", ".join(f"'{c}'" for c in list(missing_cols.index[:3]))
        pct = round((1 - completeness) * 100, 1)
        recs.append(
            f"{pct}% of cells are missing. Prioritise filling: {top_cols}. "
            "AI models require dense, complete datasets to train reliably."
        )

    # --- Deduplication ---
    if deduplication < 0.95:
        dup_rows = int(df.duplicated().sum())
        recs.append(
            f"Remove {dup_rows} duplicate row(s) before any AI or analytics work — "
            "they skew aggregations and inflate model confidence."
        )

    # --- Inconsistent column names ---
    bad_name_issues = [
        i for i in data_issues
        if i.issue_type == IssueType.INCONSISTENT_COLUMN_NAMES
    ]
    if bad_name_issues:
        affected = bad_name_issues[0].affected_count or 0
        recs.append(
            f"{affected} column(s) use inconsistent naming (spaces, mixed case, "
            "special characters). Standardise to snake_case for pipeline compatibility."
        )

    # --- Unparsed dates ---
    date_issues = [
        i for i in data_issues if i.issue_type == IssueType.UNPARSED_DATES
    ]
    if date_issues:
        cols = ", ".join(f"'{i.column}'" for i in date_issues if i.column)
        recs.append(
            f"Date column(s) {cols} are stored as plain text. "
            "Parse them to proper datetime types to enable time-series and trend analysis."
        )

    return recs[:7]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_quality_report(session_id: str, entry: SessionEntry) -> QualityReport:
    """Compute the full quality and AI readiness report from a session entry.

    Args:
        session_id: The session identifier (echoed in the report).
        entry: The SessionEntry fetched from the session store.

    Returns:
        QualityReport with all dimension scores and recommendations.

    Raises:
        ValueError: if the session has no tabular DataFrame (document-only session).
    """
    df = entry.raw_dataframe
    if df is None or df.empty:
        raise ValueError(
            "This session contains no tabular data. "
            "Quality scoring requires a CSV or Excel upload via /ingest/tabular."
        )

    data_issues = entry.data_issues
    col_count = len(df.columns)
    row_count = len(df)

    # --- Data quality dimensions ---
    completeness_score, missing_cells, total_cells = _completeness(df)
    deduplication_score, duplicate_rows, _fuzzy_dup_rows = _deduplication(df)
    consistency_score = _consistency(data_issues, col_count)
    structural_score = _structural_integrity(data_issues, col_count)

    # --- Operational dimensions ---
    process_dig_score, total_steps, automated_steps, manual_steps = \
        _process_digitisation(entry.workflow_analysis)
    tool_mat_score, tools_detected = \
        _tool_maturity(entry.company_metadata.get("tools_used", []))
    supp_stats = getattr(entry, "supplementary_doc_stats", {}) or {}
    data_cov_score = _data_coverage(
        getattr(entry, "documents_provided", []) or [],
        supp_stats,
    )

    # --- Composite ---
    ai_readiness_score = round(
        completeness_score * _W_COMPLETENESS
        + deduplication_score * _W_DEDUPLICATION
        + consistency_score * _W_CONSISTENCY
        + structural_score * _W_STRUCTURAL
        + process_dig_score * _W_PROCESS_DIGITISATION
        + tool_mat_score * _W_TOOL_MATURITY
        + data_cov_score * _W_DATA_COVERAGE,
        4,
    )

    return QualityReport(
        session_id=session_id,
        row_count=row_count,
        column_count=col_count,
        total_cells=total_cells,
        missing_cells=missing_cells,
        duplicate_rows=duplicate_rows,
        completeness_score=completeness_score,
        deduplication_score=deduplication_score,
        consistency_score=consistency_score,
        structural_integrity_score=structural_score,
        process_digitisation_score=process_dig_score,
        tool_maturity_score=tool_mat_score,
        data_coverage_score=data_cov_score,
        total_workflow_steps=total_steps,
        automated_steps=automated_steps,
        manual_steps=manual_steps,
        tools_detected=tools_detected,
        documents_provided=getattr(entry, "documents_provided", []) or [],
        ai_readiness_score=ai_readiness_score,
        readiness_level=_readiness_level(ai_readiness_score),
        column_quality=_column_quality(df, data_issues),
        top_recommendations=_recommendations(
            completeness_score, deduplication_score,
            consistency_score, structural_score,
            process_dig_score, tool_mat_score, data_cov_score,
            df, data_issues,
            total_steps, manual_steps, tools_detected,
            getattr(entry, "documents_provided", []) or [],
        ),
    )
