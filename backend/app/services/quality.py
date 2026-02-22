"""Module 2 — Data Quality & DPDP Compliance Scanner.

FoundationIQ 3.0 (Startup Edition)

All calculations are deterministic and LLM-free.
Inputs come from what Module 1 stored in the session:
  - org_chart_df / expenses_df / sales_inquiries_df  → quality checks
  - raw_dataframe (legacy fallback)                    → quality checks
  - data_issues                                        → consistency, structural integrity
  - workflow_analysis                                  → process digitisation ratio
  - company_metadata / startup_profile                 → tool maturity

Metrics calculated:
  Metric 2 — Data Quality Score (weighted composite, >85% = pass)
  Metric 6 — DPDP Risk Level (PII column scan)

PII Detection (regex-based):
  - Email addresses
  - Phone numbers (Indian + international)
  - Credit card numbers (Visa, MC, Amex, Discover)
  - Aadhaar numbers (Indian 12-digit with spaces)
  - PAN numbers (Indian ABCDE1234F format)
  - IP addresses (IPv4)

Scoring dimensions and weights (updated for 3.0)
-------------------------------------------------
  Completeness              25 %   1 − (null_cells / total_cells) + volume
  Deduplication             20 %   1 − (dup_rows / total_rows)
  Consistency               15 %   penalty for bad names, whitespace, mixed types
  Structural Integrity      10 %   penalty for unparsed dates and mixed dtypes
  Process Digitisation      15 %   automated_steps / total_steps from workflow
  Tool Maturity              5 %   weighted score by tool sophistication tier
  Data Coverage             10 %   which CSV files were uploaded

Quality levels: High ≥0.80 · Moderate ≥0.60 · Low ≥0.40 · Critical <0.40
Quality Pass threshold: ≥ 0.85
"""

from __future__ import annotations

import re

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.ingestion import IssueType
from app.schemas.quality import (
    ColumnQuality,
    DPDPComplianceReport,
    DPDPRiskLevel,
    PIIFinding,
    QualityReport,
    ReadinessLevel,
)


# ---------------------------------------------------------------------------
# Weights (must sum to 1.0)
# ---------------------------------------------------------------------------

_W_COMPLETENESS = 0.25
_W_DEDUPLICATION = 0.20
_W_CONSISTENCY = 0.15
_W_STRUCTURAL = 0.10
_W_PROCESS_DIGITISATION = 0.15
_W_TOOL_MATURITY = 0.05
_W_DATA_COVERAGE = 0.10


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
# PII regex patterns for DPDP compliance scanning
# ---------------------------------------------------------------------------

_PII_PATTERNS: dict[str, re.Pattern[str]] = {
    "email": re.compile(
        r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b"
    ),
    "phone": re.compile(
        # Matches: +91 9876543210 | 919876543210 | 919876543210.0 (pandas float)
        # | bare 10-digit starting with 6-9
        r"\b(?:\+91[\s\-]?|91)[6-9]\d{9}(?:\.0)?\b|\b[6-9]\d{9}\b"
    ),
    "credit_card": re.compile(
        r"\b(?:4\d{3}|5[1-5]\d{2}|3[47]\d{2}|6(?:011|5\d{2}))[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{0,4}\b"
    ),
    "aadhaar": re.compile(
        # 12-digit Indian UID — exclude numbers starting with 91[6-9] (phone + country code)
        r"\b(?!91[6-9]\d{9})[2-9]\d{3}[\s\-]?\d{4}[\s\-]?\d{4}\b"
    ),
    "pan": re.compile(
        r"\b[A-Z]{5}\d{4}[A-Z]\b"
    ),
    "ip_address": re.compile(
        r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"
    ),
}

_PII_RECOMMENDATIONS: dict[str, str] = {
    "email": "Hash or mask email addresses before sharing with LLM APIs or analytics pipelines.",
    "phone": "Mask phone numbers (show last 4 digits only) or remove entirely before processing.",
    "credit_card": "CRITICAL: Remove or tokenise credit card numbers immediately — PCI-DSS violation risk.",
    "aadhaar": "CRITICAL: Aadhaar numbers are sensitive under DPDP Act — must be masked or removed before any processing.",
    "pan": "PAN numbers are financial identifiers — mask or remove before sharing externally.",
    "ip_address": "IP addresses can be PII under DPDP — anonymise or aggregate before analysis.",
}


def _scan_pii(df: pd.DataFrame) -> list[PIIFinding]:
    """Scan all columns in a DataFrame for PII patterns.

    All columns are cast to string before scanning so that phone numbers
    stored as float64 by pandas (e.g. +919876543210 → 919876543210.0)
    are still matched by the phone regex.

    Returns a list of PIIFinding for every (column, pii_type) pair found.
    """
    findings: list[PIIFinding] = []
    for col in df.columns:
        series = df[col].dropna().astype(str)
        total_values = len(series)
        if total_values == 0:
            continue

        for pii_type, pattern in _PII_PATTERNS.items():
            matches = series.str.contains(pattern, na=False)
            match_count = int(matches.sum())
            if match_count == 0:
                continue

            exposure = round((match_count / total_values) * 100, 2)
            if exposure > 50:
                risk = "High"
            elif exposure >= 10:
                risk = "Medium"
            else:
                risk = "Low"

            findings.append(PIIFinding(
                column=str(col),
                pii_type=pii_type,
                sample_count=match_count,
                total_values=total_values,
                exposure_pct=exposure,
                risk_level=risk,
                recommendation=_PII_RECOMMENDATIONS[pii_type],
            ))
    return findings


def _dpdp_risk_level(pii_findings: list[PIIFinding]) -> DPDPRiskLevel:
    """Determine overall DPDP risk based on how many columns contain PII."""
    pii_columns = {f.column for f in pii_findings}
    n = len(pii_columns)
    if n > 3:
        return "Critical"
    if n >= 2:
        return "High"
    if n == 1:
        return "Medium"
    return "Low"


def _dpdp_compliance(df: pd.DataFrame) -> DPDPComplianceReport:
    """Run full DPDP compliance scan on a DataFrame."""
    findings = _scan_pii(df)
    risk = _dpdp_risk_level(findings)
    pii_columns = {f.column for f in findings}
    total_pii_values = sum(f.sample_count for f in findings)

    has_high_risk = any(f.risk_level == "High" for f in findings)
    has_critical_types = any(
        f.pii_type in ("credit_card", "aadhaar") for f in findings
    )

    warnings: list[str] = []
    if has_critical_types:
        warnings.append(
            "CRITICAL: Credit card or Aadhaar data detected — "
            "mandatory anonymisation required under DPDP Act before any data sharing."
        )
    if has_high_risk:
        warnings.append(
            "High PII exposure detected in one or more columns — "
            "do NOT send this data to external LLM APIs without masking."
        )
    if findings:
        warnings.append(
            f"{len(pii_columns)} column(s) contain personally identifiable information. "
            "Apply column-level masking, hashing, or removal before analytics."
        )
    if not findings:
        warnings.append("No PII detected — data appears safe for LLM API processing.")

    return DPDPComplianceReport(
        risk_level=risk,
        total_pii_columns=len(pii_columns),
        total_pii_values=total_pii_values,
        pii_findings=findings,
        compliance_warnings=warnings,
        llm_api_safe=not has_high_risk and not has_critical_types,
    )


def _merge_dpdp_reports(reports: list[DPDPComplianceReport]) -> DPDPComplianceReport:
    """Merge multiple DPDP reports (one per DataFrame) into a single report."""
    if not reports:
        return DPDPComplianceReport(
            risk_level="Low",
            total_pii_columns=0,
            total_pii_values=0,
            pii_findings=[],
            compliance_warnings=["No data files to scan."],
            llm_api_safe=True,
        )
    if len(reports) == 1:
        return reports[0]

    all_findings: list[PIIFinding] = []
    all_warnings: set[str] = set()
    for r in reports:
        all_findings.extend(r.pii_findings)
        all_warnings.update(r.compliance_warnings)

    risk = _dpdp_risk_level(all_findings)
    pii_columns = {f.column for f in all_findings}
    total_pii_values = sum(f.sample_count for f in all_findings)
    llm_safe = all(r.llm_api_safe for r in reports)

    # Remove the "No PII detected" message from clean-file reports when other
    # files in the same session do contain PII — it's contradictory.
    if all_findings:
        all_warnings.discard("No PII detected \u2014 data appears safe for LLM API processing.")

    return DPDPComplianceReport(
        risk_level=risk,
        total_pii_columns=len(pii_columns),
        total_pii_values=total_pii_values,
        pii_findings=all_findings,
        compliance_warnings=sorted(all_warnings),
        llm_api_safe=llm_safe,
    )


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
# Data coverage score — Startup Edition
# ---------------------------------------------------------------------------

# Points for each CSV file  (must sum to 1.0)
_COVERAGE_POINTS: dict[str, float] = {
    "sales_inquiries": 0.40,   # Primary revenue pipeline data
    "expenses":        0.35,   # Financial health
    "org_chart":       0.25,   # Team structure
}


def _data_coverage(documents_provided: list[str], supplementary_doc_stats: dict) -> float:
    """Score based on which CSV files the startup uploaded AND their content quality.

    Base points per file: sales_inquiries=0.40, expenses=0.35, org_chart=0.25.

    For each file, content quality is validated:
      No stats provided       → full base points  (backward-compatible / test fallback)
      CSV/Excel ≥ 50 rows     → 100 % of base points
      CSV/Excel 10–49 rows    → 60 % of base points
      CSV/Excel < 10 rows     → 20 % of base points  (nearly empty)
      Unreadable / corrupt    → 30 % of base points
    """
    if not documents_provided:
        return 0.0

    score = 0.0
    for doc in documents_provided:
        doc_key = doc.lower().replace(" ", "_")
        base = _COVERAGE_POINTS.get(doc_key, 0.0)
        if base == 0.0:
            # Legacy document types — give small credit
            base = 0.10

        stats = supplementary_doc_stats.get(doc_key)
        if stats is None:
            score += base          # no stats → full credit (backward-compat / tests)
        elif not stats.get("readable", True):
            score += base * 0.30   # unreadable / corrupt file
        else:
            rows = stats.get("row_count", 0)
            if rows >= 50:
                score += base
            elif rows >= 10:
                score += base * 0.60
            else:
                score += base * 0.20

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

def _column_quality(
    named_dfs: list[tuple[str, pd.DataFrame]],
    data_issues: list,
    pii_findings: list[PIIFinding] | None = None,
) -> list[ColumnQuality]:
    """Per-column quality breakdown including PII types.

    Operates per-file so that columns from org_chart don't show 33%
    completeness just because sales_inquiries rows have no value for them.
    """
    col_issue_map: dict[str, set[str]] = {}
    for issue in data_issues:
        if issue.column:
            col_issue_map.setdefault(issue.column, set()).add(issue.issue_type.value)

    col_pii_map: dict[str, set[str]] = {}
    for finding in (pii_findings or []):
        col_pii_map.setdefault(finding.column, set()).add(finding.pii_type)

    results = []
    for _, df in named_dfs:
        total_rows = len(df)
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            completeness = round(1.0 - (null_count / total_rows), 4) if total_rows else 1.0
            results.append(ColumnQuality(
                name=str(col),
                dtype=str(df[col].dtype),
                completeness=completeness,
                null_count=null_count,
                issue_types=sorted(col_issue_map.get(str(col), set())),
                pii_types=sorted(col_pii_map.get(str(col), set())),
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
    dpdp_report: DPDPComplianceReport | None = None,
) -> list[str]:
    """Generate up to 10 prioritised, actionable recommendations."""
    recs: list[str] = []

    # --- DPDP compliance (critical first) ---
    if dpdp_report and dpdp_report.risk_level in ("Critical", "High"):
        pii_cols = {f.column for f in dpdp_report.pii_findings}
        recs.append(
            f"DPDP COMPLIANCE: {len(pii_cols)} column(s) contain PII. "
            "Anonymise or mask personal data before any external API calls or sharing. "
            "Non-compliance with India's DPDP Act 2023 carries significant penalties."
        )
    if dpdp_report and not dpdp_report.llm_api_safe:
        detected_types = sorted({f.pii_type for f in dpdp_report.pii_findings})
        type_str = ", ".join(detected_types) if detected_types else "PII"
        recs.append(
            f"DO NOT send this dataset to LLM APIs (Gemini, GPT, etc.) in its current form — "
            f"raw {type_str} data was detected. Apply column-level masking first."
        )

    # --- Process digitisation ---
    if process_dig < 0.30 and total_steps > 0:
        recs.append(
            f"{manual_steps} of {total_steps} workflow steps are manual. "
            "Prioritise automating high-frequency tasks like lead tracking, "
            "invoicing, and daily reporting with CRM or project management tools."
        )
    elif process_dig < 0.60 and total_steps > 0:
        recs.append(
            f"Only {int(process_dig*100)}% of workflow steps are automated. "
            "Bridge the gap by digitising remaining manual handoffs — "
            "e.g. replace spreadsheet-based tracking with integrated tools."
        )

    # --- Data coverage ---
    missing_csvs = [
        d for d in ["sales_inquiries", "expenses", "org_chart"]
        if d not in [x.lower().replace(" ", "_") for x in documents_provided]
    ]
    if missing_csvs:
        csv_labels = {
            "sales_inquiries": "sales inquiries CSV",
            "expenses": "expenses CSV",
            "org_chart": "org chart CSV",
        }
        missing_str = ", ".join(csv_labels.get(d, d) for d in missing_csvs)
        recs.append(
            f"Missing data sources: {missing_str}. "
            "Uploading all 3 CSVs improves diagnostic accuracy and unlocks "
            "richer benchmarking and automation recommendations."
        )

    # --- Tool maturity ---
    if tool_mat < 0.40:
        low_tools = [t for t in tools_used if _classify_tool(t) == 1]
        if low_tools:
            recs.append(
                f"Current toolkit relies on low-sophistication tools ({', '.join(low_tools)}). "
                "Adopt at least one integrated platform (CRM, ERP, or project management) to centralise "
                "data and reduce manual transcription errors."
            )
        else:
            recs.append(
                "No digital tools detected. Start with a basic cloud-based "
                "system (Google Sheets, a CRM, or accounting app) to create a "
                "machine-readable data trail that AI can work with."
            )
    elif tool_mat < 0.60:
        recs.append(
            "Tools in use are mostly generic productivity software. "
            "Consider upgrading to domain-specific platforms (CRM, project management, "
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

    return recs[:10]


# ---------------------------------------------------------------------------
# Helpers — gather DataFrames from a session
# ---------------------------------------------------------------------------

def _gather_dataframes(entry: SessionEntry) -> list[tuple[str, pd.DataFrame]]:
    """Collect all available DataFrames from a session with their names.

    Returns:
        List of (name, df) tuples. Empty DataFrames are excluded.
    """
    candidates: list[tuple[str, pd.DataFrame | None]] = [
        ("org_chart", entry.org_chart_df),
        ("expenses", entry.expenses_df),
        ("sales_inquiries", entry.sales_inquiries_df),
        ("raw_dataframe", entry.raw_dataframe),
    ]
    return [(name, df) for name, df in candidates if df is not None and not df.empty]


def _merge_dataframes(dfs: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """Concatenate multiple DataFrames for aggregate quality scoring.

    Uses ``pd.concat`` with outer join. If only one DF, returns it as-is.
    """
    if len(dfs) == 1:
        return dfs[0][1]
    return pd.concat([df for _, df in dfs], ignore_index=True, sort=False)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_quality_report(session_id: str, entry: SessionEntry) -> QualityReport:
    """Compute the full Data Quality & DPDP Compliance report.

    FoundationIQ 3.0 (Startup Edition)

    Scans all available DataFrames in the session (org_chart, expenses,
    sales_inquiries, and legacy raw_dataframe).  Produces:
      Metric 2 — Data Quality Score (weighted composite)
      Metric 6 — DPDP Risk Level (PII scan)

    Args:
        session_id: The session identifier (echoed in the report).
        entry: The SessionEntry fetched from the session store.

    Returns:
        QualityReport with all dimension scores, DPDP compliance, and recommendations.

    Raises:
        ValueError: if the session has no tabular DataFrame at all.
    """
    named_dfs = _gather_dataframes(entry)
    if not named_dfs:
        raise ValueError(
            "This session contains no tabular data. "
            "Quality scoring requires CSV uploads via /ingest/startup."
        )

    # Primary DF for quality scoring = merged view of all CSVs
    df = _merge_dataframes(named_dfs)

    data_issues = entry.data_issues
    col_count = len(df.columns)
    row_count = len(df)

    # --- Completeness: computed per-file then weighted by cell count -------
    # Using the merged DF would falsely mark every column as ~33% complete
    # because org_chart columns are NaN for expenses/sales_inquiries rows.
    total_cells = 0
    missing_cells = 0
    _weighted_comp = 0.0
    for _, _df_item in named_dfs:
        _c_score, _c_miss, _c_total = _completeness(_df_item)
        total_cells += _c_total
        missing_cells += _c_miss
        _weighted_comp += _c_score * _c_total
    completeness_score = round(_weighted_comp / total_cells, 4) if total_cells else 1.0
    deduplication_score, duplicate_rows, _fuzzy_dup_rows = _deduplication(df)
    consistency_score = _consistency(data_issues, col_count)
    structural_score = _structural_integrity(data_issues, col_count)

    # --- Operational dimensions ---
    process_dig_score, total_steps, automated_steps, manual_steps = \
        _process_digitisation(entry.workflow_analysis)
    # current_tech_stack (StartupProfile field) takes priority over legacy tools_used
    tools_used = (
        entry.company_metadata.get("current_tech_stack")
        or entry.company_metadata.get("tools_used")
        or []
    )
    tool_mat_score, tools_detected = _tool_maturity(tools_used)
    supp_stats = getattr(entry, "supplementary_doc_stats", {}) or {}

    # Build documents_provided from which DFs exist
    docs_provided = getattr(entry, "documents_provided", []) or []
    if not docs_provided:
        docs_provided = [name for name, _ in named_dfs if name != "raw_dataframe"]

    data_cov_score = _data_coverage(docs_provided, supp_stats)

    # --- Composite (Metric 2) ---
    data_quality_score = round(
        completeness_score * _W_COMPLETENESS
        + deduplication_score * _W_DEDUPLICATION
        + consistency_score * _W_CONSISTENCY
        + structural_score * _W_STRUCTURAL
        + process_dig_score * _W_PROCESS_DIGITISATION
        + tool_mat_score * _W_TOOL_MATURITY
        + data_cov_score * _W_DATA_COVERAGE,
        4,
    )
    quality_pass = data_quality_score >= 0.85

    # --- DPDP Compliance (Metric 6) ---
    dpdp_reports = [_dpdp_compliance(df_item) for _, df_item in named_dfs]
    dpdp_report = _merge_dpdp_reports(dpdp_reports)

    # Collect PII findings for column quality annotation
    all_pii_findings = dpdp_report.pii_findings

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
        documents_provided=docs_provided,
        data_quality_score=data_quality_score,
        ai_readiness_score=data_quality_score,  # legacy alias
        quality_pass=quality_pass,
        readiness_level=_readiness_level(data_quality_score),
        dpdp_compliance=dpdp_report,
        column_quality=_column_quality(named_dfs, data_issues, all_pii_findings),
        top_recommendations=_recommendations(
            completeness_score, deduplication_score,
            consistency_score, structural_score,
            process_dig_score, tool_mat_score, data_cov_score,
            df, data_issues,
            total_steps, manual_steps, tools_detected,
            docs_provided,
            dpdp_report,
        ),
    )
