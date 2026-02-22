"""Module 3 — Workflow Bottleneck & Speed Analyzer.

FoundationIQ 3.0 (Startup Edition)

Pipeline (pure Pandas, no LLM)
-------------------------------
1. Load sales_inquiries_df from the session store.
2. Auto-detect Inquiry_Date and Payment_Date columns (case-insensitive).
3. Parse both columns as datetime (coerce errors → NaT).
4. For each row where both dates are present, compute TAT in hours.
5. Flag bottlenecks: TAT > 48 hours.
6. Compute Metric 11 (avg TAT improvement %) and Metric 4 (total hours saved).
7. Generate a Mermaid TD flowchart illustrating the bottleneck distribution.
8. Return BottleneckReport.
"""

from __future__ import annotations

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.benchmark import BottleneckReport, InquiryTAT

_BOTTLENECK_THRESHOLD_HOURS: float = 48.0
_AUTOMATION_TARGET_HOURS: float = 2.0

# Accepted column name variants (normalised to lowercase for matching)
_INQUIRY_DATE_VARIANTS: set[str] = {
    "inquiry_date", "inquiry date", "date_of_inquiry",
    "created_at", "date_received",
}
_PAYMENT_DATE_VARIANTS: set[str] = {
    "payment_date", "payment date", "fulfillment_date",
    "closed_at", "date_closed", "paid_at",
}
_INQUIRY_ID_VARIANTS: set[str] = {
    "inquiry_id", "id", "inquiry_number", "ticket_id", "lead_id",
}
_STATUS_VARIANTS: set[str] = {"status", "inquiry_status", "state"}


# ---------------------------------------------------------------------------
# Column detection helper
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, candidates: set[str]) -> str | None:
    """Return the first DataFrame column that matches any candidate name.

    Matching is case-insensitive and whitespace-stripped.
    """
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


# ---------------------------------------------------------------------------
# Mermaid flowchart
# ---------------------------------------------------------------------------

def _mermaid_flowchart(
    avg_tat: float,
    bottleneck_count: int,
    fast_count: int,
    automation_target: float,
    total_hours_saved: float,
    threshold: float,
    avg_bottleneck_excess: float = 0.0,
) -> str:
    improvement_pct = round(((avg_tat - automation_target) / avg_tat) * 100) if avg_tat > 0 else 0
    return (
        "flowchart TD\n"
        f'    A["📩 Inquiry Received"] --> B["👤 Manual Review and Follow-up\n'
        f'Avg TAT: {avg_tat:.1f}h"]\n'
        f"    B --> C{{TAT Threshold {threshold:.0f}h}}\n"
        f'    C -->|"✅ On Track — {fast_count} inquiries"| D["💳 Payment Received\nWithin SLA"]\n'
        f'    C -->|"⚠️ Bottleneck — {bottleneck_count} inquiries"| E["⏳ Delayed Conversion\n'
        f'Avg {avg_bottleneck_excess:.1f}h over SLA"]\n'
        f'    E --> F["🤖 Proposed: API Automation\nTarget TAT: {automation_target:.0f}h"]\n'
        f"    F --> D\n"
        f'    D --> G["💡 Saving Potential\n'
        f"{total_hours_saved:.0f}h total · {improvement_pct}% TAT reduction\"]"
    )


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _recommendations(
    avg_tat: float,
    bottleneck_count: int,
    closed_count: int,
    total_hours_saved: float,
    bottleneck_pct: float,
) -> list[str]:
    recs: list[str] = []

    if bottleneck_count > 0:
        recs.append(
            f"{bottleneck_count} of {closed_count} closed inquiries exceeded the 48-hour SLA. "
            "Implement an automated API webhook to assign new inquiries to an SDR within minutes "
            "of receipt — eliminating manual triage delay."
        )

    if avg_tat > 24:
        recs.append(
            f"Average inquiry-to-payment TAT is {avg_tat:.1f}h. "
            "Set up a Zoho CRM automation or Zapier workflow to send follow-up emails "
            "automatically at 4h, 12h, and 24h intervals after inquiry receipt."
        )

    if total_hours_saved > 100:
        recs.append(
            f"API automation could recover {total_hours_saved:.0f} hours of lost pipeline time. "
            "Prioritise building a Razorpay payment-link trigger that fires immediately "
            "after a demo is completed."
        )

    if bottleneck_pct > 50:
        recs.append(
            f"{bottleneck_pct:.0f}% of conversions are slow. "
            "Investigate the SDR capacity: if 2 SDRs are handling all follow-ups manually, "
            "AI-assisted email sequencing (via Mailchimp automation) can scale this to 10x."
        )

    if avg_tat <= 24 and bottleneck_count == 0:
        recs.append(
            "Excellent pipeline velocity — all inquiries converted within 24h. "
            "Focus on increasing inbound inquiry volume rather than speed."
        )

    return recs[:5]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_bottleneck_report(session_id: str, entry: SessionEntry) -> BottleneckReport:
    """Compute the full Bottleneck & Speed report for a session.

    Reads sales_inquiries_df from the session.  Calculates TAT per inquiry,
    flags bottlenecks, and generates Metric 11 + Metric 4.

    Raises:
        ValueError: if no sales_inquiries data is present, or if required
                    date columns cannot be found.
    """
    df = entry.sales_inquiries_df
    if df is None or df.empty:
        raise ValueError(
            "No sales_inquiries data in this session. "
            "Upload sales_inquiries.csv via Module 1 (/api/v1/ingest/startup)."
        )

    # ── Auto-detect columns ──────────────────────────────────────────────
    inquiry_id_col = _find_col(df, _INQUIRY_ID_VARIANTS)
    inquiry_date_col = _find_col(df, _INQUIRY_DATE_VARIANTS)
    payment_date_col = _find_col(df, _PAYMENT_DATE_VARIANTS)
    status_col = _find_col(df, _STATUS_VARIANTS)

    warnings: list[str] = []

    if inquiry_date_col is None:
        raise ValueError(
            "Cannot find an Inquiry_Date column. "
            "Expected one of: 'Inquiry_Date', 'inquiry_date', 'created_at'."
        )
    if payment_date_col is None:
        raise ValueError(
            "Cannot find a Payment_Date column. "
            "Expected one of: 'Payment_Date', 'payment_date', 'closed_at'."
        )

    # ── Parse datetimes ──────────────────────────────────────────────────
    df = df.copy()
    df["_inq_dt"] = pd.to_datetime(df[inquiry_date_col], errors="coerce")
    df["_pay_dt"] = pd.to_datetime(df[payment_date_col], errors="coerce")

    if df["_inq_dt"].isna().all():
        raise ValueError(
            f"Column '{inquiry_date_col}' contains no parseable datetime values."
        )

    # ── Closed vs total ──────────────────────────────────────────────────
    total_inquiries = len(df)
    closed_mask = df["_pay_dt"].notna() & df["_inq_dt"].notna()
    closed_df = df[closed_mask].copy()
    closed_inquiries = len(closed_df)

    if closed_inquiries == 0:
        warnings.append(
            "No closed inquiries found — all rows have a missing Payment_Date. "
            "TAT analysis requires at least one completed transaction."
        )
        return BottleneckReport(
            session_id=session_id,
            total_inquiries=total_inquiries,
            closed_inquiries=0,
            avg_tat_hours=0.0,
            median_tat_hours=0.0,
            max_tat_hours=0.0,
            min_tat_hours=0.0,
            bottleneck_count=0,
            bottleneck_pct=0.0,
            avg_tat_improvement_pct=0.0,
            total_hours_saved=0.0,
            avg_hours_saved_per_inquiry=0.0,
            inquiry_tat_list=_build_tat_list(df, inquiry_id_col, inquiry_date_col,
                                              payment_date_col, status_col),
            warnings=warnings,
        )

    # ── Calculate TAT in hours ───────────────────────────────────────────
    closed_df["_tat_h"] = (
        (closed_df["_pay_dt"] - closed_df["_inq_dt"]).dt.total_seconds() / 3600.0
    )

    # Drop rows where payment happened before inquiry (data error)
    neg_mask = closed_df["_tat_h"] < 0
    if neg_mask.any():
        warnings.append(
            f"{int(neg_mask.sum())} row(s) had Payment_Date before Inquiry_Date "
            "and were excluded from TAT statistics."
        )
        closed_df = closed_df[~neg_mask].copy()
        closed_inquiries = len(closed_df)

    if closed_inquiries == 0:
        return BottleneckReport(
            session_id=session_id,
            total_inquiries=total_inquiries,
            closed_inquiries=0,
            avg_tat_hours=0.0,
            median_tat_hours=0.0,
            max_tat_hours=0.0,
            min_tat_hours=0.0,
            bottleneck_count=0,
            bottleneck_pct=0.0,
            avg_tat_improvement_pct=0.0,
            total_hours_saved=0.0,
            avg_hours_saved_per_inquiry=0.0,
            inquiry_tat_list=_build_tat_list(df, inquiry_id_col, inquiry_date_col,
                                              payment_date_col, status_col),
            warnings=warnings,
        )

    # ── TAT statistics ───────────────────────────────────────────────────
    avg_tat = float(closed_df["_tat_h"].mean())
    median_tat = float(closed_df["_tat_h"].median())
    max_tat = float(closed_df["_tat_h"].max())
    min_tat = float(closed_df["_tat_h"].min())

    # ── Bottlenecks ──────────────────────────────────────────────────────
    bottleneck_mask = closed_df["_tat_h"] > _BOTTLENECK_THRESHOLD_HOURS
    bottleneck_count = int(bottleneck_mask.sum())
    bottleneck_pct = round((bottleneck_count / closed_inquiries) * 100, 2)
    fast_count = closed_inquiries - bottleneck_count
    # Average extra hours beyond threshold, among bottleneck inquiries only
    avg_bottleneck_excess = (
        float((closed_df.loc[bottleneck_mask, "_tat_h"] - _BOTTLENECK_THRESHOLD_HOURS).mean())
        if bottleneck_count > 0 else 0.0
    )

    # ── Automation metrics ───────────────────────────────────────────────
    avg_tat_improvement_pct = round(
        ((avg_tat - _AUTOMATION_TARGET_HOURS) / avg_tat) * 100, 2
    ) if avg_tat > _AUTOMATION_TARGET_HOURS else 0.0

    savings_per_row = (closed_df["_tat_h"] - _AUTOMATION_TARGET_HOURS).clip(lower=0)
    total_hours_saved = round(float(savings_per_row.sum()), 2)
    avg_hours_saved = round(float(savings_per_row.mean()), 2)

    # ── Per-inquiry list (all rows, not just closed) ─────────────────────
    tat_list = _build_tat_list(df, inquiry_id_col, inquiry_date_col,
                                payment_date_col, status_col)

    # ── Mermaid flowchart ─────────────────────────────────────────────────
    mermaid = _mermaid_flowchart(
        avg_tat, bottleneck_count, fast_count,
        _AUTOMATION_TARGET_HOURS, total_hours_saved, _BOTTLENECK_THRESHOLD_HOURS,
        avg_bottleneck_excess,
    )

    # ── Recommendations ───────────────────────────────────────────────────
    recs = _recommendations(avg_tat, bottleneck_count, closed_inquiries,
                             total_hours_saved, bottleneck_pct)

    return BottleneckReport(
        session_id=session_id,
        total_inquiries=total_inquiries,
        closed_inquiries=closed_inquiries,
        avg_tat_hours=round(avg_tat, 2),
        median_tat_hours=round(median_tat, 2),
        max_tat_hours=round(max_tat, 2),
        min_tat_hours=round(min_tat, 2),
        bottleneck_count=bottleneck_count,
        bottleneck_pct=bottleneck_pct,
        avg_tat_improvement_pct=avg_tat_improvement_pct,
        total_hours_saved=total_hours_saved,
        avg_hours_saved_per_inquiry=avg_hours_saved,
        inquiry_tat_list=tat_list,
        mermaid_flowchart=mermaid,
        recommendations=recs,
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Private helper — build per-inquiry TAT list over ALL rows (not just closed)
# ---------------------------------------------------------------------------

def _build_tat_list(
    df: pd.DataFrame,
    inquiry_id_col: str | None,
    inquiry_date_col: str,
    payment_date_col: str,
    status_col: str | None,
) -> list[InquiryTAT]:
    records: list[InquiryTAT] = []
    for _, row in df.iterrows():
        iid = str(row[inquiry_id_col]) if inquiry_id_col else "—"
        inq_str = str(row[inquiry_date_col]) if pd.notna(row.get("_inq_dt")) else str(row.get(inquiry_date_col, ""))
        pay_str = str(row[payment_date_col]) if pd.notna(row.get("_pay_dt")) else None

        tat_h: float | None = None
        if pd.notna(row.get("_inq_dt")) and pd.notna(row.get("_pay_dt")):
            delta = (row["_pay_dt"] - row["_inq_dt"]).total_seconds() / 3600.0
            tat_h = round(delta, 2) if delta >= 0 else None

        is_bottleneck = tat_h is not None and tat_h > _BOTTLENECK_THRESHOLD_HOURS
        status = str(row[status_col]) if status_col and pd.notna(row.get(status_col)) else ""

        records.append(InquiryTAT(
            inquiry_id=iid,
            inquiry_date=inq_str,
            payment_date=pay_str,
            tat_hours=tat_h,
            is_bottleneck=is_bottleneck,
            status=status,
        ))
    return records
