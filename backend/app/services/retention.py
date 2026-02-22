"""Module 6 — Growth & Retention Benchmarking.

Pipeline
--------
1. Parse sales_inquiries.csv from session (Repeat_Customer_Flag column).
2. Compute current churn proxy and NRR estimate from inquiry data.
3. Call LLM (Gemini) to benchmark against 2026 industry standards by sub-type.
4. Apply AI-personalisation & automated-follow-up simulation to project:
     Metric 9  — Churn Reduction Potential   (current → projected churn %)
     Metric 10 — NRR Projection              (targeting 106%+)
5. Build 5-axis growth radar data for chart display.
6. Return a fully populated RetentionReport.

Requires: Module 1 session with sales_inquiries_df loaded.
Optional: startup_profile (for sub_type / MRR — enriches LLM prompt).
"""

from __future__ import annotations

from app.core.session_store import SessionEntry
from app.schemas.retention import (
    CompetitorChurnBenchmark,
    RadarDataPoint,
    RetentionReport,
)
from app.services.llm import analyse_retention_benchmarks


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

# Fallback benchmarks when LLM is unavailable (key = lower-cased sub_type)
_FALLBACK_BENCHMARKS: dict[str, dict] = {
    "saas": {
        "industry_avg_churn_pct": 3.5,
        "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0,
    },
    "edtech": {
        "industry_avg_churn_pct": 6.5,
        "top_tier_churn_pct": 3.0,
        "nrr_benchmark_pct": 98.0,
    },
    "fintech": {
        "industry_avg_churn_pct": 4.0,
        "top_tier_churn_pct": 1.8,
        "nrr_benchmark_pct": 105.0,
    },
    "e-commerce": {
        "industry_avg_churn_pct": 8.0,
        "top_tier_churn_pct": 4.0,
        "nrr_benchmark_pct": 95.0,
    },
}

# Radar axis definitions: (axis_label, how_to_compute)
_RADAR_AXES = [
    "Win Rate",
    "Repeat Customer Rate",
    "Churn vs Industry",
    "NRR vs Benchmark",
    "Pipeline Health",
]


def _normalise_column(df, candidates: list[str]) -> str | None:
    """Return first matching column name (case-insensitive) from candidates."""
    lower_cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_cols:
            return lower_cols[c.lower()]
    return None


def _parse_sales_metrics(entry: SessionEntry) -> dict:
    """Extract churn-proxy metrics from sales_inquiries_df.

    Returns a dict with:
        total_inquiries, closed_won_count, repeat_customer_count,
        new_customer_count, lost_count, pending_count,
        win_rate_pct, repeat_rate_pct
    Raises ValueError if the DataFrame is missing or has no usable data.
    """
    df = entry.sales_inquiries_df
    if df is None or df.empty:
        raise ValueError(
            "sales_inquiries.csv not found in session. "
            "Re-run Module 1 with sales_inquiries.csv uploaded."
        )

    # Normalise column names
    status_col  = _normalise_column(df, ["Status", "status"])
    repeat_col  = _normalise_column(df, ["Repeat_Customer_Flag", "repeat_customer_flag", "Repeat_Customer"])

    if status_col is None:
        raise ValueError("sales_inquiries.csv must contain a 'Status' column.")
    if repeat_col is None:
        raise ValueError("sales_inquiries.csv must contain a 'Repeat_Customer_Flag' column.")

    total = len(df)
    status_lower = df[status_col].str.strip().str.lower()

    closed_won = df[status_lower == "closed won"]
    lost       = df[status_lower == "lost"]
    pending    = df[~status_lower.isin(["closed won", "lost"])]

    closed_won_count = len(closed_won)
    lost_count       = len(lost)
    pending_count    = len(pending)

    # Repeat customer analysis only on Closed Won rows
    repeat_flag_lower = closed_won[repeat_col].str.strip().str.lower()
    repeat_count = int((repeat_flag_lower == "yes").sum())
    new_count    = int((repeat_flag_lower == "no").sum())

    win_rate_pct    = (closed_won_count / total * 100) if total > 0 else 0.0
    repeat_rate_pct = (repeat_count / closed_won_count * 100) if closed_won_count > 0 else 0.0

    return {
        "total_inquiries":      total,
        "closed_won_count":     closed_won_count,
        "repeat_customer_count": repeat_count,
        "new_customer_count":   new_count,
        "lost_count":           lost_count,
        "pending_count":        pending_count,
        "win_rate_pct":         round(win_rate_pct, 1),
        "repeat_rate_pct":      round(repeat_rate_pct, 1),
    }


def _estimate_current_churn(repeat_rate_pct: float) -> float:
    """Estimate monthly churn from repeat customer rate.

    Logic: churn ≈ (100 - repeat_rate) / 100 × 12 / 12 normalised.
    Repeat rate = % of won customers who came back → inverse is churn proxy.
    Capped between 1% and 25% for realism.
    """
    raw = max(0.0, 100.0 - repeat_rate_pct)
    # Scale: 0% repeat → ~12% monthly churn; 100% repeat → ~0% churn
    churn = raw * 0.12
    return round(min(25.0, max(1.0, churn)), 1)


def _estimate_current_nrr(
    repeat_rate_pct: float,
    win_rate_pct: float,
    churn_pct: float,
) -> float:
    """Estimate current NRR from available proxy signals.

    NRR ≈ (1 − churn) × (1 + expansion) × 100
    Expansion proxy: win_rate / 100 × repeat_rate / 100 × 0.15.
    Result capped to realistic range 70–130%.
    """
    monthly_retention = 1.0 - (churn_pct / 100.0)
    annual_retention  = monthly_retention ** 12
    expansion_proxy   = (win_rate_pct / 100.0) * (repeat_rate_pct / 100.0) * 0.15
    nrr = (annual_retention + expansion_proxy) * 100.0
    return round(min(130.0, max(70.0, nrr)), 1)


def _build_radar(
    win_rate_pct: float,
    repeat_rate_pct: float,
    current_churn_pct: float,
    industry_avg_churn_pct: float,
    projected_nrr_pct: float,
    nrr_benchmark_pct: float,
    closed_won_count: int,
    total_inquiries: int,
) -> list[RadarDataPoint]:
    """Build 5-axis growth radar chart data points (0–100 scale)."""

    # Axis 1: Win Rate — 50% win rate = 100/100 (ambitious target)
    win_score = min(100.0, win_rate_pct * 2.0)

    # Axis 2: Repeat Customer Rate — 80% repeat = 100/100
    repeat_score = min(100.0, repeat_rate_pct * 1.25)

    # Axis 3: Churn vs Industry — lower churn is better
    #   Score 100 at 0% churn, 50 at industry avg, 0 at 3× industry avg
    churn_score = max(0.0, min(100.0, (1.0 - current_churn_pct / (max(0.1, industry_avg_churn_pct) * 3.0)) * 100.0))

    # Axis 4: NRR vs Benchmark — 100 when at benchmark, 0 at 60%, anchored to benchmark
    nrr_score = max(0.0, min(100.0, (projected_nrr_pct - 60.0) / (nrr_benchmark_pct - 60.0) * 100.0))

    # Axis 5: Pipeline Health — (closed_won / total) × velocity bonus
    pipeline_score = min(100.0, win_rate_pct * 1.8)

    # Industry averages on 0-100 scale
    ind_win    = 50.0  # 25% win rate = industry average
    ind_repeat = 50.0  # 40% repeat = industry average
    ind_churn  = 66.7  # at industry average churn → score = (1 - 1/3) * 100 = 66.7
    ind_nrr    = max(0.0, min(100.0, (nrr_benchmark_pct - 5.0 - 60.0) / (nrr_benchmark_pct - 60.0) * 100.0))
    ind_pipe   = 45.0

    top_win    = 90.0
    top_repeat = 90.0
    top_churn  = 90.0
    top_nrr    = 95.0
    top_pipe   = 85.0

    return [
        RadarDataPoint(axis="Win Rate",            startup_value=round(win_score, 1),    industry_avg=ind_win,    top_tier=top_win),
        RadarDataPoint(axis="Repeat Rate",         startup_value=round(repeat_score, 1), industry_avg=ind_repeat, top_tier=top_repeat),
        RadarDataPoint(axis="Churn vs Industry",   startup_value=round(churn_score, 1),  industry_avg=ind_churn,  top_tier=top_churn),
        RadarDataPoint(axis="NRR vs Benchmark",    startup_value=round(nrr_score, 1),    industry_avg=ind_nrr,    top_tier=top_nrr),
        RadarDataPoint(axis="Pipeline Health",     startup_value=round(pipeline_score, 1), industry_avg=ind_pipe, top_tier=top_pipe),
    ]


def _build_headline(
    current_churn_pct: float,
    projected_churn_pct: float,
    projected_nrr_pct: float,
    nrr_benchmark_pct: float,
) -> str:
    churn_delta = round(current_churn_pct - projected_churn_pct, 1)
    nrr_status  = "above" if projected_nrr_pct >= nrr_benchmark_pct else "below"
    return (
        f"AI automation can cut churn by {churn_delta:.1f}pp "
        f"(Metric 9) and push NRR to {projected_nrr_pct:.0f}% "
        f"— {nrr_status} the {nrr_benchmark_pct:.0f}% industry benchmark (Metric 10)."
    )


# ═══════════════════════════════════════════════════════════════════════════
# Public entry point
# ═══════════════════════════════════════════════════════════════════════════

def compute_retention_report(session_id: str, entry: SessionEntry) -> RetentionReport:
    """Compute the full Growth & Retention Benchmarking report for a session.

    Raises:
        ValueError: if sales_inquiries_df is missing or malformed.
        RuntimeError: if GEMINI_API_KEY is not set.
    """
    warnings: list[str] = []

    # ── 1. Parse Pandas metrics ────────────────────────────────────────────
    metrics = _parse_sales_metrics(entry)

    # ── 2. Derive churn + NRR proxies ──────────────────────────────────────
    current_churn_pct = _estimate_current_churn(metrics["repeat_rate_pct"])
    current_nrr_pct   = _estimate_current_nrr(
        metrics["repeat_rate_pct"],
        metrics["win_rate_pct"],
        current_churn_pct,
    )

    # ── 3. Get sub_type from session profile ───────────────────────────────
    profile  = entry.startup_profile or {}
    sub_type = profile.get("sub_type", "SaaS")

    # ── 4. Call LLM for 2026 benchmarks ───────────────────────────────────
    llm_data: dict = {}
    try:
        llm_data = analyse_retention_benchmarks(
            sub_type=sub_type,
            current_churn_pct=current_churn_pct,
            current_nrr_pct=current_nrr_pct,
            win_rate_pct=metrics["win_rate_pct"],
            repeat_rate_pct=metrics["repeat_rate_pct"],
            total_inquiries=metrics["total_inquiries"],
        )
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"LLM benchmark unavailable — using static 2026 baselines. ({exc})")
        fallback = _FALLBACK_BENCHMARKS.get(sub_type.lower(), _FALLBACK_BENCHMARKS["saas"])
        llm_data = {
            "industry_avg_churn_pct": fallback["industry_avg_churn_pct"],
            "top_tier_churn_pct":     fallback["top_tier_churn_pct"],
            "nrr_benchmark_pct":      fallback["nrr_benchmark_pct"],
            "projected_churn_pct":    max(
                fallback["top_tier_churn_pct"],
                current_churn_pct * 0.65,
            ),
            "projected_nrr_pct":      min(130.0, current_nrr_pct + 8.0),
            "growth_levers": [
                "Automate follow-up sequences to re-engage at-risk customers",
                "Deploy AI-driven personalisation to increase repeat purchases",
                "Implement proactive churn prediction using inquiry patterns",
            ],
            "sector_risks": [
                "Pricing pressure from well-funded competitors",
                "High customer acquisition cost without retention loop",
            ],
            "competitor_benchmarks": [
                {"company": "Industry Median", "sector": sub_type, "churn_pct": fallback["industry_avg_churn_pct"], "nrr_pct": fallback["nrr_benchmark_pct"]},
                {"company": "Top Decile",      "sector": sub_type, "churn_pct": fallback["top_tier_churn_pct"],    "nrr_pct": fallback["nrr_benchmark_pct"] + 8.0},
            ],
            "executive_summary": (
                f"Based on static 2026 benchmarks for {sub_type}, your current estimated churn of "
                f"{current_churn_pct:.1f}% compares against an industry average of "
                f"{fallback['industry_avg_churn_pct']:.1f}%. AI-driven retention automation could "
                f"reduce churn to {max(fallback['top_tier_churn_pct'], current_churn_pct * 0.65):.1f}% "
                f"and lift NRR toward {min(130.0, current_nrr_pct + 8.0):.0f}%."
            ),
        }

    # ── 5. Extract + validate LLM fields ──────────────────────────────────
    industry_avg_churn = float(llm_data.get("industry_avg_churn_pct", 4.0))
    top_tier_churn     = float(llm_data.get("top_tier_churn_pct", 2.0))
    nrr_benchmark      = float(llm_data.get("nrr_benchmark_pct", 106.0))
    projected_churn    = float(llm_data.get("projected_churn_pct", current_churn_pct * 0.70))
    projected_nrr      = float(llm_data.get("projected_nrr_pct", current_nrr_pct + 8.0))

    churn_reduction = round(current_churn_pct - projected_churn, 2)

    # ── 6. Build competitor benchmarks ────────────────────────────────────
    raw_comps: list[dict] = llm_data.get("competitor_benchmarks", [])
    competitor_benchmarks = [
        CompetitorChurnBenchmark(
            company=c.get("company", "Unknown"),
            sector=c.get("sector", sub_type),
            churn_pct=float(c.get("churn_pct", 4.0)),
            nrr_pct=float(c["nrr_pct"]) if c.get("nrr_pct") is not None else None,
        )
        for c in raw_comps[:5]
    ]

    # ── 7. Build radar chart data ─────────────────────────────────────────
    radar_data = _build_radar(
        win_rate_pct=metrics["win_rate_pct"],
        repeat_rate_pct=metrics["repeat_rate_pct"],
        current_churn_pct=current_churn_pct,
        industry_avg_churn_pct=industry_avg_churn,
        projected_nrr_pct=projected_nrr,
        nrr_benchmark_pct=nrr_benchmark,
        closed_won_count=metrics["closed_won_count"],
        total_inquiries=metrics["total_inquiries"],
    )

    # ── 8. Warnings ────────────────────────────────────────────────────────
    if metrics["total_inquiries"] < 20:
        warnings.append(
            f"Only {metrics['total_inquiries']} inquiries found — churn estimates are indicative, "
            "not statistically reliable. Upload more data for higher accuracy."
        )
    if projected_nrr < nrr_benchmark:
        warnings.append(
            f"Projected NRR ({projected_nrr:.0f}%) remains below the "
            f"{sub_type} benchmark ({nrr_benchmark:.0f}%). "
            "Consider upsell/cross-sell automation to close the gap."
        )

    headline = _build_headline(current_churn_pct, projected_churn, projected_nrr, nrr_benchmark)

    return RetentionReport(
        session_id=session_id,
        # Raw stats
        total_inquiries=metrics["total_inquiries"],
        closed_won_count=metrics["closed_won_count"],
        repeat_customer_count=metrics["repeat_customer_count"],
        new_customer_count=metrics["new_customer_count"],
        lost_count=metrics["lost_count"],
        pending_count=metrics["pending_count"],
        win_rate_pct=metrics["win_rate_pct"],
        repeat_rate_pct=metrics["repeat_rate_pct"],
        # Metric 9
        current_churn_pct=current_churn_pct,
        projected_churn_pct=round(projected_churn, 2),
        churn_reduction_pct=churn_reduction,
        industry_avg_churn_pct=round(industry_avg_churn, 2),
        top_tier_churn_pct=round(top_tier_churn, 2),
        # Metric 10
        current_nrr_pct=current_nrr_pct,
        projected_nrr_pct=round(projected_nrr, 1),
        nrr_benchmark_pct=round(nrr_benchmark, 1),
        # LLM insights
        growth_levers=llm_data.get("growth_levers", []),
        sector_risks=llm_data.get("sector_risks", []),
        competitor_benchmarks=competitor_benchmarks,
        # Radar
        radar_data=radar_data,
        # Narrative
        headline=headline,
        executive_summary=llm_data.get("executive_summary", ""),
        warnings=warnings,
    )
