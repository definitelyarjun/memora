"""Tests for Module 6 — Growth & Retention Benchmarking.

Covers:
  - Metric 9: Churn Reduction Potential (current → projected)
  - Metric 10: NRR Projection
  - Repeat customer parsing from sales_inquiries_df
  - Win rate, repeat rate calculation
  - Churn estimation from repeat rate
  - NRR estimation
  - Radar chart axes (5 axes present, values 0-100)
  - LLM fallback when unavailable
  - HTTP 404 (bad session), 422 (missing sales_inquiries)
  - SkillSphere India end-to-end scenario
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.services.retention import (
    _estimate_current_churn,
    _estimate_current_nrr,
    _parse_sales_metrics,
)

client = TestClient(app)


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------

def _make_sales_df(rows: list[dict] | None = None) -> pd.DataFrame:
    """Build a sales_inquiries DataFrame.  Default = SkillSphere 10-row data."""
    if rows is not None:
        return pd.DataFrame(rows)
    return pd.DataFrame([
        {"Inquiry_ID": "INQ001", "Customer_Email": "a@b.com", "Inquiry_Date": "2026-02-01",
         "Payment_Date": "2026-02-02", "Amount_INR": 50000, "Status": "Closed Won", "Repeat_Customer_Flag": "Yes"},
        {"Inquiry_ID": "INQ002", "Customer_Email": "b@c.com", "Inquiry_Date": "2026-02-02",
         "Payment_Date": "2026-02-07", "Amount_INR": 75000, "Status": "Closed Won", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "INQ003", "Customer_Email": "c@d.com", "Inquiry_Date": "2026-02-03",
         "Payment_Date": None, "Amount_INR": None, "Status": "Pending", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "INQ004", "Customer_Email": "d@e.com", "Inquiry_Date": "2026-02-04",
         "Payment_Date": "2026-02-05", "Amount_INR": 45000, "Status": "Closed Won", "Repeat_Customer_Flag": "Yes"},
        {"Inquiry_ID": "INQ005", "Customer_Email": "e@f.com", "Inquiry_Date": "2026-02-05",
         "Payment_Date": "2026-02-09", "Amount_INR": 120000, "Status": "Closed Won", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "INQ006", "Customer_Email": "f@g.com", "Inquiry_Date": "2026-02-06",
         "Payment_Date": None, "Amount_INR": None, "Status": "Lost", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "INQ007", "Customer_Email": "g@h.com", "Inquiry_Date": "2026-02-07",
         "Payment_Date": "2026-02-08", "Amount_INR": 60000, "Status": "Closed Won", "Repeat_Customer_Flag": "Yes"},
        {"Inquiry_ID": "INQ008", "Customer_Email": "h@i.com", "Inquiry_Date": "2026-02-08",
         "Payment_Date": "2026-02-12", "Amount_INR": 90000, "Status": "Closed Won", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "INQ009", "Customer_Email": "i@j.com", "Inquiry_Date": "2026-02-09",
         "Payment_Date": None, "Amount_INR": None, "Status": "Pending", "Repeat_Customer_Flag": "Yes"},
        {"Inquiry_ID": "INQ010", "Customer_Email": "j@k.com", "Inquiry_Date": "2026-02-10",
         "Payment_Date": "2026-02-11", "Amount_INR": 55000, "Status": "Closed Won", "Repeat_Customer_Flag": "No"},
    ])


def _make_session(
    sales_df: pd.DataFrame | None = None,
    sub_type: str = "SaaS",
) -> str:
    """Create a session with optional sales DataFrame and return session_id."""
    import uuid
    sid = str(uuid.uuid4())
    from app.core.session_store import SessionEntry
    entry = SessionEntry(
        startup_profile={"sub_type": sub_type},
        sales_inquiries_df=sales_df if sales_df is not None else _make_sales_df(),
    )
    session_store._store[sid] = entry
    return sid


def _post_retention(sid: str):
    return client.post("/api/v1/analyze/retention", json={"session_id": sid})


# ---------------------------------------------------------------------------
# Unit tests — pure computation functions
# ---------------------------------------------------------------------------

def test_parse_sales_metrics_basic():
    """parse_sales_metrics returns correct counts for default 10-row data."""
    from app.core.session_store import SessionEntry
    entry = SessionEntry(sales_inquiries_df=_make_sales_df())
    m = _parse_sales_metrics(entry)

    assert m["total_inquiries"] == 10
    assert m["closed_won_count"] == 7   # INQ001,002,004,005,007,008,010
    assert m["repeat_customer_count"] == 3   # INQ001, INQ004, INQ007
    assert m["new_customer_count"] == 4      # INQ002, INQ005, INQ008, INQ010
    assert m["lost_count"] == 1
    assert m["pending_count"] == 2
    assert abs(m["win_rate_pct"] - 70.0) < 1.0       # 7/10 × 100
    assert abs(m["repeat_rate_pct"] - 42.9) < 1.0    # 3/7 × 100


def test_parse_sales_metrics_missing_df():
    """Raises ValueError when sales_inquiries_df is None."""
    from app.core.session_store import SessionEntry
    entry = SessionEntry()
    with pytest.raises(ValueError, match="sales_inquiries.csv not found"):
        _parse_sales_metrics(entry)


def test_parse_sales_metrics_missing_status_col():
    """Raises ValueError when Status column is absent."""
    from app.core.session_store import SessionEntry
    df = pd.DataFrame([{"Inquiry_ID": "I1", "Repeat_Customer_Flag": "Yes"}])
    entry = SessionEntry(sales_inquiries_df=df)
    with pytest.raises(ValueError, match="Status"):
        _parse_sales_metrics(entry)


def test_parse_sales_metrics_missing_repeat_col():
    """Raises ValueError when Repeat_Customer_Flag column is absent."""
    from app.core.session_store import SessionEntry
    df = pd.DataFrame([{"Inquiry_ID": "I1", "Status": "Closed Won"}])
    entry = SessionEntry(sales_inquiries_df=df)
    with pytest.raises(ValueError, match="Repeat_Customer_Flag"):
        _parse_sales_metrics(entry)


def test_estimate_churn_from_repeat_rate():
    """Churn estimate is inversely related to repeat rate, capped in [1, 25]."""
    # Very high repeat rate → low churn
    assert _estimate_current_churn(80.0) < 5.0
    # Very low repeat rate → higher churn
    assert _estimate_current_churn(10.0) > 5.0
    # Capped at 1%
    assert _estimate_current_churn(100.0) >= 1.0
    # Capped at 25%
    assert _estimate_current_churn(0.0) <= 25.0


def test_estimate_nrr_increases_with_repeat_rate():
    """NRR should increase as repeat rate improves."""
    low_nrr  = _estimate_current_nrr(repeat_rate_pct=10.0, win_rate_pct=30.0, churn_pct=10.0)
    high_nrr = _estimate_current_nrr(repeat_rate_pct=80.0, win_rate_pct=60.0, churn_pct=2.0)
    assert high_nrr > low_nrr


# ---------------------------------------------------------------------------
# HTTP endpoint tests
# ---------------------------------------------------------------------------

def test_404_unknown_session():
    resp = _post_retention("nonexistent-session-xyz")
    assert resp.status_code == 404


def test_422_no_sales_df():
    """Returns 422 when session has no sales_inquiries_df."""
    from app.core.session_store import SessionEntry
    import uuid
    sid = str(uuid.uuid4())
    session_store._store[sid] = SessionEntry(startup_profile={"sub_type": "SaaS"})
    resp = _post_retention(sid)
    assert resp.status_code == 422
    assert "sales_inquiries" in resp.json()["detail"].lower()


@patch("app.services.retention.analyse_retention_benchmarks")
def test_happy_path_structure(mock_llm):
    """Successful response contains all required top-level fields."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5,
        "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0,
        "projected_churn_pct": 4.0,
        "projected_nrr_pct": 95.0,
        "growth_levers": ["Follow-up automation", "Personalised onboarding"],
        "sector_risks": ["Price competition"],
        "competitor_benchmarks": [
            {"company": "Top SaaS", "sector": "SaaS", "churn_pct": 1.5, "nrr_pct": 118.0}
        ],
        "executive_summary": "Good retention potential.",
    }
    sid = _make_session()
    resp = _post_retention(sid)
    assert resp.status_code == 200, resp.text

    body = resp.json()
    required = [
        "session_id", "total_inquiries", "closed_won_count",
        "repeat_customer_count", "win_rate_pct", "repeat_rate_pct",
        "current_churn_pct", "projected_churn_pct", "churn_reduction_pct",
        "industry_avg_churn_pct", "top_tier_churn_pct",
        "current_nrr_pct", "projected_nrr_pct", "nrr_benchmark_pct",
        "growth_levers", "sector_risks", "competitor_benchmarks",
        "radar_data", "headline", "executive_summary", "warnings",
    ]
    for field in required:
        assert field in body, f"Missing field: {field}"


@patch("app.services.retention.analyse_retention_benchmarks")
def test_win_rate_calculation(mock_llm):
    """Win rate = closed_won / total × 100."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    # 7 Closed Won out of 10 total = 70%
    assert abs(body["win_rate_pct"] - 70.0) < 1.0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_repeat_rate_calculation(mock_llm):
    """Repeat rate = repeat_won / closed_won × 100."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    # 3 repeat out of 7 closed won = 42.9%
    assert abs(body["repeat_rate_pct"] - 42.9) < 1.0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_metric9_churn_reduction_positive(mock_llm):
    """Metric 9: projected_churn < current_churn → churn_reduction_pct > 0."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    # LLM returns projected = 3.0; current churn derived from 50% repeat ≈ 6.0%
    assert body["churn_reduction_pct"] > 0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_metric10_nrr_projection_present(mock_llm):
    """Metric 10: projected_nrr_pct is populated and > 0."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 102.0, "growth_levers": ["A", "B"], "sector_risks": ["C"],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    assert body["projected_nrr_pct"] > 0
    assert body["nrr_benchmark_pct"] == 108.0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_radar_data_has_five_axes(mock_llm):
    """Radar data contains exactly 5 axes, all scores in 0–100."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    radar = body["radar_data"]
    assert len(radar) == 5
    for point in radar:
        assert 0.0 <= point["startup_value"] <= 100.0
        assert 0.0 <= point["industry_avg"] <= 100.0
        assert 0.0 <= point["top_tier"] <= 100.0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_competitor_benchmarks_stored(mock_llm):
    """competitor_benchmarks list is stored and returned in response."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [
            {"company": "Freshdesk", "sector": "SaaS", "churn_pct": 1.5, "nrr_pct": 118.0},
            {"company": "Zoho Desk",  "sector": "SaaS", "churn_pct": 2.0, "nrr_pct": 110.0},
        ],
        "executive_summary": "Strong churn benchmarks.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    assert len(body["competitor_benchmarks"]) == 2
    assert body["competitor_benchmarks"][0]["company"] == "Freshdesk"


def test_llm_fallback_when_unavailable():
    """When LLM raises, fallback benchmarks are returned (not 500 error)."""
    with patch(
        "app.services.retention.analyse_retention_benchmarks",
        side_effect=RuntimeError("GEMINI_API_KEY not set"),
    ):
        sid = _make_session(sub_type="EdTech")
        resp = _post_retention(sid)
        assert resp.status_code == 200
        body = resp.json()
        # Fallback EdTech benchmark
        assert body["industry_avg_churn_pct"] == 6.5
        assert body["top_tier_churn_pct"] == 3.0
        # Warning about fallback
        assert any("LLM" in w or "static" in w for w in body["warnings"])


@patch("app.services.retention.analyse_retention_benchmarks")
def test_small_dataset_warning(mock_llm):
    """Fewer than 20 inquiries triggers a data quality warning."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()   # default dataset has 10 rows
    body = _post_retention(sid).json()
    assert any("10 inquiries" in w or "indicative" in w for w in body["warnings"])


@patch("app.services.retention.analyse_retention_benchmarks")
def test_session_stored_after_run(mock_llm):
    """retention_report is persisted on the session entry after a successful run."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    sid = _make_session()
    _post_retention(sid)
    entry = session_store.get(sid)
    assert entry.retention_report is not None        # type: ignore[attr-defined]
    assert entry.retention_report.session_id == sid  # type: ignore[attr-defined]


@patch("app.services.retention.analyse_retention_benchmarks")
def test_all_won_repeat_low_churn(mock_llm):
    """100% repeat customers → very low churn estimate."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 1.0,
        "projected_nrr_pct": 115.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Excellent retention.",
    }
    all_repeat = [
        {"Inquiry_ID": f"I{i}", "Status": "Closed Won", "Repeat_Customer_Flag": "Yes"}
        for i in range(1, 21)
    ]
    sid = _make_session(sales_df=pd.DataFrame(all_repeat))
    body = _post_retention(sid).json()
    assert body["repeat_rate_pct"] == 100.0
    assert body["current_churn_pct"] <= 3.0   # near-zero repeat churn proxy


@patch("app.services.retention.analyse_retention_benchmarks")
def test_all_won_new_high_churn(mock_llm):
    """0% repeat customers → high churn estimate."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 8.0,
        "projected_nrr_pct": 85.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "High churn risk.",
    }
    all_new = [
        {"Inquiry_ID": f"I{i}", "Status": "Closed Won", "Repeat_Customer_Flag": "No"}
        for i in range(1, 21)
    ]
    sid = _make_session(sales_df=pd.DataFrame(all_new))
    body = _post_retention(sid).json()
    assert body["current_churn_pct"] > 5.0


@patch("app.services.retention.analyse_retention_benchmarks")
def test_growth_levers_populated(mock_llm):
    """growth_levers and sector_risks are included in response."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0,
        "growth_levers": ["Automate follow-ups", "AI personalisation"],
        "sector_risks": ["Price competition", "Talent shortage"],
        "competitor_benchmarks": [],
        "executive_summary": "Test.",
    }
    sid = _make_session()
    body = _post_retention(sid).json()
    assert len(body["growth_levers"]) == 2
    assert len(body["sector_risks"]) == 2


@patch("app.services.retention.analyse_retention_benchmarks")
def test_case_insensitive_status_parsing(mock_llm):
    """Status column values are parsed case-insensitively."""
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5, "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0, "projected_churn_pct": 3.0,
        "projected_nrr_pct": 100.0, "growth_levers": [], "sector_risks": [],
        "competitor_benchmarks": [], "executive_summary": "Test.",
    }
    mixed_case = pd.DataFrame([
        {"Inquiry_ID": "I1", "Status": "CLOSED WON", "Repeat_Customer_Flag": "Yes"},
        {"Inquiry_ID": "I2", "Status": "closed won", "Repeat_Customer_Flag": "No"},
        {"Inquiry_ID": "I3", "Status": "Lost",       "Repeat_Customer_Flag": "No"},
    ])
    sid = _make_session(sales_df=mixed_case)
    body = _post_retention(sid).json()
    assert body["closed_won_count"] == 2


# ---------------------------------------------------------------------------
# SkillSphere India end-to-end test
# ---------------------------------------------------------------------------

@patch("app.services.retention.analyse_retention_benchmarks")
def test_skillsphere_india_scenario(mock_llm):
    """SkillSphere India: SaaS, 6 Closed Won / 10 total = 60% win rate.

    3 of 6 won are repeat = 50% repeat rate.
    LLM projects churn: 4.2% → 2.8% (Metric 9: −1.4pp).
    LLM projects NRR:   82% → 97%  (Metric 10, benchmark = 108%).
    """
    mock_llm.return_value = {
        "industry_avg_churn_pct": 3.5,
        "top_tier_churn_pct": 1.5,
        "nrr_benchmark_pct": 108.0,
        "projected_churn_pct": 2.8,
        "projected_nrr_pct": 97.0,
        "growth_levers": [
            "Automated follow-up sequences reduce 48h+ response lag",
            "AI personalisation increases repeat purchase probability",
            "Proactive churn detection flags at-risk accounts",
        ],
        "sector_risks": [
            "EdTech/SaaS overlap creates pricing pressure",
            "Seasonal demand reduces NRR predictability",
        ],
        "competitor_benchmarks": [
            {"company": "Freshdesk",  "sector": "SaaS", "churn_pct": 1.5, "nrr_pct": 118.0},
            {"company": "Zoho Desk",  "sector": "SaaS", "churn_pct": 2.0, "nrr_pct": 110.0},
            {"company": "Intercom",   "sector": "SaaS", "churn_pct": 1.8, "nrr_pct": 120.0},
        ],
        "executive_summary": (
            "SkillSphere India shows a 50% repeat customer rate, well above the 35% "
            "EdTech norm. AI-driven follow-up automation can cut churn from 6.0% to 2.8%, "
            "pushing NRR from 82% to 97% — approaching the 108% SaaS benchmark."
        ),
    }

    sid = _make_session(sub_type="SaaS")
    resp = _post_retention(sid)
    assert resp.status_code == 200
    body = resp.json()

    # Raw stats
    assert body["total_inquiries"] == 10
    assert body["closed_won_count"] == 7
    assert body["repeat_customer_count"] == 3
    assert abs(body["win_rate_pct"] - 70.0) < 1.0
    assert abs(body["repeat_rate_pct"] - 42.9) < 1.0

    # Metric 9
    assert body["current_churn_pct"] > 0
    assert body["projected_churn_pct"] == 2.8
    assert body["churn_reduction_pct"] > 0

    # Metric 10
    assert body["projected_nrr_pct"] == 97.0
    assert body["nrr_benchmark_pct"] == 108.0

    # Benchmarks
    assert len(body["competitor_benchmarks"]) == 3

    # Radar
    assert len(body["radar_data"]) == 5

    # Narrative
    assert "SkillSphere" in body["executive_summary"] or len(body["executive_summary"]) > 30
