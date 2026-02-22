"""Tests for Module 3 — POST /api/v1/analyze/bottleneck."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.services.benchmark import compute_bottleneck_report, _find_col, _BOTTLENECK_THRESHOLD_HOURS

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(base: datetime, offset_hours: float) -> str:
    return (base + timedelta(hours=offset_hours)).strftime("%Y-%m-%d %H:%M:%S")


BASE = datetime(2026, 2, 1, 10, 0, 0)

# SkillSphere-style dataset: 7 closed, 3 pending
# TATs (closed): 28.5, 121.8, 21.8, 98.0, 25.0, 98.8, 26.2  (hours)
# Bottlenecks (>48h):  INQ002 (121.8), INQ005 (98.0), INQ008 (98.8)  → 3 bottlenecks
_SAMPLE_DF = pd.DataFrame({
    "Inquiry_ID":    ["INQ001","INQ002","INQ003","INQ004","INQ005",
                      "INQ006","INQ007","INQ008","INQ009","INQ010"],
    "Inquiry_Date":  [_dt(BASE, 0),  _dt(BASE, 23.25), _dt(BASE, 54.75),
                      _dt(BASE, 73.33), _dt(BASE, 100), _dt(BASE, 120.5),
                      _dt(BASE, 143), _dt(BASE, 173.25), _dt(BASE, 193.75),
                      _dt(BASE, 219.33)],
    "Payment_Date":  [_dt(BASE, 28.5), _dt(BASE, 145.08), None,
                      _dt(BASE, 95.17), _dt(BASE, 198.0), None,
                      _dt(BASE, 168.0), _dt(BASE, 272.08), None,
                      _dt(BASE, 245.5)],
    "Amount_INR":    [50000, 75000, None, 45000, 120000, None, 60000, 90000, None, 55000],
    "Status":        ["Closed Won","Closed Won","Pending","Closed Won","Closed Won",
                      "Lost","Closed Won","Closed Won","Pending","Closed Won"],
    "Repeat_Customer_Flag": ["Yes","No","No","Yes","No","No","Yes","No","Yes","No"],
})


def _make_session_with_inquiries(df: pd.DataFrame | None = None) -> str:
    return session_store.create(
        startup_profile={"company_name": "SkillSphere India", "num_employees": 10},
        sales_inquiries_df=df if df is not None else _SAMPLE_DF.copy(),
        data_issues=[],
    )


def _make_session_no_inquiries() -> str:
    return session_store.create(
        startup_profile={"company_name": "SkillSphere India"},
        data_issues=[],
    )


def _post_bottleneck(session_id: str):
    return client.post("/api/v1/analyze/bottleneck", json={"session_id": session_id})


# ---------------------------------------------------------------------------
# Tests: happy path
# ---------------------------------------------------------------------------

def test_bottleneck_happy_path_status_200():
    sid = _make_session_with_inquiries()
    resp = _post_bottleneck(sid)
    assert resp.status_code == 200


def test_bottleneck_correct_totals():
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert body["total_inquiries"] == 10
    assert body["closed_inquiries"] == 7


def test_bottleneck_count_and_pct():
    """INQ002 (121.8h), INQ005 (98.0h), INQ008 (98.8h) are >48h → 3 bottlenecks."""
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert body["bottleneck_count"] == 3
    assert body["bottleneck_pct"] == pytest.approx(3 / 7 * 100, abs=0.5)


def test_bottleneck_avg_tat_is_positive():
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert body["avg_tat_hours"] > 0
    assert body["median_tat_hours"] > 0
    assert body["max_tat_hours"] >= body["avg_tat_hours"]
    assert body["min_tat_hours"] <= body["avg_tat_hours"]


def test_bottleneck_tat_improvement_pct_metric_11():
    """Metric 11: improvement % = (avg_tat - 2) / avg_tat * 100."""
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    avg = body["avg_tat_hours"]
    expected = round(((avg - 2.0) / avg) * 100, 2)
    assert body["avg_tat_improvement_pct"] == pytest.approx(expected, abs=0.1)


def test_bottleneck_total_hours_saved_metric_4():
    """Metric 4: sum of (tat - 2h) clipped to 0 over all closed inquiries."""
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert body["total_hours_saved"] > 0


def test_bottleneck_inquiry_tat_list_length():
    """Per-inquiry list must contain all 10 rows (including pending)."""
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert len(body["inquiry_tat_list"]) == 10


def test_bottleneck_pending_rows_have_null_tat():
    """Rows without Payment_Date should have tat_hours = null."""
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    null_tat_rows = [r for r in body["inquiry_tat_list"] if r["tat_hours"] is None]
    assert len(null_tat_rows) == 3   # INQ003, INQ006, INQ009


def test_bottleneck_mermaid_flowchart_non_empty():
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    mermaid = body["mermaid_flowchart"]
    assert isinstance(mermaid, str) and len(mermaid) > 50
    assert "flowchart" in mermaid


def test_bottleneck_recommendations_non_empty():
    sid = _make_session_with_inquiries()
    body = _post_bottleneck(sid).json()
    assert len(body["recommendations"]) >= 1


def test_bottleneck_stored_in_session():
    """Result must be persisted as benchmark_report for Module 7."""
    sid = _make_session_with_inquiries()
    _post_bottleneck(sid)
    entry = session_store.get(sid)
    assert entry is not None
    assert entry.benchmark_report is not None
    assert entry.benchmark_report.session_id == sid   # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Tests: edge cases
# ---------------------------------------------------------------------------

def test_bottleneck_session_not_found_404():
    resp = _post_bottleneck("nonexistent_abc123")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


def test_bottleneck_no_sales_inquiries_422():
    """Session without sales_inquiries_df → 422."""
    sid = _make_session_no_inquiries()
    resp = _post_bottleneck(sid)
    assert resp.status_code == 422


def test_bottleneck_zero_bottlenecks_all_fast():
    """All TATs ≤ 48h → bottleneck_count == 0."""
    fast_df = pd.DataFrame({
        "Inquiry_ID":   ["INQ001", "INQ002", "INQ003"],
        "Inquiry_Date": [_dt(BASE, 0), _dt(BASE, 24), _dt(BASE, 48)],
        "Payment_Date": [_dt(BASE, 10), _dt(BASE, 30), _dt(BASE, 60)],
        "Status":       ["Closed Won", "Closed Won", "Closed Won"],
    })
    sid = _make_session_with_inquiries(fast_df)
    body = _post_bottleneck(sid).json()
    assert body["bottleneck_count"] == 0
    assert body["bottleneck_pct"] == 0.0
    assert body["closed_inquiries"] == 3


def test_bottleneck_all_pending_returns_zero_stats():
    """All rows missing Payment_Date → closed_inquiries == 0, no error."""
    pending_df = pd.DataFrame({
        "Inquiry_ID":   ["INQ001", "INQ002"],
        "Inquiry_Date": [_dt(BASE, 0), _dt(BASE, 24)],
        "Payment_Date": [None, None],
        "Status":       ["Pending", "Pending"],
    })
    sid = _make_session_with_inquiries(pending_df)
    body = _post_bottleneck(sid).json()
    assert body["closed_inquiries"] == 0
    assert body["avg_tat_hours"] == 0.0
    assert body["bottleneck_count"] == 0


def test_bottleneck_tat_exact_value():
    """Single inquiry with exactly 120h TAT → avg_tat_hours ≈ 120, is_bottleneck True."""
    df = pd.DataFrame({
        "Inquiry_ID":   ["INQ001"],
        "Inquiry_Date": [_dt(BASE, 0)],
        "Payment_Date": [_dt(BASE, 120)],
        "Status":       ["Closed Won"],
    })
    sid = _make_session_with_inquiries(df)
    body = _post_bottleneck(sid).json()
    assert body["avg_tat_hours"] == pytest.approx(120.0, abs=0.01)
    assert body["bottleneck_count"] == 1
    assert body["inquiry_tat_list"][0]["is_bottleneck"] is True


# ---------------------------------------------------------------------------
# Unit tests: service helpers
# ---------------------------------------------------------------------------

def test_find_col_case_insensitive():
    df = pd.DataFrame({"Inquiry_Date": [], "Payment_Date": []})
    assert _find_col(df, {"inquiry_date"}) == "Inquiry_Date"
    assert _find_col(df, {"payment_date"}) == "Payment_Date"
    assert _find_col(df, {"nonexistent"}) is None


def test_find_col_exact_match():
    df = pd.DataFrame({"inquiry_date": [], "PAYMENT_DATE": []})
    assert _find_col(df, {"inquiry_date"}) == "inquiry_date"
    assert _find_col(df, {"payment_date"}) == "PAYMENT_DATE"
