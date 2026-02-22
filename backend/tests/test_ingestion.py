"""Tests for Module 1 — Startup Ingestion & Profiling.

FoundationIQ 3.0 (Startup Edition)
"""

from __future__ import annotations

import io
import json
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas.ingestion import (
    IssueType,
    StartupProfile,
    StartupProfileAnalysis,
)

client = TestClient(app)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

ONBOARDING_FORM = {
    "company_name": "TestCo SaaS",
    "sub_type": "SaaS",
    "mrr_last_3_months": json.dumps([80000, 95000, 110000]),
    "monthly_growth_goal_pct": 15.0,
    "patience_months": 6,
    "current_tech_stack": "Stripe, Zapier, Google Sheets",
    "num_employees": 12,
    "industry": "Technology",
}

_MOCK_PROFILE_ANALYSIS = StartupProfileAnalysis(
    mrr_trend="Growing",
    mrr_mom_growth_pct=17.2,
    growth_gap="Actual growth (~17%) exceeds the 15% target — on track.",
    tech_stack_maturity="Developing",
    key_observations=[
        "MRR shows healthy upward trend over 3 months",
        "Small team of 12 — high automation potential per head",
        "Tech stack includes Stripe (billing) but lacks CRM",
    ],
    recommended_focus_areas=["Sales cycle automation", "Churn monitoring"],
    executive_summary="TestCo is a growing SaaS startup with strong MRR momentum.",
)


def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def _sample_org_chart() -> pd.DataFrame:
    return pd.DataFrame({
        "role": ["Engineer", "Designer", "PM", "Sales Rep"],
        "department": ["Engineering", "Design", "Product", "Sales"],
        "salary": [120000, 90000, 110000, 85000],
    })


def _sample_expenses() -> pd.DataFrame:
    return pd.DataFrame({
        "category": ["AWS", "Slack", "Zoom", "Google Workspace"],
        "amount": [15000, 3000, 2000, 5000],
        "month": ["2025-01", "2025-01", "2025-01", "2025-01"],
    })


def _sample_sales_inquiries() -> pd.DataFrame:
    return pd.DataFrame({
        "inquiry_date": ["2025-01-01", "2025-01-05", "2025-01-10"],
        "payment_date": ["2025-01-15", "2025-01-20", None],
        "repeat_customer": ["Yes", "No", "Yes"],
        "amount": [50000, 30000, 25000],
    })


# ---------------------------------------------------------------------------
# Test: Health check
# ---------------------------------------------------------------------------

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["app"] == "FoundationIQ"


# ---------------------------------------------------------------------------
# Test: Successful ingestion with all 3 files
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_startup_ingest_all_files(mock_llm):
    org = _csv_bytes(_sample_org_chart())
    exp = _csv_bytes(_sample_expenses())
    sales = _csv_bytes(_sample_sales_inquiries())

    resp = client.post(
        "/api/v1/ingest/startup",
        files={
            "org_chart_file": ("org_chart.csv", io.BytesIO(org), "text/csv"),
            "expenses_file": ("expenses.csv", io.BytesIO(exp), "text/csv"),
            "sales_inquiries_file": ("sales_inquiries.csv", io.BytesIO(sales), "text/csv"),
        },
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()

    assert body["session_id"]
    assert body["startup_profile"]["company_name"] == "TestCo SaaS"
    assert body["startup_profile"]["sub_type"] == "SaaS"
    assert body["startup_profile"]["mrr_last_3_months"] == [80000, 95000, 110000]
    assert set(body["files_uploaded"]) == {"org_chart", "expenses", "sales_inquiries"}
    assert body["total_rows"] == 4 + 4 + 3  # org(4) + exp(4) + sales(3)

    # Per-file summaries present
    assert body["org_chart"] is not None
    assert body["org_chart"]["row_count"] == 4
    assert body["expenses"] is not None
    assert body["expenses"]["row_count"] == 4
    assert body["sales_inquiries"] is not None
    assert body["sales_inquiries"]["row_count"] == 3


# ---------------------------------------------------------------------------
# Test: Ingestion with no files (onboarding only)
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_startup_ingest_no_files(mock_llm):
    resp = client.post(
        "/api/v1/ingest/startup",
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["session_id"]
    assert body["files_uploaded"] == []
    assert body["org_chart"] is None
    assert body["expenses"] is None
    assert body["sales_inquiries"] is None
    assert body["total_rows"] == 0
    assert body["total_issues"] == 0


# ---------------------------------------------------------------------------
# Test: Only org_chart uploaded
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_startup_ingest_org_chart_only(mock_llm):
    org = _csv_bytes(_sample_org_chart())

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"org_chart_file": ("org_chart.csv", io.BytesIO(org), "text/csv")},
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["files_uploaded"] == ["org_chart"]
    assert body["org_chart"]["row_count"] == 4
    assert body["expenses"] is None
    assert body["sales_inquiries"] is None


# ---------------------------------------------------------------------------
# Test: Profile analysis returned
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_profile_analysis_returned(mock_llm):
    resp = client.post(
        "/api/v1/ingest/startup",
        data=ONBOARDING_FORM,
    )

    body = resp.json()
    pa = body["profile_analysis"]
    assert pa is not None
    assert pa["mrr_trend"] == "Growing"
    assert pa["mrr_mom_growth_pct"] == 17.2
    assert pa["tech_stack_maturity"] == "Developing"
    assert len(pa["key_observations"]) >= 1
    assert len(pa["recommended_focus_areas"]) >= 1


# ---------------------------------------------------------------------------
# Test: LLM failure is non-fatal
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", side_effect=RuntimeError("No API key"))
def test_ingest_succeeds_without_llm(mock_llm):
    resp = client.post(
        "/api/v1/ingest/startup",
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["profile_analysis"] is None


# ---------------------------------------------------------------------------
# Test: Invalid MRR JSON → 422
# ---------------------------------------------------------------------------

def test_invalid_mrr_json():
    form = {**ONBOARDING_FORM, "mrr_last_3_months": "not-json"}
    resp = client.post("/api/v1/ingest/startup", data=form)
    assert resp.status_code == 422
    assert "mrr_last_3_months" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Test: MRR wrong length → 422
# ---------------------------------------------------------------------------

def test_mrr_wrong_length():
    form = {**ONBOARDING_FORM, "mrr_last_3_months": json.dumps([100, 200])}
    resp = client.post("/api/v1/ingest/startup", data=form)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: Invalid sub_type → 422
# ---------------------------------------------------------------------------

def test_invalid_sub_type():
    form = {**ONBOARDING_FORM, "sub_type": "Biotech"}
    resp = client.post("/api/v1/ingest/startup", data=form)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: Unsupported file type → 400
# ---------------------------------------------------------------------------

def test_unsupported_file_type():
    resp = client.post(
        "/api/v1/ingest/startup",
        files={"org_chart_file": ("org.json", io.BytesIO(b'{}'), "application/json")},
        data=ONBOARDING_FORM,
    )
    assert resp.status_code == 400
    assert "Unsupported file type" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Test: Missing expected columns flagged
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_missing_expected_columns_flagged(mock_llm):
    """Upload an org_chart without the 'salary' column — should flag it."""
    bad_org = pd.DataFrame({
        "role": ["Engineer", "Designer"],
        "department": ["Engineering", "Design"],
        # 'salary' column missing
    })
    csv_data = _csv_bytes(bad_org)

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"org_chart_file": ("org_chart.csv", io.BytesIO(csv_data), "text/csv")},
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    org = body["org_chart"]
    issue_types = [i["issue_type"] for i in org["data_issues"]]
    assert "missing_expected_columns" in issue_types


# ---------------------------------------------------------------------------
# Test: Missing values flagged in sales_inquiries
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_missing_values_flagged(mock_llm):
    sales = _sample_sales_inquiries()  # has 1 null in payment_date
    csv_data = _csv_bytes(sales)

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"sales_inquiries_file": ("sales.csv", io.BytesIO(csv_data), "text/csv")},
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    issues = body["sales_inquiries"]["data_issues"]
    missing = [i for i in issues if i["issue_type"] == "missing_values"]
    assert missing, "Expected missing_values issue for payment_date column"


# ---------------------------------------------------------------------------
# Test: Duplicate rows flagged
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_duplicate_rows_flagged(mock_llm):
    df = pd.DataFrame({
        "category": ["AWS", "AWS", "Slack"],
        "amount": [15000, 15000, 3000],
        "month": ["2025-01", "2025-01", "2025-01"],
    })
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"expenses_file": ("expenses.csv", io.BytesIO(csv_data), "text/csv")},
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    issues = body["expenses"]["data_issues"]
    dup_issues = [i for i in issues if i["issue_type"] == "duplicate_rows"]
    assert dup_issues, "Expected duplicate_rows issue"


# ---------------------------------------------------------------------------
# Test: Unparsed date columns flagged
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_unparsed_dates_flagged(mock_llm):
    sales = _sample_sales_inquiries()
    csv_data = _csv_bytes(sales)

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"sales_inquiries_file": ("sales.csv", io.BytesIO(csv_data), "text/csv")},
        data=ONBOARDING_FORM,
    )

    assert resp.status_code == 200
    body = resp.json()
    issues = body["sales_inquiries"]["data_issues"]
    unparsed = [i for i in issues if i["issue_type"] == "unparsed_dates"]
    assert unparsed, "Expected unparsed_dates issue for date columns"


# ---------------------------------------------------------------------------
# Test: Tech stack parsed correctly
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_tech_stack_parsing(mock_llm):
    resp = client.post(
        "/api/v1/ingest/startup",
        data=ONBOARDING_FORM,
    )

    body = resp.json()
    stack = body["startup_profile"]["current_tech_stack"]
    assert "Stripe" in stack
    assert "Zapier" in stack
    assert "Google Sheets" in stack


# ---------------------------------------------------------------------------
# Test: Session ID can be retrieved
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_startup_profile", return_value=_MOCK_PROFILE_ANALYSIS)
def test_session_retrievable(mock_llm):
    org = _csv_bytes(_sample_org_chart())

    resp = client.post(
        "/api/v1/ingest/startup",
        files={"org_chart_file": ("org_chart.csv", io.BytesIO(org), "text/csv")},
        data=ONBOARDING_FORM,
    )

    sid = resp.json()["session_id"]

    # Verify session data is stored via the quality endpoint (requires session)
    from app.core.session_store import session_store
    entry = session_store.get(sid)
    assert entry is not None
    assert entry.startup_profile["company_name"] == "TestCo SaaS"
    assert entry.org_chart_df is not None
    assert len(entry.org_chart_df) == 4
