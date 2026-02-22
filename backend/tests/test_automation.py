"""Tests for Module 4 — POST /api/v1/analyze/role-audit."""

from __future__ import annotations

import math

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.services.automation import _lookup_role, _vulnerability, _compute_rpe

client = TestClient(app)

# ---------------------------------------------------------------------------
# Sample org chart mirroring test_data/org_chart.csv
# ---------------------------------------------------------------------------

_SAMPLE_ORG = pd.DataFrame({
    "Employee_ID": ["EMP001","EMP002","EMP003","EMP004","EMP005",
                    "EMP006","EMP007","EMP008","EMP009","EMP010"],
    "Name": ["Rahul Sharma","Priya Menon","Amit Patel","Neha Gupta",
             "Vikram Singh","Sanjay Kumar","Anjali Desai","Rohan Das",
             "Kavita Reddy","Meera Nair"],
    "Job_Title": ["Founder & CEO","CTO","Senior Developer","Junior Developer",
                  "Sales Director","SDR (Sales Dev Rep)","SDR (Sales Dev Rep)",
                  "Customer Support Exec","Customer Support Exec","HR & Payroll Admin"],
    "Department": ["Executive","Engineering","Engineering","Engineering",
                   "Sales","Sales","Sales","Operations","Operations","Operations"],
    "Monthly_Salary_INR": [150000,140000,110000,60000,120000,
                           45000,45000,35000,35000,40000],
    "Hours_Per_Week": [60,50,40,40,45,45,45,40,40,40],
})

_SAMPLE_PROFILE = {
    "company_name": "SkillSphere India",
    "sub_type": "SaaS",
    "mrr_last_3_months": [1200000, 1350000, 1500000],
    "monthly_growth_goal_pct": 15,
    "patience_months": 3,
    "current_tech_stack": ["Razorpay", "Zoho CRM", "Google Workspace", "Slack", "Mailchimp"],
    "num_employees": 10,
    "industry": "B2B EdTech SaaS",
}


def _make_session(
    org_df: pd.DataFrame | None = None,
    profile: dict | None = None,
) -> str:
    return session_store.create(
        startup_profile=profile if profile is not None else _SAMPLE_PROFILE.copy(),
        org_chart_df=org_df if org_df is not None else _SAMPLE_ORG.copy(),
        data_issues=[],
    )


def _make_session_no_org() -> str:
    return session_store.create(
        startup_profile=_SAMPLE_PROFILE.copy(),
        data_issues=[],
    )


def _post_role_audit(session_id: str):
    return client.post("/api/v1/analyze/role-audit", json={"session_id": session_id})


# ---------------------------------------------------------------------------
# Happy-path integration tests
# ---------------------------------------------------------------------------

def test_role_audit_status_200():
    sid = _make_session()
    resp = _post_role_audit(sid)
    assert resp.status_code == 200


def test_role_audit_total_employees():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["total_employees"] == 10


def test_role_audit_roles_list_length():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert len(body["roles"]) == 10


# ---------------------------------------------------------------------------
# Metric 3: Role Automation Potential
# ---------------------------------------------------------------------------

def test_metric_3_sdr_high_vulnerability():
    """SDRs should be classified as 70% automatable → High vulnerability."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    sdr_roles = [r for r in body["roles"] if "sdr" in r["job_title"].lower()
                 or "sales dev" in r["job_title"].lower()]
    assert len(sdr_roles) == 2
    for r in sdr_roles:
        assert r["automation_pct"] == pytest.approx(70.0, abs=1)
        assert r["vulnerability_level"] == "High"


def test_metric_3_ceo_low_vulnerability():
    """Founder/CEO should be Low vulnerability (5%)."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    exec_roles = [r for r in body["roles"]
                  if "ceo" in r["job_title"].lower() or "founder" in r["job_title"].lower()]
    assert len(exec_roles) >= 1
    for r in exec_roles:
        assert r["automation_pct"] < 30
        assert r["vulnerability_level"] == "Low"


def test_metric_3_hr_payroll_high_vulnerability():
    """HR & Payroll Admin should be High vulnerability (≥75%)."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    hr_roles = [r for r in body["roles"]
                if "hr" in r["job_title"].lower() or "payroll" in r["job_title"].lower()]
    assert len(hr_roles) >= 1
    for r in hr_roles:
        assert r["automation_pct"] >= 60
        assert r["vulnerability_level"] == "High"


def test_metric_3_customer_support_high_vulnerability():
    """Customer Support Exec should be High (65%)."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    cs_roles = [r for r in body["roles"] if "support" in r["job_title"].lower()]
    assert len(cs_roles) == 2
    for r in cs_roles:
        assert r["automation_pct"] >= 60
        assert r["vulnerability_level"] == "High"


def test_metric_3_vulnerability_counts():
    """SkillSphere chart: CEO(L), CTO(L), Sr Dev(L), Jr Dev(M), Sales Dir(L),
    SDR×2(H), Cust Support×2(H), HR(H) → High=5, Medium=1, Low=4."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["high_vulnerability_count"] == 5
    assert body["medium_vulnerability_count"] == 1
    assert body["low_vulnerability_count"] == 4


def test_metric_3_avg_automation_pct_range():
    """Average automation % should be between 30 and 70 for a typical mixed org."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert 30 <= body["avg_automation_pct"] <= 70


def test_metric_3_hours_saved_positive():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["total_hours_saved_per_week"] > 0
    # Every role should have hours_saved = hours_per_week * automation_pct / 100
    for r in body["roles"]:
        expected = round(r["hours_per_week"] * r["automation_pct"] / 100, 1)
        assert r["hours_saved_per_week"] == pytest.approx(expected, abs=0.5)


def test_metric_3_top_automatable_role():
    """Top role should be HR/Payroll or one of the high % roles."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["top_automatable_pct"] >= 70
    assert len(body["top_automatable_role"]) > 0


def test_metric_3_automatable_tasks_non_empty():
    """Every role should list at least one automatable task."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    for r in body["roles"]:
        assert len(r["automatable_tasks"]) >= 1


def test_metric_3_upskilling_rec_non_empty():
    """Every role should carry an upskilling recommendation."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    for r in body["roles"]:
        assert len(r["upskilling_rec"]) > 5


# ---------------------------------------------------------------------------
# Metric 8: RPE Lift
# ---------------------------------------------------------------------------

def test_metric_8_rpe_lift_calculation():
    """RPE lift = (1.15^3 - 1)*100 ≈ 52.09% for 15% growth over 3 months."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    rpe = body["rpe_metrics"]

    current_mrr = 1_500_000.0
    headcount   = 10
    projected   = current_mrr * math.pow(1.15, 3)

    assert rpe["current_rpe_monthly"] == pytest.approx(current_mrr / headcount, abs=1)
    assert rpe["projected_mrr"] == pytest.approx(projected, rel=0.001)
    assert rpe["projected_rpe_monthly"] == pytest.approx(projected / headcount, rel=0.001)
    lift = ((projected / headcount) - (current_mrr / headcount)) / (current_mrr / headcount) * 100
    assert rpe["rpe_lift_pct"] == pytest.approx(lift, abs=0.2)


def test_metric_8_rpe_lift_positive():
    """With positive growth, RPE lift must be > 0."""
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["rpe_metrics"]["rpe_lift_pct"] > 0
    assert body["rpe_metrics"]["rpe_lift_inr"] > 0


def test_metric_8_headcount_matches_org_chart():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["rpe_metrics"]["headcount"] == 10


def test_metric_8_zero_growth_rate():
    """With 0% growth, projected MRR == current MRR → lift = 0."""
    profile = {**_SAMPLE_PROFILE, "monthly_growth_goal_pct": 0}
    sid = _make_session(profile=profile)
    body = _post_role_audit(sid).json()
    assert body["rpe_metrics"]["rpe_lift_pct"] == pytest.approx(0.0, abs=0.01)


def test_metric_8_missing_mrr_graceful():
    """Session without mrr_last_3_months should not crash — RPE set to 0."""
    profile = {k: v for k, v in _SAMPLE_PROFILE.items() if k != "mrr_last_3_months"}
    sid = _make_session(profile=profile)
    resp = _post_role_audit(sid)
    assert resp.status_code == 200
    body = resp.json()
    assert body["rpe_metrics"]["current_mrr"] == 0.0
    assert len(body["warnings"]) >= 1


# ---------------------------------------------------------------------------
# Output quality
# ---------------------------------------------------------------------------

def test_recommendations_non_empty():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert len(body["recommendations"]) >= 3


def test_mermaid_chart_non_empty():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert "flowchart" in body["mermaid_chart"]
    assert len(body["mermaid_chart"]) > 50


def test_automation_coverage_equals_avg_pct_over_100():
    sid = _make_session()
    body = _post_role_audit(sid).json()
    assert body["automation_coverage"] == pytest.approx(
        body["avg_automation_pct"] / 100.0, abs=0.001
    )


def test_result_stored_in_session():
    sid = _make_session()
    _post_role_audit(sid)
    entry = session_store.get(sid)
    assert entry is not None
    assert entry.automation_report is not None
    assert entry.automation_report.session_id == sid  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_session_not_found_404():
    resp = _post_role_audit("nonexistent_xyz_999")
    assert resp.status_code == 404


def test_no_org_chart_df_422():
    sid = _make_session_no_org()
    resp = _post_role_audit(sid)
    assert resp.status_code == 422
    assert "org chart" in resp.json()["detail"].lower()


def test_unknown_title_uses_fallback_40():
    """A title not in the DB should fall back to 40% automation."""
    single_row = pd.DataFrame({
        "Employee_ID": ["EMP001"],
        "Name": ["Test User"],
        "Job_Title": ["Galactic Unicorn Manager"],
        "Department": ["Imaginary"],
        "Monthly_Salary_INR": [50000],
        "Hours_Per_Week": [40],
    })
    sid = _make_session(org_df=single_row)
    body = _post_role_audit(sid).json()
    assert body["roles"][0]["automation_pct"] == pytest.approx(40.0, abs=1)


def test_single_employee_no_crash():
    """Report should work with just one employee."""
    single_row = pd.DataFrame({
        "Employee_ID": ["EMP001"],
        "Name": ["Rahul Sharma"],
        "Job_Title": ["SDR (Sales Dev Rep)"],
        "Department": ["Sales"],
        "Monthly_Salary_INR": [45000],
        "Hours_Per_Week": [45],
    })
    sid = _make_session(org_df=single_row)
    resp = _post_role_audit(sid)
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_employees"] == 1
    assert body["high_vulnerability_count"] == 1


# ---------------------------------------------------------------------------
# Unit tests: service helpers
# ---------------------------------------------------------------------------

def test_lookup_role_exact():
    """SDR key should match 'sdr' in title."""
    match = _lookup_role("SDR (Sales Dev Rep)")
    assert match["pct"] == 70


def test_lookup_role_senior_dev_specific():
    """'senior developer' is more specific than 'developer', should win."""
    match = _lookup_role("Senior Developer")
    assert match["pct"] == 28


def test_lookup_role_junior_dev_specific():
    match = _lookup_role("Junior Developer")
    assert match["pct"] == 40


def test_lookup_role_cto():
    match = _lookup_role("CTO")
    assert match["pct"] == 10


def test_lookup_role_fallback():
    match = _lookup_role("Supreme Overlord of Chaos")
    assert match["pct"] == 40  # default


def test_vulnerability_thresholds():
    assert _vulnerability(70) == "High"
    assert _vulnerability(60) == "High"
    assert _vulnerability(59) == "Medium"
    assert _vulnerability(30) == "Medium"
    assert _vulnerability(29) == "Low"
    assert _vulnerability(5)  == "Low"


def test_compute_rpe_math():
    profile = {"mrr_last_3_months": [1000000, 1200000, 1500000],
               "monthly_growth_goal_pct": 10, "patience_months": 6}
    rpe, warnings = _compute_rpe(profile, headcount=10)
    assert warnings == []
    expected_projected = 1_500_000 * math.pow(1.10, 6)
    assert rpe.projected_mrr == pytest.approx(expected_projected, rel=0.001)
    assert rpe.current_rpe_monthly == pytest.approx(150_000, abs=1)
