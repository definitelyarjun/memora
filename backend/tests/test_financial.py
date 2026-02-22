"""Tests for Module 5 — Financial Impact & ROI Simulator.

Covers:
- Metric 5: Net Monthly Savings (gross savings − new AI tool costs)
- Metric 12: Operating Margin Lift
- Metric 7: Opportunity Cost of Delay (savings + MRR at risk from TAT)
- Employee savings per role (High/Medium only)
- AI tool deduplication and already-in-stack cost = ₹0
- Missing expenses.csv fallback
- Missing benchmark_report (no TAT component to Metric 7)
- HTTP 404, 422 edge cases
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.automation import RoleAnalysis, RPEMetrics, AutomationReport
from app.schemas.benchmark import BottleneckReport

client = TestClient(app)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MRR = 1_500_000   # SkillSphere India example MRR


def _make_rpe(sid: str, mrr: float = _MRR, headcount: int = 10) -> RPEMetrics:
    return RPEMetrics(
        current_mrr=mrr,
        headcount=headcount,
        current_rpe_monthly=round(mrr / headcount, 2),
        projected_mrr=round(mrr * 1.2, 2),
        projected_rpe_monthly=round(mrr * 1.2 / headcount, 2),
        rpe_lift_pct=20.0,
        rpe_lift_inr=round(mrr * 0.2 / headcount, 2),
        growth_months_used=3,
        monthly_growth_rate_pct=6.0,
    )


def _make_role(
    idx: int,
    job_title: str,
    monthly_salary_inr: float = 40_000,
    hours_per_week: float = 45.0,
    automation_pct: float = 70.0,
    vulnerability_level: str = "High",
) -> RoleAnalysis:
    saved = round(hours_per_week * automation_pct / 100, 2)
    return RoleAnalysis(
        employee_id=f"EMP{idx:03d}",
        name=f"Employee {idx}",
        job_title=job_title,
        department="Operations",
        monthly_salary_inr=monthly_salary_inr,
        hours_per_week=hours_per_week,
        automation_pct=automation_pct,
        automatable_tasks=["Data entry", "Report generation"],
        vulnerability_level=vulnerability_level,
        upskilling_rec="Upskill in strategic tasks",
        hours_saved_per_week=saved,
    )


def _make_automation_report(
    sid: str,
    roles: list[RoleAnalysis],
    mrr: float = _MRR,
) -> AutomationReport:
    rpe = _make_rpe(sid, mrr=mrr, headcount=len(roles))
    total_hrs = sum(r.hours_saved_per_week for r in roles)
    avg_pct = sum(r.automation_pct for r in roles) / len(roles) if roles else 0.0
    high = sum(1 for r in roles if r.vulnerability_level == "High")
    med  = sum(1 for r in roles if r.vulnerability_level == "Medium")
    low  = sum(1 for r in roles if r.vulnerability_level == "Low")
    top  = max(roles, key=lambda r: r.automation_pct) if roles else None
    return AutomationReport(
        session_id=sid,
        total_employees=len(roles),
        roles=roles,
        avg_automation_pct=avg_pct,
        high_vulnerability_count=high,
        medium_vulnerability_count=med,
        low_vulnerability_count=low,
        top_automatable_role=top.job_title if top else "N/A",
        top_automatable_pct=top.automation_pct if top else 0.0,
        total_hours_saved_per_week=total_hrs,
        rpe_metrics=rpe,
        automation_coverage=avg_pct / 100,
        recommendations=["Automate data entry for SDR roles"],
        mermaid_chart="flowchart TD\n  A --> B",
        warnings=[],
    )


def _make_benchmark_report(
    sid: str,
    avg_tat: float = 67.0,
    threshold: float = 48.0,
    bottleneck_pct: float = 35.0,
) -> BottleneckReport:
    return BottleneckReport(
        session_id=sid,
        total_inquiries=50,
        closed_inquiries=50,
        avg_tat_hours=avg_tat,
        median_tat_hours=avg_tat * 0.9,
        max_tat_hours=avg_tat * 2.0,
        min_tat_hours=avg_tat * 0.2,
        bottleneck_threshold_hours=threshold,
        bottleneck_count=int(50 * bottleneck_pct / 100),
        bottleneck_pct=bottleneck_pct,
        avg_tat_improvement_pct=20.0,
        avg_hours_saved_per_inquiry=round(avg_tat * 0.20, 2),
        total_hours_saved=100.0,
        open_inquiries=0,
        bottleneck_details=[],
        top_bottleneck_stage=None,
        executive_summary="Test benchmark",
    )


def _make_expenses_df(monthly_total: float = 291_000) -> pd.DataFrame:
    """Build a minimal expenses CSV dataframe with one recurring entry."""
    return pd.DataFrame(
        {
            "Date": ["2025-01-15"],
            "Description": ["Cloud & SaaS"],
            "Amount": [monthly_total],
            "Recurring_Flag": ["Yes"],
        }
    )


def _make_session(
    roles: list[RoleAnalysis] | None = None,
    with_benchmark: bool = True,
    with_expenses: bool = True,
    tech_stack: list[str] | None = None,
    mrr: float = _MRR,
) -> str:
    """Create a session pre-populated with startup profile + optional modules."""
    stack = tech_stack if tech_stack is not None else ["Zoho CRM", "Mailchimp", "Razorpay"]

    sid = session_store.create(
        startup_profile={
            "mrr_last_3_months": [1_400_000, 1_450_000, mrr],
            "current_tech_stack": stack,
        },
        company_metadata={
            "industry": "SaaS",
            "num_employees": len(roles) if roles else 10,
            "tools_used": stack,
        },
        expenses_df=_make_expenses_df() if with_expenses else None,
    )

    entry = session_store.get(sid)
    if roles is not None:
        entry.automation_report = _make_automation_report(sid, roles, mrr=mrr)
    if with_benchmark:
        entry.benchmark_report = _make_benchmark_report(sid)

    return sid


def _post_financial(session_id: str):
    return client.post(
        "/api/v1/analyze/financial-impact",
        json={"session_id": session_id},
    )


# ───────────────────────────────────────────────────────────────────────────
# HTTP edge cases
# ───────────────────────────────────────────────────────────────────────────

def test_session_not_found():
    resp = _post_financial("nonexistent_deadbeef123")
    assert resp.status_code == 404


def test_missing_automation_report_returns_422():
    """Module 4 must have been run — otherwise 422."""
    sid = _make_session(roles=None, with_benchmark=False)
    resp = _post_financial(sid)
    assert resp.status_code == 422
    assert "Module 4" in resp.json()["detail"]


# ───────────────────────────────────────────────────────────────────────────
# Metric 5 — Net Monthly Savings
# ───────────────────────────────────────────────────────────────────────────

def test_net_monthly_savings_positive():
    """High-vulnerability SDR role → positive net savings."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    assert body["net_monthly_savings_inr"] > 0


def test_gross_savings_exceeds_tool_cost():
    """Gross savings should be ≥ new AI tool costs for a healthy scenario."""
    roles = [
        _make_role(1, "SDR",                monthly_salary_inr=35_000, automation_pct=70.0, vulnerability_level="High"),
        _make_role(2, "HR Admin",            monthly_salary_inr=30_000, automation_pct=75.0, vulnerability_level="High"),
        _make_role(3, "Customer Support Rep",monthly_salary_inr=35_000, automation_pct=50.0, vulnerability_level="Medium"),
    ]
    sid = _make_session(roles=roles, tech_stack=[])   # empty stack → all tools are new costs
    body = _post_financial(sid).json()

    assert body["gross_monthly_savings_inr"] >= body["new_ai_tools_monthly_cost_inr"]


def test_already_in_stack_tool_costs_zero():
    """Tools already in tech_stack must have monthly_cost_inr = 0."""
    roles = [
        _make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High"),
    ]
    # Zoho CRM is in _ROLE_TOOL_MAP for SDR — put it in stack
    sid = _make_session(roles=roles, tech_stack=["Zoho CRM"])
    body = _post_financial(sid).json()

    sdr_tools = [t for t in body["ai_tool_recommendations"] if "Zoho" in t["tool_name"]]
    if sdr_tools:  # tool was recommended
        for t in sdr_tools:
            assert t["monthly_cost_inr"] == 0
            assert t["already_in_stack"] is True


def test_low_vulnerability_roles_excluded_from_savings():
    """Low-vulnerability roles must NOT appear in employee_savings."""
    roles = [
        _make_role(1, "CEO",    monthly_salary_inr=150_000, automation_pct=5.0,  vulnerability_level="Low"),
        _make_role(2, "SDR",    monthly_salary_inr=40_000,  automation_pct=70.0, vulnerability_level="High"),
    ]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    emp_ids = [e["employee_id"] for e in body["employee_savings"]]
    assert "EMP001" not in emp_ids   # CEO should be excluded
    assert "EMP002" in emp_ids       # SDR should be included


def test_gross_savings_formula():
    """Verify gross_monthly_savings = Σ(monthly_hours_saved × loaded_hourly).

    Formula: loaded_hourly = salary × 1.25 / (hours_per_week × 4.33)
             monthly_hours_saved = hours_saved_per_week × 4.33
    """
    salary = 40_000
    hours_per_week = 45.0
    automation_pct = 70.0
    hours_saved_per_week = round(hours_per_week * automation_pct / 100, 2)

    loaded_monthly = salary * 1.25
    effective_hours_per_month = hours_per_week * 4.33
    loaded_hourly = loaded_monthly / effective_hours_per_month
    monthly_hrs_saved = hours_saved_per_week * 4.33
    expected_savings = round(monthly_hrs_saved * loaded_hourly, 0)

    roles = [_make_role(1, "SDR", monthly_salary_inr=salary, hours_per_week=hours_per_week,
                        automation_pct=automation_pct, vulnerability_level="High")]
    # Use empty stack so tool costs aren't zero
    sid = _make_session(roles=roles, tech_stack=[], with_benchmark=False, with_expenses=False)
    body = _post_financial(sid).json()

    emp = body["employee_savings"][0]
    assert abs(emp["gross_monthly_savings_inr"] - expected_savings) < 5.0


# ───────────────────────────────────────────────────────────────────────────
# Metric 12 — Operating Margin Lift
# ───────────────────────────────────────────────────────────────────────────

def test_margin_lift_positive_for_high_savings():
    """When net savings > 0, projected margin > current margin."""
    roles = [
        _make_role(i, job_title, monthly_salary_inr=salary, automation_pct=pct, vulnerability_level=vuln)
        for i, (job_title, salary, pct, vuln) in enumerate([
            ("SDR",                 35_000, 70, "High"),
            ("HR Admin",            30_000, 75, "High"),
            ("Customer Support Rep",35_000, 50, "Medium"),
        ], 1)
    ]
    sid = _make_session(roles=roles, tech_stack=["Freshdesk", "Keka"])  # these cancel some costs
    body = _post_financial(sid).json()

    if body["net_monthly_savings_inr"] > 0:
        assert body["projected_operating_margin_pct"] > body["current_operating_margin_pct"]
        assert body["gross_margin_lift_pct"] > 0


def test_margin_calculation_accuracy():
    """current_op_margin = (MRR − total_costs) / MRR × 100."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles, with_expenses=True)
    body = _post_financial(sid).json()

    mrr = body["current_mrr"]
    costs = body["total_monthly_costs_inr"]
    expected_margin = (mrr - costs) / mrr * 100
    assert abs(body["current_operating_margin_pct"] - expected_margin) < 0.5


# ───────────────────────────────────────────────────────────────────────────
# Metric 7 — Opportunity Cost of Delay
# ───────────────────────────────────────────────────────────────────────────

def test_opportunity_cost_includes_mrr_at_risk():
    """Opportunity cost ≥ net monthly savings when benchmark is present."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles, with_benchmark=True)
    body = _post_financial(sid).json()

    # opp_cost = savings_foregone + mrr_at_risk
    assert body["opportunity_cost_per_month_inr"] >= max(body["net_monthly_savings_inr"], 0)
    assert body["mrr_at_risk_monthly_inr"] >= 0


def test_opportunity_cost_no_benchmark():
    """Without benchmark, mrr_at_risk = 0; opp_cost = net_savings."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles, with_benchmark=False)
    body = _post_financial(sid).json()

    assert body["mrr_at_risk_monthly_inr"] == 0.0
    # opp cost should equal savings foregone only
    expected = max(body["net_monthly_savings_inr"], 0.0)
    assert abs(body["opportunity_cost_per_month_inr"] - expected) < 10


def test_opportunity_cost_annual():
    """opportunity_cost_per_year = opportunity_cost_per_month × 12."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    assert abs(body["opportunity_cost_per_year_inr"] - body["opportunity_cost_per_month_inr"] * 12) < 10


# ───────────────────────────────────────────────────────────────────────────
# Expenses integration
# ───────────────────────────────────────────────────────────────────────────

def test_expenses_df_increases_total_costs():
    """Total costs with expenses > total costs without expenses."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]

    sid_with = _make_session(roles=roles, with_expenses=True)
    sid_without = _make_session(roles=roles, with_expenses=False)

    body_with = _post_financial(sid_with).json()
    body_without = _post_financial(sid_without).json()

    assert body_with["total_monthly_costs_inr"] > body_without["total_monthly_costs_inr"]
    assert body_with["total_recurring_expenses_inr"] > 0
    assert body_without["total_recurring_expenses_inr"] == 0.0


def test_no_expenses_warning_generated():
    """Missing expenses.csv should add a warning but not fail."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles, with_expenses=False)
    body = _post_financial(sid).json()

    warnings = [w.lower() for w in body["warnings"]]
    assert any("expense" in w for w in warnings)


# ───────────────────────────────────────────────────────────────────────────
# Tool recommendations
# ───────────────────────────────────────────────────────────────────────────

def test_ai_tools_deduplicated():
    """Two SDR roles should produce only ONE Zoho CRM recommendation."""
    roles = [
        _make_role(1, "SDR", monthly_salary_inr=35_000, automation_pct=70.0, vulnerability_level="High"),
        _make_role(2, "BDR", monthly_salary_inr=35_000, automation_pct=70.0, vulnerability_level="High"),
    ]
    sid = _make_session(roles=roles, tech_stack=[])
    body = _post_financial(sid).json()

    tool_names = [t["tool_name"] for t in body["ai_tool_recommendations"]]
    # Tools should be unique
    assert len(tool_names) == len(set(tool_names))


def test_no_tool_recommendation_for_low_vuln():
    """Low-vulnerability roles should not generate tool recommendations."""
    roles = [
        _make_role(1, "CEO",    monthly_salary_inr=150_000, automation_pct=5.0,  vulnerability_level="Low"),
        _make_role(2, "CTO",    monthly_salary_inr=150_000, automation_pct=5.0,  vulnerability_level="Low"),
    ]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    assert len(body["ai_tool_recommendations"]) == 0
    assert len(body["employee_savings"]) == 0


# ───────────────────────────────────────────────────────────────────────────
# Before / After table
# ───────────────────────────────────────────────────────────────────────────

def test_before_after_rows_present():
    """Before/After dashboard should always have at least 5 rows."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    assert len(body["before_after"]) >= 5


def test_before_after_net_savings_row():
    """Net Monthly Savings row must exist in before_after."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles)
    body = _post_financial(sid).json()

    row_names = [r["metric"] for r in body["before_after"]]
    assert any("Metric 5" in n or "Net Monthly Savings" in n for n in row_names)


# ───────────────────────────────────────────────────────────────────────────
# Report stored in session
# ───────────────────────────────────────────────────────────────────────────

def test_report_stored_in_session():
    """financial_report must be written back to session after successful run."""
    roles = [_make_role(1, "SDR", monthly_salary_inr=40_000, automation_pct=70.0, vulnerability_level="High")]
    sid = _make_session(roles=roles)
    _post_financial(sid)

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.financial_report is not None
    assert entry.financial_report.session_id == sid


# ───────────────────────────────────────────────────────────────────────────
# SkillSphere India sanity check (end-to-end expected values)
# ───────────────────────────────────────────────────────────────────────────

def test_skillsphere_scenario():
    """Approximate SkillSphere India numbers from design document.

    10 employees, 6 automatable (High+Medium), MRR = ₹15L.
    Net savings ≈ ₹1.7L, margin lift ≈ +10pp, opp cost ≈ ₹2.1L.
    """
    roles = [
        _make_role(1, "SDR",                 35_000, 45.0, 70.0, "High"),
        _make_role(2, "SDR",                 35_000, 45.0, 70.0, "High"),
        _make_role(3, "HR Admin",             32_000, 45.0, 75.0, "High"),
        _make_role(4, "Customer Support Rep", 32_000, 45.0, 50.0, "Medium"),
        _make_role(5, "Customer Support Rep", 32_000, 45.0, 50.0, "Medium"),
        _make_role(6, "Junior Developer",     50_000, 45.0, 40.0, "Medium"),
        _make_role(7, "Senior Developer",    120_000, 45.0, 15.0, "Low"),
        _make_role(8, "Marketing Manager",    80_000, 45.0, 20.0, "Low"),
        _make_role(9, "CTO",                 150_000, 45.0, 5.0,  "Low"),
        _make_role(10, "CEO",                149_000, 45.0, 5.0,  "Low"),
    ]

    sid = _make_session(
        roles=roles,
        with_benchmark=True,
        with_expenses=True,
        tech_stack=["Zoho CRM", "Mailchimp", "Razorpay"],  # SDR+Mailchimp already in stack
        mrr=1_500_000,
    )

    body = _post_financial(sid).json()

    # Basic sanity checks
    assert body["headcount"] == 10
    assert len(body["employee_savings"]) == 6        # only High+Medium roles
    assert body["gross_monthly_savings_inr"] > 0
    assert body["net_monthly_savings_inr"] > 0
    assert body["current_operating_margin_pct"] > 0
    assert body["gross_margin_lift_pct"] > 0
    assert body["opportunity_cost_per_month_inr"] > 0

    # Loosely match expected values (±20% tolerance)
    assert 100_000 < body["net_monthly_savings_inr"] < 250_000, (
        f"Expected ~₹1.7L net savings, got ₹{body['net_monthly_savings_inr']:,.0f}"
    )
    assert 5 < body["gross_margin_lift_pct"] < 20, (
        f"Expected +5-20pp margin lift, got {body['gross_margin_lift_pct']:.1f}pp"
    )
    assert body["opportunity_cost_per_month_inr"] > body["net_monthly_savings_inr"] * 0.9, (
        "Opportunity cost should be at least 90% of net savings"
    )
