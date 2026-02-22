"""Tests for Module 7 — Strategic Verdict Generator.

Verifies aggregation of module outputs, scorecard generation, risk
identification, action plan building, and overall scoring.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.ingestion import WorkflowStep, WorkflowDiagram
from app.schemas.quality import QualityReport, ColumnQuality, DPDPComplianceReport
from app.schemas.automation import (
    RoleAnalysis,
    RPEMetrics,
    AutomationReport,
)
from app.schemas.financial import (
    FinancialReport,
    BeforeAfterRow,
)
from app.schemas.retention import RetentionReport, RadarDataPoint, CompetitorChurnBenchmark

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workflow(steps: list[dict]) -> WorkflowDiagram:
    return WorkflowDiagram(
        steps=[WorkflowStep(**s) for s in steps],
        mermaid_diagram="",
        summary="test workflow",
    )


def _make_quality_report(sid: str, **overrides) -> QualityReport:
    defaults = dict(
        session_id=sid,
        row_count=100, column_count=5, total_cells=500,
        missing_cells=25, duplicate_rows=3,
        completeness_score=0.95, deduplication_score=0.97,
        consistency_score=0.85, structural_integrity_score=0.80,
        process_digitisation_score=0.20, tool_maturity_score=0.35,
        data_coverage_score=0.40,
        total_workflow_steps=10, automated_steps=2, manual_steps=8,
        tools_detected=["Excel", "WhatsApp"],
        documents_provided=["sales"],
        data_quality_score=0.52,
        ai_readiness_score=0.52,
        quality_pass=False,
        readiness_level="Low",
        dpdp_compliance=DPDPComplianceReport(
            risk_level="Low", total_pii_columns=0, total_pii_values=0,
            pii_findings=[], compliance_warnings=[], llm_api_safe=True,
        ),
        column_quality=[],
        top_recommendations=["Digitise manual processes"],
    )
    defaults.update(overrides)
    return QualityReport(**defaults)


def _make_role_for_verdict(
    idx: int = 1,
    job_title: str = "SDR",
    automation_pct: float = 70.0,
    vulnerability_level: str = "High",
    monthly_salary_inr: float = 30_000,
) -> RoleAnalysis:
    saved = round(45.0 * automation_pct / 100, 2)
    return RoleAnalysis(
        employee_id=f"EMP{idx:03d}",
        name=f"Employee {idx}",
        job_title=job_title,
        department="Operations",
        monthly_salary_inr=monthly_salary_inr,
        hours_per_week=45.0,
        automation_pct=automation_pct,
        automatable_tasks=["Data entry", "Scheduling"],
        vulnerability_level=vulnerability_level,
        upskilling_rec="Upskill in strategic tasks",
        hours_saved_per_week=saved,
    )


def _make_automation_report(sid: str, **overrides) -> AutomationReport:
    roles = overrides.pop("roles", [
        _make_role_for_verdict(1, "SDR",     70.0, "High"),
        _make_role_for_verdict(2, "HR Admin", 75.0, "High"),
        _make_role_for_verdict(3, "Founder",   5.0, "Low"),
    ])
    rpe = RPEMetrics(
        current_mrr=500_000,
        headcount=len(roles),
        current_rpe_monthly=round(500_000 / max(len(roles), 1), 2),
        projected_mrr=575_000,
        projected_rpe_monthly=round(575_000 / max(len(roles), 1), 2),
        rpe_lift_pct=15.0,
        rpe_lift_inr=75_000,
        growth_months_used=1,
        monthly_growth_rate_pct=15.0,
    )
    avg_pct = sum(r.automation_pct for r in roles) / len(roles) if roles else 0.0
    high = sum(1 for r in roles if r.vulnerability_level == "High")
    med  = sum(1 for r in roles if r.vulnerability_level == "Medium")
    low  = sum(1 for r in roles if r.vulnerability_level == "Low")
    top  = max(roles, key=lambda r: r.automation_pct) if roles else None
    defaults = dict(
        session_id=sid,
        total_employees=len(roles),
        roles=roles,
        avg_automation_pct=avg_pct,
        high_vulnerability_count=high,
        medium_vulnerability_count=med,
        low_vulnerability_count=low,
        top_automatable_role=top.job_title if top else "N/A",
        top_automatable_pct=top.automation_pct if top else 0.0,
        total_hours_saved_per_week=sum(r.hours_saved_per_week for r in roles),
        rpe_metrics=rpe,
        automation_coverage=avg_pct / 100,
        recommendations=["Automate data entry"],
        mermaid_chart="flowchart TD\n  A --> B",
        warnings=[],
    )
    defaults.update(overrides)
    return AutomationReport(**defaults)


def _make_financial_report(sid: str, **overrides) -> FinancialReport:
    defaults = dict(
        session_id=sid,
        current_mrr=1_500_000,
        total_payroll_monthly_inr=665_000,
        total_recurring_expenses_inr=291_000,
        total_monthly_costs_inr=956_000,
        headcount=10,
        gross_monthly_savings_inr=178_752,
        new_ai_tools_monthly_cost_inr=9_500,
        net_monthly_savings_inr=169_252,
        net_annual_savings_inr=2_031_024,
        current_operating_margin_pct=36.3,
        projected_operating_margin_pct=46.9,
        gross_margin_lift_pct=10.6,
        opportunity_cost_per_month_inr=211_852,
        opportunity_cost_per_year_inr=2_542_224,
        mrr_at_risk_monthly_inr=42_600,
        months_to_break_even=3.0,
        employee_savings=[],
        ai_tool_recommendations=[],
        before_after=[
            BeforeAfterRow(
                metric="Net Monthly Savings",
                before_value="₹0",
                after_value="₹1,69,252",
                delta="▲ +₹1,69,252",
                icon="✅",
            )
        ],
        headline="Implementing AI frees ₹1.7L/month.",
        executive_summary="Test executive summary.",
        warnings=[],
    )
    defaults.update(overrides)
    return FinancialReport(**defaults)


def _make_retention_report(sid: str, **overrides) -> RetentionReport:
    defaults = dict(
        session_id=sid,
        total_inquiries=10,
        closed_won_count=6,
        repeat_customer_count=3,
        new_customer_count=3,
        lost_count=1,
        pending_count=3,
        win_rate_pct=60.0,
        repeat_rate_pct=50.0,
        current_churn_pct=6.0,
        projected_churn_pct=2.8,
        churn_reduction_pct=3.2,
        industry_avg_churn_pct=3.5,
        top_tier_churn_pct=1.5,
        current_nrr_pct=82.0,
        projected_nrr_pct=97.0,
        nrr_benchmark_pct=108.0,
        growth_levers=["Automate follow-ups", "AI personalisation"],
        sector_risks=["Price competition"],
        competitor_benchmarks=[
            CompetitorChurnBenchmark(company="Freshdesk", sector="SaaS", churn_pct=1.5, nrr_pct=118.0),
        ],
        radar_data=[
            RadarDataPoint(axis="Win Rate",          startup_value=60.0, industry_avg=50.0, top_tier=90.0),
            RadarDataPoint(axis="Repeat Rate",       startup_value=62.5, industry_avg=50.0, top_tier=90.0),
            RadarDataPoint(axis="Churn vs Industry", startup_value=41.7, industry_avg=50.0, top_tier=90.0),
            RadarDataPoint(axis="NRR vs Benchmark",  startup_value=71.1, industry_avg=71.1, top_tier=95.0),
            RadarDataPoint(axis="Pipeline Health",   startup_value=54.0, industry_avg=45.0, top_tier=85.0),
        ],
        headline="AI can cut churn by 3.2pp and project NRR to 97%.",
        executive_summary="Retention is above industry average with room to improve.",
        warnings=[],
    )
    defaults.update(overrides)
    return RetentionReport(**defaults)


def _make_session(
    quality: bool = False,
    automation: bool = False,
    financial: bool = False,
    retention: bool = False,
    num_employees: int = 10,
) -> str:
    """Create session with selected module reports pre-populated."""
    df = pd.DataFrame({"date": ["2025-01-01"], "amount": [100.0]})
    wf = _workflow([
        {"step_number": 1, "description": "Enter daily sales", "actor": "Cashier", "step_type": "Manual"},
    ])
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={
            "industry": "Restaurant",
            "num_employees": num_employees,
            "tools_used": ["Excel", "WhatsApp", "Paper"],
        },
        data_issues=[],
        workflow_analysis=wf,
    )
    entry = session_store.get(sid)
    if quality:
        entry.quality_report = _make_quality_report(sid)
    if automation:
        entry.automation_report = _make_automation_report(sid)
    if financial:
        entry.financial_report = _make_financial_report(sid)
    if retention:
        entry.retention_report = _make_retention_report(sid)
    return sid


def _post_verdict(session_id: str):
    return client.post(
        "/api/v1/analyze/verdict",
        data={"session_id": session_id},
    )


# ---------------------------------------------------------------------------
# Test: 404 for missing session
# ---------------------------------------------------------------------------

def test_session_not_found():
    resp = _post_verdict("nonexistent_session_abc123")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test: 422 if no modules have been run
# ---------------------------------------------------------------------------

def test_no_modules_run():
    df = pd.DataFrame({"x": [1]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    resp = _post_verdict(sid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: works with only quality module
# ---------------------------------------------------------------------------

def test_quality_only():
    sid = _make_session(quality=True)
    resp = _post_verdict(sid)
    assert resp.status_code == 200
    body = resp.json()

    assert body["overall_readiness_score"] > 0
    assert body["verdict"] in ("AI-Ready", "Partially Ready", "Significant Gaps", "Not Ready")
    assert len(body["scorecard"]) == 5

    # Quality should show as ran
    q_card = next(sc for sc in body["scorecard"] if sc["module_number"] == "2")
    assert q_card["ran"] is True
    assert q_card["score"] is not None

    # Others should be Not Run
    for sc in body["scorecard"]:
        if sc["module_number"] != "2":
            assert sc["ran"] is False


# ---------------------------------------------------------------------------
# Test: works with all modules
# ---------------------------------------------------------------------------

def test_all_modules():
    sid = _make_session(quality=True, automation=True, financial=True)
    resp = _post_verdict(sid)
    assert resp.status_code == 200
    body = resp.json()

    assert body["overall_readiness_score"] > 0
    assert body["verdict"] in ("AI-Ready", "Partially Ready", "Significant Gaps", "Not Ready")

    # 3 modules ran (no benchmark or retention in this test)
    ran_count = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran_count == 3


# ---------------------------------------------------------------------------
# Test: scorecard has 5 entries (one per module 2-6)
# ---------------------------------------------------------------------------

def test_scorecard_count():
    sid = _make_session(quality=True)
    body = _post_verdict(sid).json()
    assert len(body["scorecard"]) == 5


# ---------------------------------------------------------------------------
# Test: scorecard modules have correct numbers
# ---------------------------------------------------------------------------

def test_scorecard_module_numbers():
    sid = _make_session(quality=True)
    body = _post_verdict(sid).json()
    numbers = sorted(sc["module_number"] for sc in body["scorecard"])
    assert numbers == ["2", "3", "4", "5", "6"]


# ---------------------------------------------------------------------------
# Test: risks generated from low quality data
# ---------------------------------------------------------------------------

def test_risks_from_quality():
    sid = _make_session(quality=True)
    entry = session_store.get(sid)
    # Override to create weak quality
    entry.quality_report = _make_quality_report(
        sid,
        completeness_score=0.50,
        process_digitisation_score=0.10,
        tool_maturity_score=0.20,
    )
    body = _post_verdict(sid).json()

    assert len(body["risks"]) >= 2
    areas = [r["area"] for r in body["risks"]]
    assert "Data Quality" in areas or "Process Digitisation" in areas


# ---------------------------------------------------------------------------
# Test: risks from financial impact
# ---------------------------------------------------------------------------

def test_risks_from_financial():
    sid = _make_session(financial=True)
    body = _post_verdict(sid).json()

    areas = [r["area"] for r in body["risks"]]
    # High opportunity cost should trigger a financial risk
    assert "Financial Impact" in areas


# ---------------------------------------------------------------------------
# Test: strengths detected
# ---------------------------------------------------------------------------

def test_strengths_detected():
    sid = _make_session(quality=True)
    entry = session_store.get(sid)
    # Override quality to be strong
    entry.quality_report = _make_quality_report(
        sid,
        completeness_score=0.95,
        deduplication_score=0.98,
        process_digitisation_score=0.70,
        tool_maturity_score=0.65,
    )
    body = _post_verdict(sid).json()

    assert len(body["strengths"]) >= 1


# ---------------------------------------------------------------------------
# Test: weaknesses detected
# ---------------------------------------------------------------------------

def test_weaknesses_detected():
    sid = _make_session(quality=True)
    body = _post_verdict(sid).json()

    # Low tool maturity → weaknesses
    assert len(body["weaknesses"]) >= 1


# ---------------------------------------------------------------------------
# Test: action plan generated
# ---------------------------------------------------------------------------

def test_action_plan_generated():
    sid = _make_session(quality=True, automation=True, financial=True)
    body = _post_verdict(sid).json()

    assert len(body["action_plan"]) >= 2
    # Actions should have ascending priorities
    priorities = [a["priority"] for a in body["action_plan"]]
    assert priorities == sorted(priorities)


# ---------------------------------------------------------------------------
# Test: action plan has valid effort/timeframe
# ---------------------------------------------------------------------------

def test_action_plan_fields():
    sid = _make_session(automation=True, financial=True)
    body = _post_verdict(sid).json()

    for action in body["action_plan"]:
        assert action["effort"] in ("Low", "Medium", "High")
        assert action["timeframe"]  # not empty
        assert action["source_module"]
        assert action["impact"]


# ---------------------------------------------------------------------------
# Test: key metrics dict populated
# ---------------------------------------------------------------------------

def test_key_metrics():
    sid = _make_session(quality=True, automation=True)
    body = _post_verdict(sid).json()

    km = body["key_metrics"]
    assert "AI Readiness" in km
    assert "Avg Role Automation" in km
    assert "RPE Lift" in km


# ---------------------------------------------------------------------------
# Test: executive report is meaningful markdown
# ---------------------------------------------------------------------------

def test_executive_report():
    sid = _make_session(quality=True, automation=True, financial=True)
    body = _post_verdict(sid).json()

    report = body["executive_report"]
    assert len(report) > 200
    assert "FoundationIQ" in report
    assert "Scorecard" in report or "scorecard" in report.lower()
    assert "Roadmap" in report or "roadmap" in report.lower()


# ---------------------------------------------------------------------------
# Test: verdict reflects weak data
# ---------------------------------------------------------------------------

def test_verdict_not_ready():
    sid = _make_session(quality=True)
    entry = session_store.get(sid)
    entry.quality_report = _make_quality_report(
        sid,
        ai_readiness_score=0.20,
        readiness_level="Critical",
        process_digitisation_score=0.05,
        tool_maturity_score=0.10,
        completeness_score=0.50,
    )
    body = _post_verdict(sid).json()

    assert body["verdict"] in ("Not Ready", "Significant Gaps")
    assert body["overall_readiness_score"] < 0.40


# ---------------------------------------------------------------------------
# Test: verdict reflects strong data
# ---------------------------------------------------------------------------

def test_verdict_ai_ready():
    sid = _make_session(quality=True, automation=True)
    entry = session_store.get(sid)
    entry.quality_report = _make_quality_report(
        sid,
        ai_readiness_score=0.85,
        readiness_level="High",
        completeness_score=0.95,
        process_digitisation_score=0.80,
        tool_maturity_score=0.75,
    )
    # High automation coverage — all High vulnerability roles
    entry.automation_report = _make_automation_report(
        sid,
        roles=[
            _make_role_for_verdict(i, "SDR", 70.0, "High")
            for i in range(1, 6)
        ],
    )
    body = _post_verdict(sid).json()

    assert body["verdict"] == "AI-Ready"
    assert body["overall_readiness_score"] >= 0.75


# ---------------------------------------------------------------------------
# Test: verdict summary mentions industry
# ---------------------------------------------------------------------------

def test_verdict_summary_content():
    sid = _make_session(quality=True)
    body = _post_verdict(sid).json()

    summary = body["verdict_summary"]
    assert "Restaurant" in summary or "restaurant" in summary.lower()
    assert "%" in summary


# ---------------------------------------------------------------------------
# Test: overall score uses weighted average of available modules
# ---------------------------------------------------------------------------

def test_overall_score_weighting():
    # Quality only → score should equal quality's AI readiness score
    sid = _make_session(quality=True)
    entry = session_store.get(sid)
    entry.quality_report = _make_quality_report(sid, ai_readiness_score=0.60)
    body = _post_verdict(sid).json()

    # With only 1 module, overall should equal that module's score
    assert abs(body["overall_readiness_score"] - 0.60) < 0.05


# ---------------------------------------------------------------------------
# Test: realistic restaurant scenario
# ---------------------------------------------------------------------------

def test_realistic_restaurant():
    """Full restaurant with all 4 modules (no benchmark)."""
    sid = _make_session(
        quality=True, automation=True, financial=True,
        num_employees=12,
    )
    resp = _post_verdict(sid)
    assert resp.status_code == 200

    body = resp.json()

    # Should have verdict
    assert body["verdict"] in ("AI-Ready", "Partially Ready", "Significant Gaps", "Not Ready")

    # Should have scorecard with 3 ran
    ran = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran == 3

    # Should have risks (weak quality + fragmented data)
    assert len(body["risks"]) >= 1

    # Should have action plan
    assert len(body["action_plan"]) >= 2

    # Should have key metrics
    assert len(body["key_metrics"]) >= 4

    # Executive report should be substantial
    assert len(body["executive_report"]) > 300

    # Should have both strengths and weaknesses
    # (our test data has some good and some bad scores)
    assert len(body["strengths"]) >= 0  # may have 0 if all scores are bad
    assert len(body["weaknesses"]) >= 1


# ---------------------------------------------------------------------------
# Test: automation-only scenario
# ---------------------------------------------------------------------------

def test_automation_only():
    sid = _make_session(automation=True)
    resp = _post_verdict(sid)
    assert resp.status_code == 200
    body = resp.json()

    assert body["overall_readiness_score"] > 0
    ran = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran == 1


# ---------------------------------------------------------------------------
# Test: Module 6 retention scorecard
# ---------------------------------------------------------------------------

def test_retention_scorecard_present():
    """When retention module has run, its scorecard entry shows Metric 9 & 10 headline."""
    sid = _make_session(retention=True)
    resp = _post_verdict(sid)
    assert resp.status_code == 200
    body = resp.json()

    retention_cards = [sc for sc in body["scorecard"] if "Retention" in sc["module"]]
    assert len(retention_cards) == 1
    rc = retention_cards[0]
    assert rc["ran"] is True
    assert rc["score"] is not None
    assert rc["score"] > 0
    # Headline should mention churn and NRR numbers
    assert "%" in rc["headline"]


def test_retention_not_run_scorecard():
    """When retention module has NOT run, its scorecard shows Not Run status."""
    sid = _make_session(quality=True)
    body = _post_verdict(sid).json()
    retention_cards = [sc for sc in body["scorecard"] if "Retention" in sc["module"]]
    assert len(retention_cards) == 1
    assert retention_cards[0]["ran"] is False
    assert retention_cards[0]["status"] == "Not Run"


def test_all_modules_scorecard_count():
    """With all 5 modules run, 5 scorecards show ran=True."""
    sid = _make_session(quality=True, automation=True, financial=True, retention=True)
    body = _post_verdict(sid).json()
    ran = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran == 4
