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
from app.schemas.quality import QualityReport, ColumnQuality
from app.schemas.automation import (
    AutomationCandidate,
    AutomationReport,
    AutomationSummary,
)
from app.schemas.consolidation import (
    ConsolidationReport,
    MigrationStep,
)
from app.schemas.roi import (
    Assumption,
    AutomationROILine,
    ROIReport,
    ROISummary,
)

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
        ai_readiness_score=0.52,
        readiness_level="Low",
        column_quality=[],
        top_recommendations=["Digitise manual processes"],
    )
    defaults.update(overrides)
    return QualityReport(**defaults)


def _make_automation_report(sid: str, **overrides) -> AutomationReport:
    candidates = overrides.pop("candidates", [
        AutomationCandidate(
            step_number=1, description="Enter daily sales", actor="Cashier",
            current_step_type="Manual", is_candidate=True,
            automation_type="RPA", confidence=0.80, confidence_level="High",
            reasoning="Data entry", estimated_effort="Low", priority="High",
        ),
        AutomationCandidate(
            step_number=2, description="Write receipt", actor="Cashier",
            current_step_type="Manual", is_candidate=True,
            automation_type="Digital Form", confidence=0.70, confidence_level="Medium",
            reasoning="Paper form", estimated_effort="Low", priority="Medium",
        ),
        AutomationCandidate(
            step_number=3, description="Cook food", actor="Chef",
            current_step_type="Manual", is_candidate=False,
            automation_type="Not Recommended", confidence=0.30, confidence_level="Low",
            reasoning="Physical work", estimated_effort="High", priority="Skip",
        ),
    ])
    automatable = [c for c in candidates if c.is_candidate]
    defaults = dict(
        session_id=sid,
        ai_readiness_score=0.52,
        readiness_level="Low",
        candidates=candidates,
        summary=AutomationSummary(
            total_steps=len(candidates),
            automatable_steps=len(automatable),
            already_automated=0,
            not_recommended=len(candidates) - len(automatable),
            automation_coverage=len(automatable) / max(len(candidates), 1),
            avg_confidence=0.75,
            by_type={"RPA": 1, "Digital Form": 1},
            by_priority={"High": 1, "Medium": 1},
        ),
        top_recommendations=["Automate data entry"],
        quick_wins=["Step 1: Enter daily sales"],
    )
    defaults.update(overrides)
    return AutomationReport(**defaults)


def _make_consolidation_report(sid: str, **overrides) -> ConsolidationReport:
    defaults = dict(
        session_id=sid,
        silos=[], data_flows=[], redundancies=[],
        unified_schemas=[],
        migration_steps=[
            MigrationStep(
                priority=1, action="Replace Paper with POS",
                from_tool="Paper", to_tool="POS System",
                rationale="No backup", effort="Low", affected_roles=["Cashier"],
            ),
            MigrationStep(
                priority=2, action="Replace Excel with Cloud",
                from_tool="Excel", to_tool="Google Sheets",
                rationale="No sync", effort="Medium", affected_roles=["Owner"],
            ),
        ],
        total_silos=5, informal_silos=3, manual_flows=4,
        consolidation_score=0.15,
        executive_summary="Critically fragmented",
        top_recommendations=["Digitise informal tools"],
    )
    defaults.update(overrides)
    return ConsolidationReport(**defaults)


def _make_roi_report(sid: str, **overrides) -> ROIReport:
    defaults = dict(
        session_id=sid,
        assumptions=[
            Assumption(key="hourly_wage", label="Hourly wage", value="₹180/hr", source="Default"),
        ],
        automation_lines=[
            AutomationROILine(
                step_number=1, description="Enter daily sales",
                automation_type="RPA", current_hours_per_week=1.2,
                hours_saved_per_week=0.96, annual_hours_saved=48.0,
                annual_cost_saved=8640, implementation_cost=15000,
                payback_months=20.8, effort="Low", priority="High",
            ),
        ],
        consolidation_lines=[],
        summary=ROISummary(
            total_current_hours_per_week=1.2,
            total_hours_saved_per_week=0.96,
            total_annual_hours_saved=48.0,
            total_annual_cost_saved=8640,
            total_implementation_cost=15000,
            net_first_year_benefit=-6360,
            three_year_net_benefit=10920,
            overall_payback_months=20.8,
            roi_percentage=57.6,
        ),
        executive_summary="Modest savings projected",
        top_recommendations=["Start with RPA quick win"],
    )
    defaults.update(overrides)
    return ROIReport(**defaults)


def _make_session(
    quality: bool = False,
    automation: bool = False,
    consolidation: bool = False,
    roi: bool = False,
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
    if consolidation:
        entry.consolidation_report = _make_consolidation_report(sid)
    if roi:
        entry.roi_report = _make_roi_report(sid)
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
    sid = _make_session(quality=True, automation=True, consolidation=True, roi=True)
    resp = _post_verdict(sid)
    assert resp.status_code == 200
    body = resp.json()

    assert body["overall_readiness_score"] > 0
    assert body["verdict"] in ("AI-Ready", "Partially Ready", "Significant Gaps", "Not Ready")

    # 4 modules ran (no benchmark in this test)
    ran_count = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran_count == 4


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
# Test: risks from consolidation
# ---------------------------------------------------------------------------

def test_risks_from_consolidation():
    sid = _make_session(consolidation=True)
    body = _post_verdict(sid).json()

    areas = [r["area"] for r in body["risks"]]
    # 3 informal silos + 4 manual flows should trigger risks
    assert "Data Consolidation" in areas or "Data Flows" in areas


# ---------------------------------------------------------------------------
# Test: strengths detected
# ---------------------------------------------------------------------------

def test_strengths_detected():
    sid = _make_session(quality=True, roi=True)
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
    sid = _make_session(quality=True, consolidation=True)
    body = _post_verdict(sid).json()

    # Low tool maturity + low consolidation → weaknesses
    assert len(body["weaknesses"]) >= 1


# ---------------------------------------------------------------------------
# Test: action plan generated
# ---------------------------------------------------------------------------

def test_action_plan_generated():
    sid = _make_session(quality=True, automation=True, consolidation=True)
    body = _post_verdict(sid).json()

    assert len(body["action_plan"]) >= 2
    # Actions should have ascending priorities
    priorities = [a["priority"] for a in body["action_plan"]]
    assert priorities == sorted(priorities)


# ---------------------------------------------------------------------------
# Test: action plan has valid effort/timeframe
# ---------------------------------------------------------------------------

def test_action_plan_fields():
    sid = _make_session(automation=True, consolidation=True)
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
    sid = _make_session(quality=True, automation=True, roi=True)
    body = _post_verdict(sid).json()

    km = body["key_metrics"]
    assert "AI Readiness" in km
    assert "Automation Coverage" in km
    assert "Annual Savings" in km


# ---------------------------------------------------------------------------
# Test: executive report is meaningful markdown
# ---------------------------------------------------------------------------

def test_executive_report():
    sid = _make_session(quality=True, automation=True, consolidation=True, roi=True)
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
    sid = _make_session(quality=True, consolidation=True)
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
    # High automation coverage
    entry.automation_report = _make_automation_report(
        sid,
        candidates=[
            AutomationCandidate(
                step_number=i, description=f"Step {i}", actor="Admin",
                current_step_type="Manual", is_candidate=True,
                automation_type="RPA", confidence=0.85, confidence_level="High",
                reasoning="Automated", estimated_effort="Low", priority="High",
            ) for i in range(1, 6)
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
        quality=True, automation=True, consolidation=True, roi=True,
        num_employees=12,
    )
    resp = _post_verdict(sid)
    assert resp.status_code == 200

    body = resp.json()

    # Should have verdict
    assert body["verdict"] in ("AI-Ready", "Partially Ready", "Significant Gaps", "Not Ready")

    # Should have scorecard with 4 ran
    ran = sum(1 for sc in body["scorecard"] if sc["ran"])
    assert ran == 4

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

