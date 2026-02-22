"""Tests for Module 6 — ROI Estimator.

Verifies time-saved/cost-saved calculations, payback periods,
line-item generation, edge cases, and realistic scenarios.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.ingestion import WorkflowStep, WorkflowDiagram
from app.schemas.automation import (
    AutomationCandidate,
    AutomationReport,
    AutomationSummary,
)
from app.schemas.consolidation import (
    ConsolidationReport,
    DataSilo,
    MigrationStep,
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


def _make_automation_report(session_id: str, candidates: list[dict]) -> AutomationReport:
    """Build a minimal AutomationReport from candidate dicts."""
    cands = []
    for c in candidates:
        cands.append(AutomationCandidate(
            step_number=c.get("step_number", 1),
            description=c.get("description", "Test step"),
            actor=c.get("actor", "Admin"),
            current_step_type=c.get("current_step_type", "Manual"),
            is_candidate=c.get("is_candidate", True),
            automation_type=c.get("automation_type", "RPA"),
            confidence=c.get("confidence", 0.75),
            confidence_level=c.get("confidence_level", "Medium"),
            reasoning=c.get("reasoning", "Test reasoning"),
            estimated_effort=c.get("estimated_effort", "Medium"),
            priority=c.get("priority", "High"),
        ))
    automatable = [c for c in cands if c.is_candidate]
    return AutomationReport(
        session_id=session_id,
        ai_readiness_score=0.55,
        readiness_level="Moderate",
        candidates=cands,
        summary=AutomationSummary(
            total_steps=len(cands),
            automatable_steps=len(automatable),
            already_automated=0,
            not_recommended=len(cands) - len(automatable),
            automation_coverage=len(automatable) / max(len(cands), 1),
            avg_confidence=0.75,
            by_type={},
            by_priority={},
        ),
        top_recommendations=[],
        quick_wins=[],
    )


def _make_consolidation_report(session_id: str, migrations: list[dict]) -> ConsolidationReport:
    """Build a minimal ConsolidationReport from migration dicts."""
    steps = []
    for m in migrations:
        steps.append(MigrationStep(
            priority=m.get("priority", 1),
            action=m.get("action", "Replace Paper with Digital"),
            from_tool=m.get("from_tool", "Paper"),
            to_tool=m.get("to_tool", "Digital Form"),
            rationale=m.get("rationale", "Test rationale"),
            effort=m.get("effort", "Low"),
            affected_roles=m.get("affected_roles", []),
        ))
    return ConsolidationReport(
        session_id=session_id,
        silos=[],
        data_flows=[],
        redundancies=[],
        unified_schemas=[],
        migration_steps=steps,
        total_silos=len(migrations),
        informal_silos=0,
        manual_flows=0,
        consolidation_score=0.3,
        executive_summary="Test summary",
        top_recommendations=[],
    )


def _make_session_with_reports(
    auto_candidates: list[dict] | None = None,
    consol_migrations: list[dict] | None = None,
    num_employees: int = 10,
) -> str:
    """Create a session pre-populated with Module 4 and/or Module 5 results."""
    df = pd.DataFrame({"date": ["2025-01-01"], "amount": [100.0]})
    wf = _workflow([
        {"step_number": 1, "description": "Enter daily sales", "actor": "Cashier", "step_type": "Manual"},
    ])

    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={
            "industry": "Retail",
            "num_employees": num_employees,
            "tools_used": ["Excel"],
        },
        data_issues=[],
        workflow_analysis=wf,
    )

    entry = session_store.get(sid)
    if auto_candidates is not None:
        entry.automation_report = _make_automation_report(sid, auto_candidates)
    if consol_migrations is not None:
        entry.consolidation_report = _make_consolidation_report(sid, consol_migrations)

    return sid


def _post_roi(session_id: str):
    return client.post(
        "/api/v1/analyze/roi",
        data={"session_id": session_id},
    )


# ---------------------------------------------------------------------------
# Test: basic automation ROI line generation
# ---------------------------------------------------------------------------

def test_automation_lines_generated():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "description": "Enter daily sales into Excel", "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Medium", "priority": "High"},
            {"step_number": 2, "description": "Print receipt", "automation_type": "Digital Form", "is_candidate": True, "estimated_effort": "Low", "priority": "Medium"},
        ],
    )
    body = _post_roi(sid).json()

    assert len(body["automation_lines"]) == 2
    for line in body["automation_lines"]:
        assert line["annual_hours_saved"] > 0
        assert line["annual_cost_saved"] > 0
        assert line["implementation_cost"] > 0


# ---------------------------------------------------------------------------
# Test: non-candidates excluded from ROI
# ---------------------------------------------------------------------------

def test_non_candidates_excluded():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "is_candidate": True, "automation_type": "RPA"},
            {"step_number": 2, "is_candidate": False, "automation_type": "Not Recommended"},
        ],
    )
    body = _post_roi(sid).json()
    assert len(body["automation_lines"]) == 1
    assert body["automation_lines"][0]["step_number"] == 1


# ---------------------------------------------------------------------------
# Test: consolidation ROI lines generated
# ---------------------------------------------------------------------------

def test_consolidation_lines_generated():
    sid = _make_session_with_reports(
        consol_migrations=[
            {"priority": 1, "action": "Replace Paper with POS", "from_tool": "Paper", "to_tool": "Square POS", "effort": "Low"},
            {"priority": 2, "action": "Replace Excel with Google Sheets", "from_tool": "Excel", "to_tool": "Google Sheets", "effort": "Medium"},
        ],
    )
    body = _post_roi(sid).json()

    assert len(body["consolidation_lines"]) == 2
    for line in body["consolidation_lines"]:
        assert line["annual_hours_saved"] > 0
        assert line["annual_cost_saved"] > 0


# ---------------------------------------------------------------------------
# Test: Low effort consolidation saves more hours than Medium
# ---------------------------------------------------------------------------

def test_consolidation_effort_ordering():
    sid = _make_session_with_reports(
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
            {"priority": 2, "effort": "Medium", "from_tool": "Excel", "to_tool": "Cloud"},
        ],
    )
    body = _post_roi(sid).json()

    lines = body["consolidation_lines"]
    low_line = next(l for l in lines if l["effort"] == "Low")
    med_line = next(l for l in lines if l["effort"] == "Medium")
    # Low effort (informal→digital) should save MORE hours because informal
    # tools have higher overhead
    assert low_line["hours_saved_per_week"] > med_line["hours_saved_per_week"]


# ---------------------------------------------------------------------------
# Test: payback months calculated correctly
# ---------------------------------------------------------------------------

def test_payback_months():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Medium"},
        ],
    )
    body = _post_roi(sid).json()

    line = body["automation_lines"][0]
    assert line["payback_months"] is not None
    assert line["payback_months"] > 0
    # Verify formula: payback = impl_cost / (annual_cost / 12)
    expected = line["implementation_cost"] / (line["annual_cost_saved"] / 12)
    assert abs(line["payback_months"] - round(expected, 1)) < 0.2


# ---------------------------------------------------------------------------
# Test: summary totals are correct
# ---------------------------------------------------------------------------

def test_summary_totals():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
            {"step_number": 2, "automation_type": "Digital Form", "is_candidate": True},
        ],
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
        ],
    )
    body = _post_roi(sid).json()
    s = body["summary"]

    # Total hours = sum of all lines
    auto_hours = sum(l["annual_hours_saved"] for l in body["automation_lines"])
    consol_hours = sum(l["annual_hours_saved"] for l in body["consolidation_lines"])
    assert abs(s["total_annual_hours_saved"] - (auto_hours + consol_hours)) < 1.0

    # Total cost = sum of all lines
    auto_cost = sum(l["annual_cost_saved"] for l in body["automation_lines"])
    consol_cost = sum(l["annual_cost_saved"] for l in body["consolidation_lines"])
    assert abs(s["total_annual_cost_saved"] - (auto_cost + consol_cost)) < 10

    # Net first year = savings - implementation
    assert abs(s["net_first_year_benefit"] - (s["total_annual_cost_saved"] - s["total_implementation_cost"])) < 10


# ---------------------------------------------------------------------------
# Test: 3-year net benefit formula
# ---------------------------------------------------------------------------

def test_three_year_benefit():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
    )
    body = _post_roi(sid).json()
    s = body["summary"]

    expected_3y = (s["total_annual_cost_saved"] * 3) - s["total_implementation_cost"]
    assert abs(s["three_year_net_benefit"] - expected_3y) < 10


# ---------------------------------------------------------------------------
# Test: ROI percentage
# ---------------------------------------------------------------------------

def test_roi_percentage():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
    )
    body = _post_roi(sid).json()
    s = body["summary"]

    if s["total_implementation_cost"] > 0:
        expected_roi = (s["total_annual_cost_saved"] / s["total_implementation_cost"]) * 100
        assert abs(s["roi_percentage"] - expected_roi) < 1.0


# ---------------------------------------------------------------------------
# Test: assumptions exposed in report
# ---------------------------------------------------------------------------

def test_assumptions_present():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
    )
    body = _post_roi(sid).json()

    assert len(body["assumptions"]) >= 4
    keys = [a["key"] for a in body["assumptions"]]
    assert "hourly_wage" in keys
    assert "working_weeks" in keys
    assert "num_employees" in keys


# ---------------------------------------------------------------------------
# Test: executive summary present and meaningful
# ---------------------------------------------------------------------------

def test_executive_summary():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
        ],
    )
    body = _post_roi(sid).json()

    summary = body["executive_summary"]
    assert len(summary) > 50
    assert "hours" in summary.lower() or "savings" in summary.lower() or "₹" in summary


# ---------------------------------------------------------------------------
# Test: recommendations generated
# ---------------------------------------------------------------------------

def test_recommendations_generated():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Low"},
        ],
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
        ],
    )
    body = _post_roi(sid).json()

    assert len(body["top_recommendations"]) >= 1


# ---------------------------------------------------------------------------
# Test: 404 for missing session
# ---------------------------------------------------------------------------

def test_session_not_found():
    resp = _post_roi("nonexistent_session_12345")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test: 422 if no Module 4 and no Module 5
# ---------------------------------------------------------------------------

def test_no_prerequisites():
    df = pd.DataFrame({"x": [1]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    resp = _post_roi(sid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: works with only Module 4 (no Module 5)
# ---------------------------------------------------------------------------

def test_automation_only():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
        consol_migrations=None,
    )
    resp = _post_roi(sid)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["automation_lines"]) == 1
    assert len(body["consolidation_lines"]) == 0


# ---------------------------------------------------------------------------
# Test: works with only Module 5 (no Module 4)
# ---------------------------------------------------------------------------

def test_consolidation_only():
    sid = _make_session_with_reports(
        auto_candidates=None,
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
        ],
    )
    resp = _post_roi(sid)
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["automation_lines"]) == 0
    assert len(body["consolidation_lines"]) == 1


# ---------------------------------------------------------------------------
# Test: report stored in session
# ---------------------------------------------------------------------------

def test_report_stored_in_session():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
        ],
    )
    _post_roi(sid)
    entry = session_store.get(sid)
    assert entry is not None
    assert entry.roi_report is not None
    assert entry.roi_report.session_id == sid


# ---------------------------------------------------------------------------
# Test: RPA saves more hours/week than Digital Form
# ---------------------------------------------------------------------------

def test_rpa_saves_more_than_digital_form():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
            {"step_number": 2, "automation_type": "Digital Form", "is_candidate": True},
        ],
    )
    body = _post_roi(sid).json()

    lines = body["automation_lines"]
    rpa = next(l for l in lines if l["automation_type"] == "RPA")
    df_line = next(l for l in lines if l["automation_type"] == "Digital Form")
    assert rpa["hours_saved_per_week"] > df_line["hours_saved_per_week"]


# ---------------------------------------------------------------------------
# Test: AI/ML has highest implementation cost
# ---------------------------------------------------------------------------

def test_aiml_highest_implementation_cost():
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "AI/ML", "is_candidate": True},
            {"step_number": 2, "automation_type": "RPA", "is_candidate": True},
            {"step_number": 3, "automation_type": "Digital Form", "is_candidate": True},
        ],
    )
    body = _post_roi(sid).json()

    lines = body["automation_lines"]
    aiml = next(l for l in lines if l["automation_type"] == "AI/ML")
    for l in lines:
        if l["automation_type"] != "AI/ML":
            assert aiml["implementation_cost"] >= l["implementation_cost"]


# ---------------------------------------------------------------------------
# Test: realistic restaurant scenario
# ---------------------------------------------------------------------------

def test_realistic_restaurant():
    """Full restaurant workflow with 8 automatable steps + 5 consolidation steps."""
    sid = _make_session_with_reports(
        num_employees=12,
        auto_candidates=[
            {"step_number": 1, "description": "Record daily ingredient usage", "automation_type": "Digital Form", "is_candidate": True, "estimated_effort": "Low", "priority": "High"},
            {"step_number": 2, "description": "Enter orders into notepad", "automation_type": "Digital Form", "is_candidate": True, "estimated_effort": "Low", "priority": "High"},
            {"step_number": 3, "description": "Calculate customer bill", "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Medium", "priority": "Critical"},
            {"step_number": 4, "description": "Transfer daily totals to Excel", "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Medium", "priority": "High"},
            {"step_number": 5, "description": "Send orders to kitchen via WhatsApp", "automation_type": "API Integration", "is_candidate": True, "estimated_effort": "Medium", "priority": "Medium"},
            {"step_number": 6, "description": "Check stock levels manually", "automation_type": "RPA", "is_candidate": True, "estimated_effort": "Medium", "priority": "Medium"},
            {"step_number": 7, "description": "Prepare food", "automation_type": "Not Recommended", "is_candidate": False},
            {"step_number": 8, "description": "Write staff schedule in diary", "automation_type": "Digital Form", "is_candidate": True, "estimated_effort": "Low", "priority": "Medium"},
        ],
        consol_migrations=[
            {"priority": 1, "action": "Replace Paper logbook with POS", "from_tool": "Paper logbook", "to_tool": "Square POS", "effort": "Low"},
            {"priority": 2, "action": "Replace WhatsApp with KitchenDisplay", "from_tool": "WhatsApp", "to_tool": "Kitchen Display System", "effort": "Low"},
            {"priority": 3, "action": "Replace Calculator with POS", "from_tool": "Calculator", "to_tool": "Square POS", "effort": "Low"},
            {"priority": 4, "action": "Replace Excel with Google Sheets", "from_tool": "Excel", "to_tool": "Google Sheets", "effort": "Medium"},
            {"priority": 5, "action": "Replace Ledger with Tally", "from_tool": "Ledger book", "to_tool": "Tally", "effort": "Medium"},
        ],
    )
    resp = _post_roi(sid)
    assert resp.status_code == 200

    body = resp.json()

    # Should have 7 automation lines (8 candidates minus 1 Not Recommended)
    assert len(body["automation_lines"]) == 7

    # Should have 5 consolidation lines
    assert len(body["consolidation_lines"]) == 5

    s = body["summary"]

    # Annual savings should be meaningful (not zero)
    assert s["total_annual_cost_saved"] > 50_000  # at least ₹50k/year for 12 items

    # Implementation cost should be realistic
    assert 50_000 < s["total_implementation_cost"] < 500_000

    # Payback should be reasonable (under 36 months = 3 years)
    assert s["overall_payback_months"] is not None
    assert s["overall_payback_months"] < 36

    # 3-year net should be positive
    assert s["three_year_net_benefit"] > 0

    # ROI percentage should be meaningful (conservative estimates → ~40%+)
    assert s["roi_percentage"] > 30

    # Executive summary should mention hrs/savings
    assert "hour" in body["executive_summary"].lower() or "₹" in body["executive_summary"]

    # Should have recommendations
    assert len(body["top_recommendations"]) >= 2


# ---------------------------------------------------------------------------
# Test: all lines have consistent hours→cost conversion
# ---------------------------------------------------------------------------

def test_hours_to_cost_consistency():
    """Every line's annual cost should equal annual_hours × ₹180/hr."""
    sid = _make_session_with_reports(
        auto_candidates=[
            {"step_number": 1, "automation_type": "RPA", "is_candidate": True},
            {"step_number": 2, "automation_type": "API Integration", "is_candidate": True},
        ],
        consol_migrations=[
            {"priority": 1, "effort": "Low", "from_tool": "Paper", "to_tool": "App"},
        ],
    )
    body = _post_roi(sid).json()
    wage = 180  # default hourly wage

    for line in body["automation_lines"]:
        expected_cost = line["annual_hours_saved"] * wage
        assert abs(line["annual_cost_saved"] - expected_cost) < 10

    for line in body["consolidation_lines"]:
        expected_cost = line["annual_hours_saved"] * wage
        assert abs(line["annual_cost_saved"] - expected_cost) < 10
