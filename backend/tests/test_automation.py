"""Tests for Module 4 — POST /api/v1/analyze/automation.

Classification engine is fully deterministic (rule-based).
Tests verify keyword matching, automation type assignment,
confidence scoring, priority ranking, and edge cases.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.ingestion import DataIssue, WorkflowStep, WorkflowDiagram
from app.schemas.quality import QualityReport, ColumnQuality

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workflow(steps: list[dict]) -> WorkflowDiagram:
    return WorkflowDiagram(
        steps=[WorkflowStep(**s) for s in steps],
        mermaid_diagram="",
        summary="test",
    )


def _quality_report(session_id: str, ai_readiness: float = 0.70) -> QualityReport:
    """Create a minimal QualityReport with the given readiness score."""
    level = "High" if ai_readiness >= 0.80 else (
        "Moderate" if ai_readiness >= 0.60 else (
            "Low" if ai_readiness >= 0.40 else "Critical"
        )
    )
    return QualityReport(
        session_id=session_id,
        row_count=100,
        column_count=5,
        total_cells=500,
        missing_cells=10,
        duplicate_rows=2,
        completeness_score=0.98,
        deduplication_score=0.98,
        consistency_score=0.90,
        structural_integrity_score=1.0,
        process_digitisation_score=0.50,
        tool_maturity_score=0.60,
        data_coverage_score=0.40,
        total_workflow_steps=5,
        automated_steps=2,
        manual_steps=3,
        tools_detected=["Excel"],
        documents_provided=["sales"],
        ai_readiness_score=ai_readiness,
        readiness_level=level,
        column_quality=[],
        top_recommendations=[],
    )


def _make_session(
    steps: list[dict],
    ai_readiness: float = 0.70,
    tools: list[str] | None = None,
) -> str:
    """Create a session with workflow + quality report ready for Module 4."""
    df = pd.DataFrame({"x": list(range(100))})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={
            "industry": "Retail",
            "num_employees": 10,
            "tools_used": tools or ["Excel"],
        },
        data_issues=[],
        workflow_analysis=_workflow(steps),
    )
    # Inject quality report directly
    qr = _quality_report(sid, ai_readiness)
    session_store.patch(sid, quality_report=qr)
    return sid


def _post_automation(session_id: str):
    return client.post(
        "/api/v1/analyze/automation",
        data={"session_id": session_id},
    )


# ---------------------------------------------------------------------------
# Test: already automated steps are skipped
# ---------------------------------------------------------------------------

def test_already_automated_steps_skipped():
    steps = [
        {"step_number": 1, "description": "Auto-generate invoice", "actor": "System", "step_type": "Automated"},
        {"step_number": 2, "description": "Process payment via POS", "actor": "System", "step_type": "Automated"},
    ]
    sid = _make_session(steps)
    resp = _post_automation(sid)

    assert resp.status_code == 200
    body = resp.json()

    assert body["summary"]["total_steps"] == 2
    assert body["summary"]["already_automated"] == 2
    assert body["summary"]["automatable_steps"] == 0

    for c in body["candidates"]:
        assert c["is_candidate"] is False
        assert c["priority"] == "Skip"


# ---------------------------------------------------------------------------
# Test: data entry manual step → RPA
# ---------------------------------------------------------------------------

def test_data_entry_classified_as_rpa():
    steps = [
        {"step_number": 1, "description": "Enter customer orders into Excel", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "RPA"
    assert c["confidence"] >= 0.40


# ---------------------------------------------------------------------------
# Test: calculation step → RPA
# ---------------------------------------------------------------------------

def test_calculation_classified_as_rpa():
    steps = [
        {"step_number": 1, "description": "Calculate daily totals and reconcile", "actor": "Cashier", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "RPA"


# ---------------------------------------------------------------------------
# Test: communication step → API Integration
# ---------------------------------------------------------------------------

def test_communication_classified_as_api():
    steps = [
        {"step_number": 1, "description": "Call supplier on WhatsApp to confirm order", "actor": "Owner", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "API Integration"


# ---------------------------------------------------------------------------
# Test: document handling step → Digital Form
# ---------------------------------------------------------------------------

def test_document_handling_classified_as_digital_form():
    steps = [
        {"step_number": 1, "description": "Print receipt and hand to customer", "actor": "Cashier", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "Digital Form"


# ---------------------------------------------------------------------------
# Test: decision step → Decision Engine
# ---------------------------------------------------------------------------

def test_decision_classified_as_decision_engine():
    steps = [
        {"step_number": 1, "description": "Check if stock below threshold", "actor": "Manager", "step_type": "Decision"},
    ]
    # Use readiness < 0.60 so AI/ML upgrade doesn't trigger
    sid = _make_session(steps, ai_readiness=0.50)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "Decision Engine"


# ---------------------------------------------------------------------------
# Test: physical step → Not Recommended
# ---------------------------------------------------------------------------

def test_physical_step_not_recommended():
    steps = [
        {"step_number": 1, "description": "Prepare and cook dishes", "actor": "Chef", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is False
    assert c["automation_type"] == "Not Recommended"


# ---------------------------------------------------------------------------
# Test: scheduling step → API Integration
# ---------------------------------------------------------------------------

def test_scheduling_classified_as_api():
    steps = [
        {"step_number": 1, "description": "Book reservation in diary", "actor": "Host", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    # Could be API Integration (scheduling) or Digital Form (diary)
    assert c["automation_type"] in ("API Integration", "Digital Form")


# ---------------------------------------------------------------------------
# Test: AI/ML upgrade when readiness is high
# ---------------------------------------------------------------------------

def test_ai_ml_upgrade_high_readiness():
    steps = [
        {"step_number": 1, "description": "Review and verify customer data entry records", "actor": "Analyst", "step_type": "Manual"},
    ]
    # High readiness → eligible for AI/ML upgrade
    sid = _make_session(steps, ai_readiness=0.85)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["is_candidate"] is True
    assert c["automation_type"] == "AI/ML"


# ---------------------------------------------------------------------------
# Test: AI/ML NOT suggested when readiness is low
# ---------------------------------------------------------------------------

def test_no_ai_ml_when_readiness_low():
    steps = [
        {"step_number": 1, "description": "Review and verify data entry records", "actor": "Analyst", "step_type": "Manual"},
    ]
    # Low readiness → should NOT get AI/ML
    sid = _make_session(steps, ai_readiness=0.35)
    body = _post_automation(sid).json()

    c = body["candidates"][0]
    assert c["automation_type"] != "AI/ML"
    # AI readiness warning should appear in recommendations
    recs_text = " ".join(body["top_recommendations"]).lower()
    assert "ai readiness" in recs_text or "below 60" in recs_text


# ---------------------------------------------------------------------------
# Test: full restaurant workflow — realistic mix
# ---------------------------------------------------------------------------

def test_realistic_restaurant_workflow():
    steps = [
        {"step_number": 1, "description": "Check ingredient stocks in logbook", "actor": "Kitchen Manager", "step_type": "Manual"},
        {"step_number": 2, "description": "Record stock quantities", "actor": "Kitchen Manager", "step_type": "Manual"},
        {"step_number": 3, "description": "Check if stock below threshold", "actor": "Kitchen Manager", "step_type": "Decision"},
        {"step_number": 4, "description": "Place verbal orders to supplier", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 5, "description": "Receive deliveries and store paper invoices", "actor": "Supplier", "step_type": "Manual"},
        {"step_number": 6, "description": "Take table orders on notepad", "actor": "Waiter", "step_type": "Manual"},
        {"step_number": 7, "description": "Prepare dishes", "actor": "Chef", "step_type": "Manual"},
        {"step_number": 8, "description": "Calculate customer bill", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 9, "description": "Process customer payment", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 10, "description": "Record daily totals in ledger", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 11, "description": "Transfer daily sales to Excel", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 12, "description": "Calculate staff salaries", "actor": "Owner", "step_type": "Manual"},
    ]
    sid = _make_session(steps, ai_readiness=0.45)
    resp = _post_automation(sid)

    assert resp.status_code == 200
    body = resp.json()
    summary = body["summary"]

    # Physical cooking should NOT be automatable
    chef_step = next(c for c in body["candidates"] if c["actor"] == "Chef")
    assert chef_step["is_candidate"] is False

    # Data entry (stock recording, ledger, Excel transfer) should be automatable
    data_entry_steps = [
        c for c in body["candidates"]
        if c["is_candidate"] and c["automation_type"] == "RPA"
    ]
    assert len(data_entry_steps) >= 2

    # Decision step should be automatable
    decision_steps = [c for c in body["candidates"] if c["current_step_type"] == "Decision"]
    assert len(decision_steps) == 1
    assert decision_steps[0]["is_candidate"] is True

    # Overall: most steps should be automatable (minus cooking)
    assert summary["automatable_steps"] >= 8
    assert summary["not_recommended"] >= 1  # at least cooking
    assert summary["automation_coverage"] > 0.60

    # Recommendations should exist
    assert len(body["top_recommendations"]) >= 1
    # Low readiness warning
    assert any("ai readiness" in r.lower() or "below 60" in r.lower()
               for r in body["top_recommendations"])


# ---------------------------------------------------------------------------
# Test: confidence ordering — manual > unknown
# ---------------------------------------------------------------------------

def test_confidence_manual_higher_than_unknown():
    steps = [
        {"step_number": 1, "description": "Enter data into system", "actor": "Admin", "step_type": "Manual"},
        {"step_number": 2, "description": "Enter data into system", "actor": "Admin", "step_type": "Unknown"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    manual_conf = body["candidates"][0]["confidence"]
    unknown_conf = body["candidates"][1]["confidence"]
    assert manual_conf > unknown_conf


# ---------------------------------------------------------------------------
# Test: priority ranking logic
# ---------------------------------------------------------------------------

def test_priority_ranking():
    steps = [
        # High impact: manual + data entry + low effort
        {"step_number": 1, "description": "Print receipt and file paper invoice", "actor": "Admin", "step_type": "Manual"},
        # Low impact: physical
        {"step_number": 2, "description": "Deliver packages to customers", "actor": "Driver", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    admin = body["candidates"][0]
    driver = body["candidates"][1]

    # Admin should have higher priority
    priority_map = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3, "Skip": 4}
    assert priority_map[admin["priority"]] <= priority_map.get(driver["priority"], 4)


# ---------------------------------------------------------------------------
# Test: quick wins populated
# ---------------------------------------------------------------------------

def test_quick_wins():
    steps = [
        {"step_number": 1, "description": "Print receipt and hand to customer", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 2, "description": "File paper invoices in cabinet", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    # Digital Form candidates with low effort should appear as quick wins
    assert len(body["quick_wins"]) >= 1


# ---------------------------------------------------------------------------
# Test: summary by_type and by_priority populated
# ---------------------------------------------------------------------------

def test_summary_breakdowns():
    steps = [
        {"step_number": 1, "description": "Enter orders into Excel", "actor": "Admin", "step_type": "Manual"},
        {"step_number": 2, "description": "Call supplier to confirm", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 3, "description": "Print receipt", "actor": "Cashier", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    body = _post_automation(sid).json()

    summary = body["summary"]
    assert sum(summary["by_type"].values()) == summary["automatable_steps"]
    assert sum(summary["by_priority"].values()) == summary["automatable_steps"]


# ---------------------------------------------------------------------------
# Test: 404 for missing session
# ---------------------------------------------------------------------------

def test_session_not_found():
    resp = _post_automation("nonexistent_session_12345")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test: 422 if no workflow analysis
# ---------------------------------------------------------------------------

def test_missing_workflow():
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    # Give it a quality report but no workflow
    qr = _quality_report(sid, 0.70)
    session_store.patch(sid, quality_report=qr)

    resp = _post_automation(sid)
    assert resp.status_code == 422
    assert "workflow" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test: 422 if no quality report
# ---------------------------------------------------------------------------

def test_missing_quality_report():
    steps = [
        {"step_number": 1, "description": "Enter data", "actor": "Admin", "step_type": "Manual"},
    ]
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=_workflow(steps),
    )
    # No quality report set

    resp = _post_automation(sid)
    assert resp.status_code == 422
    assert "quality" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test: report stored back in session
# ---------------------------------------------------------------------------

def test_report_stored_in_session():
    steps = [
        {"step_number": 1, "description": "Enter data", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(steps)
    _post_automation(sid)

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.automation_report is not None
    assert entry.automation_report.session_id == sid


# ---------------------------------------------------------------------------
# Test: empty workflow (steps=[]) → 422
# ---------------------------------------------------------------------------

def test_empty_workflow_rejected():
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=_workflow([]),
    )
    qr = _quality_report(sid, 0.70)
    session_store.patch(sid, quality_report=qr)

    resp = _post_automation(sid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: readiness score carried forward correctly
# ---------------------------------------------------------------------------

def test_readiness_carried_forward():
    steps = [
        {"step_number": 1, "description": "Enter data", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(steps, ai_readiness=0.42)
    body = _post_automation(sid).json()

    assert body["ai_readiness_score"] == pytest.approx(0.42)
    assert body["readiness_level"] == "Low"
