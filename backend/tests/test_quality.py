"""Tests for Module 2 -- POST /api/v1/analyze/quality.

Scoring model (7 dimensions):
  Completeness            17 %
  Deduplication           12 %
  Consistency             11 %
  Structural Integrity     8 %
  Process Digitisation    25 %   (from workflow_analysis)
  Tool Maturity           12 %   (from company_metadata.tools_used)
  Data Coverage           15 %   (based on supplementary docs uploaded)

Data Coverage points: sales=0.40, invoices=0.25, payroll=0.20, inventory=0.15
Default (sales only) = 0.40
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.ingestion import DataIssue, IssueType, WorkflowStep, WorkflowDiagram

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _workflow(steps: list[dict] | None = None) -> WorkflowDiagram:
    if steps is None:
        steps = []
    return WorkflowDiagram(
        steps=[WorkflowStep(**s) for s in steps],
        mermaid_diagram="",
        summary="test",
    )


def _make_session(
    df: pd.DataFrame,
    issues: list[DataIssue] | None = None,
    tools: list[str] | None = None,
    workflow_steps: list[dict] | None = None,
    documents_provided: list[str] | None = None,
    supplementary_doc_stats: dict | None = None,
) -> str:
    return session_store.create(
        raw_dataframe=df,
        workflow_text="test workflow",
        company_metadata={
            "industry": "Retail",
            "num_employees": 10,
            "tools_used": tools or [],
        },
        data_issues=issues or [],
        workflow_analysis=_workflow(workflow_steps) if workflow_steps is not None else None,
        documents_provided=documents_provided,  # None -> session_store defaults to ["sales"]
        supplementary_doc_stats=supplementary_doc_stats,
    )


def _post_quality(session_id: str):
    return client.post(
        "/api/v1/analyze/quality",
        data={"session_id": session_id},
    )


# ---------------------------------------------------------------------------
# Test: perfect data + fully automated + enterprise tools -> High
# ---------------------------------------------------------------------------

def test_quality_perfect_all_dimensions():
    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount": [100.0, 200.0, 300.0],
        "status": ["paid", "pending", "paid"],
    })
    steps = [
        {"step_number": 1, "description": "Receive order via POS", "actor": "System", "step_type": "Automated"},
        {"step_number": 2, "description": "Process payment", "actor": "System", "step_type": "Automated"},
        {"step_number": 3, "description": "Generate invoice", "actor": "System", "step_type": "Automated"},
    ]
    sid = _make_session(df, tools=["Shopify", "QuickBooks", "Slack"], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()

    # volume penalty for a 3-row test dataframe (< 20 rows -> volume score 0.05)
    assert body["completeness_score"] >= 0.60
    assert body["completeness_score"] < 0.75  # not 1.0 — volume dragged it down
    assert body["deduplication_score"] == 1.0
    assert body["process_digitisation_score"] == 1.0
    assert body["tool_maturity_score"] == 1.0
    # sales-only data coverage = 0.40
    assert body["data_coverage_score"] == pytest.approx(0.40)
    assert body["ai_readiness_score"] >= 0.85
    assert body["readiness_level"] == "High"
    assert body["total_workflow_steps"] == 3
    assert body["automated_steps"] == 3
    assert body["manual_steps"] == 0


# ---------------------------------------------------------------------------
# Test: perfect data but ALL manual + low tools -> score drops hard
# ---------------------------------------------------------------------------

def test_quality_all_manual_low_tools():
    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount": [100.0, 200.0, 300.0],
    })
    steps = [
        {"step_number": 1, "description": "Check stock in logbook", "actor": "Manager", "step_type": "Manual"},
        {"step_number": 2, "description": "Call supplier on WhatsApp", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 3, "description": "Write order on notepad", "actor": "Waiter", "step_type": "Manual"},
        {"step_number": 4, "description": "Calculate bill manually", "actor": "Cashier", "step_type": "Manual"},
    ]
    sid = _make_session(df, tools=["WhatsApp", "Paper", "Calculator"], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()

    assert body["completeness_score"] < 0.75  # volume penalty (3 rows)
    assert body["process_digitisation_score"] == 0.0
    assert body["tool_maturity_score"] <= 0.20
    assert body["manual_steps"] == 4
    assert body["automated_steps"] == 0
    assert body["ai_readiness_score"] < 0.65
    assert body["readiness_level"] in ("Low", "Moderate")


# ---------------------------------------------------------------------------
# Test: mixed workflow (some automated, some manual)
# ---------------------------------------------------------------------------

def test_quality_mixed_workflow():
    df = pd.DataFrame({"value": [1, 2, 3]})
    steps = [
        {"step_number": 1, "description": "Take order on POS", "actor": "Staff", "step_type": "Automated"},
        {"step_number": 2, "description": "Kitchen prep", "actor": "Chef", "step_type": "Manual"},
        {"step_number": 3, "description": "Auto-print bill", "actor": "System", "step_type": "Automated"},
        {"step_number": 4, "description": "Manual delivery", "actor": "Driver", "step_type": "Manual"},
    ]
    sid = _make_session(df, tools=["Excel", "Google Pay"], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert abs(body["process_digitisation_score"] - 0.50) < 0.01
    assert body["automated_steps"] == 2
    assert body["manual_steps"] == 2
    assert abs(body["tool_maturity_score"] - 0.50) < 0.01


# ---------------------------------------------------------------------------
# Test: no workflow analysis -> pessimistic default (0.10)
# ---------------------------------------------------------------------------

def test_quality_no_workflow():
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df, tools=["Excel"])
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["process_digitisation_score"] == 0.10
    assert body["total_workflow_steps"] == 0


# ---------------------------------------------------------------------------
# Test: no tools -> pessimistic default (0.05)
# ---------------------------------------------------------------------------

def test_quality_no_tools():
    df = pd.DataFrame({"x": [1, 2, 3]})
    steps = [{"step_number": 1, "description": "Do something", "actor": "Staff", "step_type": "Automated"}]
    sid = _make_session(df, tools=[], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["tool_maturity_score"] == 0.05
    assert body["tools_detected"] == []


# ---------------------------------------------------------------------------
# Test: decision steps get partial credit
# ---------------------------------------------------------------------------

def test_quality_decision_steps():
    df = pd.DataFrame({"x": [1, 2]})
    steps = [
        {"step_number": 1, "description": "Check threshold", "actor": "System", "step_type": "Decision"},
        {"step_number": 2, "description": "Send notification", "actor": "System", "step_type": "Automated"},
        {"step_number": 3, "description": "Manual review", "actor": "Manager", "step_type": "Manual"},
    ]
    sid = _make_session(df, tools=["Slack"], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert 0.40 <= body["process_digitisation_score"] <= 0.50


# ---------------------------------------------------------------------------
# Test: missing values reduce completeness
# ---------------------------------------------------------------------------

def test_quality_missing_values():
    df = pd.DataFrame({
        "order_id": [1, 2, None, None, 5],
        "amount":   [10.0, None, None, 40.0, 50.0],
    })
    issues = [
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="order_id",
                  description="2 missing", affected_count=2, severity="medium"),
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="amount",
                  description="2 missing", affected_count=2, severity="medium"),
    ]
    sid = _make_session(df, issues)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["missing_cells"] == 4
    # volume-adjusted: cell_score=0.60, volume_score(5 rows)=0.05
    # completeness = 0.60*0.65 + 0.05*0.35 ≈ 0.408
    assert body["completeness_score"] < 0.55
    assert body["completeness_score"] > 0.30


# ---------------------------------------------------------------------------
# Test: duplicates reduce deduplication
# ---------------------------------------------------------------------------

def test_quality_duplicates():
    df = pd.DataFrame({"id": [1, 1, 2, 3, 3], "value": [10, 10, 20, 30, 30]})
    sid = _make_session(df)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["duplicate_rows"] == 2
    assert body["deduplication_score"] < 1.0


# ---------------------------------------------------------------------------
# Test: inconsistent columns reduce consistency
# ---------------------------------------------------------------------------

def test_quality_inconsistent_columns():
    df = pd.DataFrame({"Order Date": ["2025-01-01"], "CustomerName": ["Alice"], "total amount": [500.0]})
    issues = [DataIssue(issue_type=IssueType.INCONSISTENT_COLUMN_NAMES, column=None,
                        description="3 bad", affected_count=3, severity="medium")]
    sid = _make_session(df, issues)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    assert resp.json()["consistency_score"] < 1.0


# ---------------------------------------------------------------------------
# Test: unparsed dates reduce structural integrity
# ---------------------------------------------------------------------------

def test_quality_unparsed_dates():
    df = pd.DataFrame({"order_date": ["01/01/2025", "02/01/2025"], "amount": [100.0, 200.0]})
    issues = [DataIssue(issue_type=IssueType.UNPARSED_DATES, column="order_date",
                        description="date as text", affected_count=None, severity="medium")]
    sid = _make_session(df, issues)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    assert resp.json()["structural_integrity_score"] < 1.0


# ---------------------------------------------------------------------------
# Test: report stored back in session
# ---------------------------------------------------------------------------

def test_quality_stored_in_session():
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df)
    _post_quality(sid)

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.quality_report is not None
    assert entry.quality_report.session_id == sid


# ---------------------------------------------------------------------------
# Test: 404 for unknown session
# ---------------------------------------------------------------------------

def test_quality_session_not_found():
    resp = _post_quality("nonexistent_session_id_12345")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test: document session -> 422
# ---------------------------------------------------------------------------

def test_quality_document_session_rejected():
    sid = session_store.create(
        raw_dataframe=None,
        workflow_text="some sop text",
        company_metadata={"industry": "Retail", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    resp = _post_quality(sid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: tool tier classification
# ---------------------------------------------------------------------------

def test_tool_tiers():
    df = pd.DataFrame({"x": [1]})
    sid_high = _make_session(df, tools=["SAP", "Salesforce", "Stripe"])
    resp_high = _post_quality(sid_high)
    body_high = resp_high.json()

    sid_low = _make_session(df, tools=["WhatsApp", "Paper", "Phone"])
    resp_low = _post_quality(sid_low)
    body_low = resp_low.json()

    assert body_high["tool_maturity_score"] > body_low["tool_maturity_score"]
    assert body_high["tool_maturity_score"] == 1.0
    assert body_low["tool_maturity_score"] <= 0.20


# ---------------------------------------------------------------------------
# Test: real restaurant scenario -> NOT High
# ---------------------------------------------------------------------------

def test_realistic_restaurant_scenario():
    df = pd.DataFrame({
        "order_id": range(1, 26),
        "Customer Name": ["Name"] * 22 + [None, None, None],
        "amount": [100.0] * 24 + [None],
    })
    issues = [
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="Customer Name",
                  description="3 missing", affected_count=3, severity="medium"),
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="amount",
                  description="1 missing", affected_count=1, severity="low"),
        DataIssue(issue_type=IssueType.INCONSISTENT_COLUMN_NAMES, column=None,
                  description="1 bad column", affected_count=1, severity="medium"),
    ]
    steps = [
        {"step_number": i, "description": f"Manual step {i}", "actor": "Staff", "step_type": "Manual"}
        for i in range(1, 9)
    ]
    tools = ["Excel", "WhatsApp", "Google Pay", "PhonePe", "Tally"]

    sid = _make_session(df, issues=issues, tools=tools, workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["readiness_level"] in ("Low", "Moderate")
    assert body["ai_readiness_score"] < 0.70
    assert body["process_digitisation_score"] == 0.0
    assert body["data_coverage_score"] == pytest.approx(0.40)  # sales only by default
    recs_text = " ".join(body["top_recommendations"]).lower()
    assert "manual" in recs_text


# ---------------------------------------------------------------------------
# Test: Critical level -- all dimensions bad
# ---------------------------------------------------------------------------

def test_readiness_critical():
    rows = 20
    df = pd.DataFrame({
        "Order Date":   [None] * rows,
        "Customer ID":  [None] * rows,
        "Total Amount": [None] * rows,
    })
    issues = [
        DataIssue(issue_type=IssueType.INCONSISTENT_COLUMN_NAMES,
                  column=None, description="3 bad", affected_count=3, severity="medium"),
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="Order Date",
                  description="all missing", affected_count=rows, severity="high"),
        DataIssue(issue_type=IssueType.UNPARSED_DATES, column="Order Date",
                  description="date as text", affected_count=None, severity="medium"),
    ]
    steps = [
        {"step_number": 1, "description": "Manual check", "actor": "Staff", "step_type": "Manual"},
        {"step_number": 2, "description": "Manual entry", "actor": "Staff", "step_type": "Manual"},
    ]
    sid = _make_session(df, issues=issues, tools=[], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["readiness_level"] == "Critical"
    assert body["ai_readiness_score"] < 0.40


# ---------------------------------------------------------------------------
# Test: volume scoring penalises small datasets
# ---------------------------------------------------------------------------

def test_volume_scoring():
    df_tiny  = pd.DataFrame({"x": [1, 2, 3]})          # 3 rows  < 20  → volume 0.05
    df_large = pd.DataFrame({"x": range(2000)})          # 2000 rows     → volume 0.85

    body_tiny  = _post_quality(_make_session(df_tiny)).json()
    body_large = _post_quality(_make_session(df_large)).json()

    assert body_large["completeness_score"] > body_tiny["completeness_score"]
    assert body_tiny["completeness_score"]  < 0.75   # volume penalty applied
    assert body_large["completeness_score"] >= 0.85  # 2 000 rows → high volume score
    # Both datasets are 100 % cell-complete, so the difference is purely volume
    assert body_large["missing_cells"] == 0
    assert body_tiny["missing_cells"] == 0


# ---------------------------------------------------------------------------
# Test: fuzzy deduplication catches case/whitespace variants
# ---------------------------------------------------------------------------

def test_fuzzy_deduplication():
    # 'Alice', 'alice', 'Alice ' all normalise to the same row — NOT exact dupes
    df = pd.DataFrame({
        "name":   ["Alice", "alice", "Alice ", "Bob"],
        "amount": [100.0,   100.0,   100.0,    200.0],
    })
    sid  = _make_session(df)
    body = _post_quality(sid).json()

    assert body["duplicate_rows"] == 0    # no EXACT duplicates
    assert body["deduplication_score"] < 1.0  # but fuzzy pass caught near-dupes


# ---------------------------------------------------------------------------
# Test: content-aware coverage — row count matters, not just file presence
# ---------------------------------------------------------------------------

def test_content_aware_coverage():
    df = pd.DataFrame({"x": [1, 2, 3]})

    # Substantial invoice file (≥ 50 rows) → full base points
    sid_rich = _make_session(
        df,
        documents_provided=["sales", "invoices"],
        supplementary_doc_stats={"invoices": {"readable": True, "is_pdf": False, "row_count": 100}},
    )
    body_rich = _post_quality(sid_rich).json()

    # Nearly-empty invoice file (< 10 rows) → only 20 % of base points
    sid_empty = _make_session(
        df,
        documents_provided=["sales", "invoices"],
        supplementary_doc_stats={"invoices": {"readable": True, "is_pdf": False, "row_count": 3}},
    )
    body_empty = _post_quality(sid_empty).json()

    # Unreadable/corrupt file → only 30 % of base points
    sid_bad = _make_session(
        df,
        documents_provided=["sales", "invoices"],
        supplementary_doc_stats={"invoices": {"readable": False, "reason": "corrupt"}},
    )
    body_bad = _post_quality(sid_bad).json()

    assert body_rich["data_coverage_score"] > body_empty["data_coverage_score"]
    assert body_rich["data_coverage_score"] > body_bad["data_coverage_score"]
    # 0.40 (sales) + 0.25 (invoices full)  = 0.65
    assert body_rich["data_coverage_score"]  == pytest.approx(0.65)
    # 0.40 (sales) + 0.25*0.20 (nearly empty) = 0.45
    assert body_empty["data_coverage_score"] == pytest.approx(0.45)
    # 0.40 (sales) + 0.25*0.30 (corrupt)       = 0.475
    assert body_bad["data_coverage_score"]   == pytest.approx(0.475)


# ---------------------------------------------------------------------------
# Test: data coverage scoring based on documents_provided
# ---------------------------------------------------------------------------

def test_data_coverage_scoring():
    df = pd.DataFrame({"x": [1, 2, 3]})

    # sales only -> 0.40
    sid = _make_session(df, documents_provided=["sales"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.40)
    assert "sales" in body["documents_provided"]

    # sales + invoices -> 0.65
    sid = _make_session(df, documents_provided=["sales", "invoices"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.65)

    # sales + invoices + payroll -> 0.85
    sid = _make_session(df, documents_provided=["sales", "invoices", "payroll"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.85)

    # all four -> 1.0
    sid = _make_session(df, documents_provided=["sales", "invoices", "payroll", "inventory"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(1.0)
    assert body["readiness_level"] != "Critical"  # coverage helps push score up
