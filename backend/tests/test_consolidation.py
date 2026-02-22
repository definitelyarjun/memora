"""Tests for Module 5 — Data Consolidation Recommendation Engine.

Verifies silo discovery, data flow detection, redundancy flagging,
schema generation, migration planning, and consolidation scoring.
"""

from __future__ import annotations

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store
from app.schemas.ingestion import WorkflowStep, WorkflowDiagram

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


def _make_session(
    tools: list[str],
    steps: list[dict] | None = None,
    df: pd.DataFrame | None = None,
    documents_provided: list[str] | None = None,
) -> str:
    """Create a session with company metadata + optional workflow + dataframe."""
    if df is None:
        df = pd.DataFrame({"date": ["2025-01-01"], "amount": [100.0], "customer": ["Alice"]})

    wf = _workflow(steps) if steps else None
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={
            "industry": "Retail",
            "num_employees": 10,
            "tools_used": tools,
        },
        data_issues=[],
        workflow_analysis=wf,
        documents_provided=documents_provided or ["sales"],
    )
    return sid


def _post_consolidation(session_id: str):
    return client.post(
        "/api/v1/analyze/consolidation",
        data={"session_id": session_id},
    )


# ---------------------------------------------------------------------------
# Test: basic silo discovery from metadata
# ---------------------------------------------------------------------------

def test_silos_from_metadata():
    sid = _make_session(tools=["Excel", "WhatsApp", "Tally"])
    body = _post_consolidation(sid).json()

    assert body["total_silos"] >= 3
    names_lower = [s["name"].lower() for s in body["silos"]]
    assert any("excel" in n for n in names_lower)
    assert any("whatsapp" in n for n in names_lower)
    assert any("tally" in n for n in names_lower)


# ---------------------------------------------------------------------------
# Test: silo tier classification
# ---------------------------------------------------------------------------

def test_silo_tiers():
    sid = _make_session(tools=["Excel", "WhatsApp", "Salesforce"])
    body = _post_consolidation(sid).json()

    tier_map = {s["name"].lower(): s["tier"] for s in body["silos"]}
    # WhatsApp → Informal, Excel → Productivity, Salesforce → Enterprise
    assert any(t == "Informal" for t in tier_map.values())
    assert any(t == "Productivity" for t in tier_map.values())
    assert any(t == "Enterprise" for t in tier_map.values())


# ---------------------------------------------------------------------------
# Test: implicit silo detection from workflow descriptions
# ---------------------------------------------------------------------------

def test_implicit_silos_from_workflow():
    steps = [
        {"step_number": 1, "description": "Record orders in a paper logbook", "actor": "Admin", "step_type": "Manual"},
        {"step_number": 2, "description": "Transfer totals to Excel spreadsheet", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(tools=["Excel"], steps=steps)
    body = _post_consolidation(sid).json()

    names_lower = [s["name"].lower() for s in body["silos"]]
    # Should detect both explicit Excel AND implicit paper logbook
    assert any("logbook" in n or "paper" in n for n in names_lower)


# ---------------------------------------------------------------------------
# Test: data flow detection
# ---------------------------------------------------------------------------

def test_data_flow_detection():
    steps = [
        {"step_number": 1, "description": "Write daily totals in paper ledger", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 2, "description": "Transfer daily sales to Excel", "actor": "Owner", "step_type": "Manual"},
    ]
    sid = _make_session(tools=["Excel"], steps=steps)
    body = _post_consolidation(sid).json()

    assert body["manual_flows"] >= 1
    flow = body["data_flows"][0]
    assert flow["risk"] in ("High", "Medium")


# ---------------------------------------------------------------------------
# Test: redundancy detection
# ---------------------------------------------------------------------------

def test_redundancy_detection():
    steps = [
        {"step_number": 1, "description": "Record sales totals in paper ledger", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 2, "description": "Enter sales data in Excel", "actor": "Owner", "step_type": "Manual"},
    ]
    sid = _make_session(tools=["Excel"], steps=steps)
    body = _post_consolidation(sid).json()

    # Both paper ledger and Excel store Sales data → redundancy
    if body["redundancies"]:
        assert any("Sales" in r["overlapping_data"] or "Accounting" in r["overlapping_data"]
                    for r in body["redundancies"])


# ---------------------------------------------------------------------------
# Test: unified schema generated from DataFrame
# ---------------------------------------------------------------------------

def test_unified_schema_from_dataframe():
    df = pd.DataFrame({
        "order_date": ["2025-01-01", "2025-01-02"],
        "customer_name": ["Alice", "Bob"],
        "total_amount": [100.50, 250.00],
        "item_count": [3, 5],
    })
    sid = _make_session(tools=["Excel"], df=df)
    body = _post_consolidation(sid).json()

    assert len(body["unified_schemas"]) >= 1
    core = body["unified_schemas"][0]
    assert core["table_name"] == "core_transactions"
    col_names = [c["name"] for c in core["columns"]]
    assert "order_date" in col_names
    assert "total_amount" in col_names

    # Check semantic type inference
    date_col = next(c for c in core["columns"] if c["name"] == "order_date")
    assert date_col["dtype"] == "datetime"
    amount_col = next(c for c in core["columns"] if c["name"] == "total_amount")
    assert amount_col["dtype"] == "decimal"


# ---------------------------------------------------------------------------
# Test: supplementary schema tables generated
# ---------------------------------------------------------------------------

def test_supplementary_schemas():
    steps = [
        {"step_number": 1, "description": "Check inventory stock levels", "actor": "Manager", "step_type": "Manual"},
        {"step_number": 2, "description": "Process invoice from supplier", "actor": "Admin", "step_type": "Manual"},
    ]
    sid = _make_session(tools=["Excel"], steps=steps, documents_provided=["sales", "invoices", "inventory"])
    body = _post_consolidation(sid).json()

    table_names = [s["table_name"] for s in body["unified_schemas"]]
    # Should propose supplementary tables for detected data types
    assert "supplier_invoices" in table_names or "inventory_log" in table_names


# ---------------------------------------------------------------------------
# Test: migration steps ordered by urgency (informal first)
# ---------------------------------------------------------------------------

def test_migration_step_ordering():
    sid = _make_session(tools=["WhatsApp", "Paper", "Excel", "Salesforce"])
    body = _post_consolidation(sid).json()

    migrations = body["migration_steps"]
    # Should have migration steps for informal + productivity, not enterprise
    from_tools_lower = [m["from_tool"].lower() for m in migrations]
    assert any("whatsapp" in t for t in from_tools_lower)
    assert not any("salesforce" in t for t in from_tools_lower)

    # Informal should come before productivity
    informal_indices = [i for i, m in enumerate(migrations) if "whatsapp" in m["from_tool"].lower() or "paper" in m["from_tool"].lower()]
    excel_indices = [i for i, m in enumerate(migrations) if "excel" in m["from_tool"].lower()]
    if informal_indices and excel_indices:
        assert min(informal_indices) < min(excel_indices)


# ---------------------------------------------------------------------------
# Test: consolidation score — fragmented setup scores low
# ---------------------------------------------------------------------------

def test_score_fragmented():
    steps = [
        {"step_number": 1, "description": "Record in paper diary", "actor": "Admin", "step_type": "Manual"},
        {"step_number": 2, "description": "Call supplier on WhatsApp", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 3, "description": "Type into Excel", "actor": "Owner", "step_type": "Manual"},
    ]
    sid = _make_session(tools=["WhatsApp", "Excel", "Paper", "Calculator"], steps=steps)
    body = _post_consolidation(sid).json()

    assert body["consolidation_score"] < 0.50


# ---------------------------------------------------------------------------
# Test: consolidation score — enterprise setup scores high
# ---------------------------------------------------------------------------

def test_score_consolidated():
    sid = _make_session(tools=["Salesforce", "QuickBooks"])
    body = _post_consolidation(sid).json()

    assert body["consolidation_score"] >= 0.70


# ---------------------------------------------------------------------------
# Test: silo weaknesses populated
# ---------------------------------------------------------------------------

def test_silo_weaknesses():
    sid = _make_session(tools=["Paper", "WhatsApp"])
    body = _post_consolidation(sid).json()

    paper_silo = next((s for s in body["silos"] if "paper" in s["name"].lower()), None)
    assert paper_silo is not None
    assert len(paper_silo["weaknesses"]) >= 1
    assert any("backup" in w.lower() or "searchable" in w.lower() for w in paper_silo["weaknesses"])


# ---------------------------------------------------------------------------
# Test: executive summary present and specific
# ---------------------------------------------------------------------------

def test_executive_summary():
    sid = _make_session(tools=["Excel", "WhatsApp", "Paper"])
    body = _post_consolidation(sid).json()

    summary = body["executive_summary"]
    assert len(summary) > 50
    assert "informal" in summary.lower() or "fragmented" in summary.lower() or "separate" in summary.lower()


# ---------------------------------------------------------------------------
# Test: recommendations list populated
# ---------------------------------------------------------------------------

def test_recommendations():
    sid = _make_session(tools=["Excel", "WhatsApp", "Paper"])
    body = _post_consolidation(sid).json()

    assert len(body["top_recommendations"]) >= 1


# ---------------------------------------------------------------------------
# Test: 404 for missing session
# ---------------------------------------------------------------------------

def test_session_not_found():
    resp = _post_consolidation("nonexistent_session_99999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Test: 422 if no tools and no workflow
# ---------------------------------------------------------------------------

def test_no_tools_no_workflow():
    df = pd.DataFrame({"x": [1]})
    sid = session_store.create(
        raw_dataframe=df,
        workflow_text="test",
        company_metadata={"industry": "Retail", "num_employees": 1, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    resp = _post_consolidation(sid)
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: report stored back in session
# ---------------------------------------------------------------------------

def test_report_stored_in_session():
    sid = _make_session(tools=["Excel", "WhatsApp"])
    _post_consolidation(sid)

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.consolidation_report is not None
    assert entry.consolidation_report.session_id == sid


# ---------------------------------------------------------------------------
# Test: realistic restaurant scenario
# ---------------------------------------------------------------------------

def test_realistic_restaurant():
    steps = [
        {"step_number": 1, "description": "Check ingredient stocks in logbook", "actor": "Kitchen Manager", "step_type": "Manual"},
        {"step_number": 2, "description": "Record stock quantities in notebook", "actor": "Kitchen Manager", "step_type": "Manual"},
        {"step_number": 3, "description": "Call suppliers on WhatsApp to order", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 4, "description": "Store paper invoices in a folder", "actor": "Staff", "step_type": "Manual"},
        {"step_number": 5, "description": "Take table orders on handwritten notepad", "actor": "Waiter", "step_type": "Manual"},
        {"step_number": 6, "description": "Calculate customer bill on calculator", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 7, "description": "Record daily sales in paper ledger", "actor": "Cashier", "step_type": "Manual"},
        {"step_number": 8, "description": "Transfer daily totals to Excel", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 9, "description": "Calculate staff salaries in Excel", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 10, "description": "Hand paper invoices to accountant", "actor": "Owner", "step_type": "Manual"},
        {"step_number": 11, "description": "Accountant reconciles in Tally", "actor": "Accountant", "step_type": "Manual"},
    ]
    sid = _make_session(
        tools=["Excel", "WhatsApp", "Tally", "Calculator"],
        steps=steps,
        documents_provided=["sales", "invoices"],
    )
    resp = _post_consolidation(sid)
    assert resp.status_code == 200

    body = resp.json()

    # Should detect many silos (paper, WhatsApp, Excel, Tally, calculator, etc.)
    assert body["total_silos"] >= 4
    assert body["informal_silos"] >= 2

    # Should detect multiple data flows
    assert body["manual_flows"] >= 1

    # Score should be low (fragmented)
    assert body["consolidation_score"] < 0.60

    # Should have migration steps
    assert len(body["migration_steps"]) >= 2

    # Should have unified schemas (at least core_transactions + supplier_invoices)
    table_names = [s["table_name"] for s in body["unified_schemas"]]
    assert "core_transactions" in table_names

    # Recommendations should exist
    assert len(body["top_recommendations"]) >= 2

    # Should mention digitising informal tools
    recs_text = " ".join(body["top_recommendations"]).lower()
    assert "informal" in recs_text or "digitise" in recs_text or "paper" in recs_text


# ---------------------------------------------------------------------------
# Test: data types attached to silos from workflow keywords
# ---------------------------------------------------------------------------

def test_data_types_on_silos():
    steps = [
        {"step_number": 1, "description": "Record daily sales transactions", "actor": "Cashier", "step_type": "Manual", "tool_used": "Excel"},
        {"step_number": 2, "description": "Process supplier invoices", "actor": "Admin", "step_type": "Manual", "tool_used": "Tally"},
    ]
    sid = _make_session(tools=["Excel", "Tally"], steps=steps)
    body = _post_consolidation(sid).json()

    excel_silo = next((s for s in body["silos"] if "excel" in s["name"].lower()), None)
    assert excel_silo is not None
    assert "Sales" in excel_silo["data_types"]

    tally_silo = next((s for s in body["silos"] if "tally" in s["name"].lower()), None)
    assert tally_silo is not None
    assert "Invoices" in tally_silo["data_types"] or "Accounting" in tally_silo["data_types"]


# ---------------------------------------------------------------------------
# Test: actors tracked per silo
# ---------------------------------------------------------------------------

def test_actors_on_silos():
    steps = [
        {"step_number": 1, "description": "Record sales in Excel", "actor": "Cashier", "step_type": "Manual", "tool_used": "Excel"},
        {"step_number": 2, "description": "Update Excel with monthly totals", "actor": "Owner", "step_type": "Manual", "tool_used": "Excel"},
    ]
    sid = _make_session(tools=["Excel"], steps=steps)
    body = _post_consolidation(sid).json()

    excel_silo = next(s for s in body["silos"] if "excel" in s["name"].lower())
    assert "Cashier" in excel_silo["used_by"]
    assert "Owner" in excel_silo["used_by"]
