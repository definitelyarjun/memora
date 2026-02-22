"""Tests for Module 2 — Data Quality & DPDP Compliance Scanner.

FoundationIQ 3.0 (Startup Edition)

Scoring dimensions (7) with new weights:
  Completeness              25 %
  Deduplication             20 %
  Consistency               15 %
  Structural Integrity      10 %
  Process Digitisation      15 %   (from workflow_analysis)
  Tool Maturity              5 %   (from company_metadata.tools_used)
  Data Coverage             10 %   (based on startup CSV files uploaded)

Data Coverage points: sales_inquiries=0.40, expenses=0.35, org_chart=0.25.
Default (no docs) = 0.00.

PII Detection: email, phone, credit_card, aadhaar, pan, ip_address.
DPDP Risk Levels: Critical (>3 PII cols), High (2-3), Medium (1), Low (0).
Quality Pass threshold: >= 0.85.
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
    *,
    org_chart_df: pd.DataFrame | None = None,
    expenses_df: pd.DataFrame | None = None,
    sales_inquiries_df: pd.DataFrame | None = None,
) -> str:
    """Create a session for testing.

    ``df`` is stored as ``raw_dataframe`` for backward compat.
    Use keyword args to also populate startup-specific DataFrames.
    """
    return session_store.create(
        raw_dataframe=df,
        org_chart_df=org_chart_df,
        expenses_df=expenses_df,
        sales_inquiries_df=sales_inquiries_df,
        workflow_text="test workflow",
        company_metadata={
            "industry": "SaaS",
            "num_employees": 10,
            "tools_used": tools or [],
        },
        data_issues=issues or [],
        workflow_analysis=_workflow(workflow_steps) if workflow_steps is not None else None,
        documents_provided=documents_provided,
        supplementary_doc_stats=supplementary_doc_stats,
    )


def _post_quality(session_id: str):
    return client.post(
        "/api/v1/analyze/quality",
        data={"session_id": session_id},
    )


# ===================================================================
# Data Quality Dimension Tests
# ===================================================================


def test_quality_perfect_all_dimensions():
    """Perfect data + fully automated + enterprise tools -> High."""
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
    sid = _make_session(
        df,
        tools=["Shopify", "QuickBooks", "Slack"],
        workflow_steps=steps,
        documents_provided=["sales_inquiries", "expenses", "org_chart"],
    )
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()

    # volume penalty for a 3-row test dataframe (< 20 rows -> volume score 0.05)
    assert body["completeness_score"] >= 0.60
    assert body["completeness_score"] < 0.75
    assert body["deduplication_score"] == 1.0
    assert body["process_digitisation_score"] == 1.0
    assert body["tool_maturity_score"] == 1.0
    assert body["data_coverage_score"] == pytest.approx(1.0)
    assert body["readiness_level"] in ("High", "Moderate")
    assert body["total_workflow_steps"] == 3
    assert body["automated_steps"] == 3
    assert body["manual_steps"] == 0
    # New fields
    assert "data_quality_score" in body
    assert "quality_pass" in body
    assert "dpdp_compliance" in body
    assert body["dpdp_compliance"]["risk_level"] == "Low"


def test_quality_all_manual_low_tools():
    """Perfect data but ALL manual + low tools -> score drops hard."""
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

    assert body["completeness_score"] < 0.75
    assert body["process_digitisation_score"] == 0.0
    assert body["tool_maturity_score"] <= 0.20
    assert body["manual_steps"] == 4
    assert body["automated_steps"] == 0
    assert body["data_quality_score"] < 0.65
    assert body["readiness_level"] in ("Low", "Moderate", "Critical")


def test_quality_mixed_workflow():
    """Mixed workflow (some automated, some manual)."""
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


def test_quality_no_workflow():
    """No workflow analysis -> pessimistic default (0.10)."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df, tools=["Excel"])
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["process_digitisation_score"] == 0.10
    assert body["total_workflow_steps"] == 0


def test_quality_no_tools():
    """No tools -> pessimistic default (0.05)."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    steps = [{"step_number": 1, "description": "Do something", "actor": "Staff", "step_type": "Automated"}]
    sid = _make_session(df, tools=[], workflow_steps=steps)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["tool_maturity_score"] == 0.05
    assert body["tools_detected"] == []


def test_quality_decision_steps():
    """Decision steps get partial credit."""
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


def test_quality_missing_values():
    """Missing values reduce completeness."""
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
    assert body["completeness_score"] < 0.55
    assert body["completeness_score"] > 0.30


def test_quality_duplicates():
    """Duplicates reduce deduplication score."""
    df = pd.DataFrame({"id": [1, 1, 2, 3, 3], "value": [10, 10, 20, 30, 30]})
    sid = _make_session(df)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    body = resp.json()
    assert body["duplicate_rows"] == 2
    assert body["deduplication_score"] < 1.0


def test_quality_inconsistent_columns():
    """Inconsistent columns reduce consistency."""
    df = pd.DataFrame({"Order Date": ["2025-01-01"], "CustomerName": ["Alice"], "total amount": [500.0]})
    issues = [DataIssue(issue_type=IssueType.INCONSISTENT_COLUMN_NAMES, column=None,
                        description="3 bad", affected_count=3, severity="medium")]
    sid = _make_session(df, issues)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    assert resp.json()["consistency_score"] < 1.0


def test_quality_unparsed_dates():
    """Unparsed dates reduce structural integrity."""
    df = pd.DataFrame({"order_date": ["01/01/2025", "02/01/2025"], "amount": [100.0, 200.0]})
    issues = [DataIssue(issue_type=IssueType.UNPARSED_DATES, column="order_date",
                        description="date as text", affected_count=None, severity="medium")]
    sid = _make_session(df, issues)
    resp = _post_quality(sid)

    assert resp.status_code == 200
    assert resp.json()["structural_integrity_score"] < 1.0


# ===================================================================
# Session & Error Tests
# ===================================================================


def test_quality_stored_in_session():
    """Report stored back in session."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df)
    _post_quality(sid)

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.quality_report is not None
    assert entry.quality_report.session_id == sid
    assert hasattr(entry.quality_report, "dpdp_compliance")


def test_quality_session_not_found():
    """404 for unknown session."""
    resp = _post_quality("nonexistent_session_id_12345")
    assert resp.status_code == 404


def test_quality_document_session_rejected():
    """Document-only session -> 422."""
    sid = session_store.create(
        raw_dataframe=None,
        workflow_text="some sop text",
        company_metadata={"industry": "SaaS", "num_employees": 5, "tools_used": []},
        data_issues=[],
        workflow_analysis=None,
    )
    resp = _post_quality(sid)
    assert resp.status_code == 422


# ===================================================================
# Tool Tier Tests
# ===================================================================


def test_tool_tiers():
    """Enterprise tools score higher than informal tools."""
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


# ===================================================================
# Realistic Scenario Tests
# ===================================================================


def test_realistic_startup_scenario():
    """Realistic startup with mixed data quality -> Not High."""
    df = pd.DataFrame({
        "lead_id": range(1, 26),
        "Contact Name": ["Name"] * 22 + [None, None, None],
        "amount": [100.0] * 24 + [None],
    })
    issues = [
        DataIssue(issue_type=IssueType.MISSING_VALUES, column="Contact Name",
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
    assert body["readiness_level"] in ("Low", "Moderate", "Critical")
    assert body["data_quality_score"] < 0.70
    assert body["process_digitisation_score"] == 0.0
    recs_text = " ".join(body["top_recommendations"]).lower()
    assert "manual" in recs_text


def test_readiness_critical():
    """All dimensions bad -> Critical."""
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
    assert body["data_quality_score"] < 0.40


# ===================================================================
# Volume & Deduplication Tests
# ===================================================================


def test_volume_scoring():
    """Volume scoring penalises small datasets."""
    df_tiny  = pd.DataFrame({"x": [1, 2, 3]})
    df_large = pd.DataFrame({"x": range(2000)})

    body_tiny  = _post_quality(_make_session(df_tiny)).json()
    body_large = _post_quality(_make_session(df_large)).json()

    assert body_large["completeness_score"] > body_tiny["completeness_score"]
    assert body_tiny["completeness_score"]  < 0.75
    assert body_large["completeness_score"] >= 0.85
    assert body_large["missing_cells"] == 0
    assert body_tiny["missing_cells"] == 0


def test_fuzzy_deduplication():
    """Fuzzy deduplication catches case/whitespace variants."""
    df = pd.DataFrame({
        "name":   ["Alice", "alice", "Alice ", "Bob"],
        "amount": [100.0,   100.0,   100.0,    200.0],
    })
    sid  = _make_session(df)
    body = _post_quality(sid).json()

    assert body["duplicate_rows"] == 0
    assert body["deduplication_score"] < 1.0


# ===================================================================
# Data Coverage Tests (Startup Edition)
# ===================================================================


def test_data_coverage_all_csvs():
    """All 3 startup CSVs uploaded -> 1.0."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(
        df, documents_provided=["sales_inquiries", "expenses", "org_chart"],
    )
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(1.0)


def test_data_coverage_single_csv():
    """Only sales_inquiries -> 0.40."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df, documents_provided=["sales_inquiries"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.40)


def test_data_coverage_two_csvs():
    """sales_inquiries + expenses -> 0.75."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df, documents_provided=["sales_inquiries", "expenses"])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.75)


def test_data_coverage_empty():
    """No documents provided -> 0.0."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df, documents_provided=[])
    body = _post_quality(sid).json()
    assert body["data_coverage_score"] == pytest.approx(0.0)


def test_content_aware_coverage():
    """Row count affects coverage score for supplementary CSVs."""
    df = pd.DataFrame({"x": [1, 2, 3]})

    # Substantial expenses file (≥ 50 rows) -> full base points
    sid_rich = _make_session(
        df,
        documents_provided=["sales_inquiries", "expenses"],
        supplementary_doc_stats={"expenses": {"readable": True, "row_count": 100}},
    )
    body_rich = _post_quality(sid_rich).json()

    # Nearly-empty expenses file (< 10 rows) -> only 20% of base points
    sid_empty = _make_session(
        df,
        documents_provided=["sales_inquiries", "expenses"],
        supplementary_doc_stats={"expenses": {"readable": True, "row_count": 3}},
    )
    body_empty = _post_quality(sid_empty).json()

    # Unreadable/corrupt file -> only 30% of base points
    sid_bad = _make_session(
        df,
        documents_provided=["sales_inquiries", "expenses"],
        supplementary_doc_stats={"expenses": {"readable": False, "reason": "corrupt"}},
    )
    body_bad = _post_quality(sid_bad).json()

    assert body_rich["data_coverage_score"] > body_empty["data_coverage_score"]
    assert body_rich["data_coverage_score"] > body_bad["data_coverage_score"]
    # 0.40 (sales_inquiries) + 0.35 (expenses full) = 0.75
    assert body_rich["data_coverage_score"]  == pytest.approx(0.75)
    # 0.40 (sales_inquiries) + 0.35*0.20 (nearly empty) = 0.47
    assert body_empty["data_coverage_score"] == pytest.approx(0.47)
    # 0.40 (sales_inquiries) + 0.35*0.30 (corrupt) = 0.505
    assert body_bad["data_coverage_score"]   == pytest.approx(0.505)


# ===================================================================
# PII Detection Tests (DPDP Compliance)
# ===================================================================


def test_pii_email_detection():
    """Detects email addresses in string columns."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "contact": ["alice@example.com", "bob@company.org", "charlie@test.net"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    assert dpdp["total_pii_columns"] >= 1
    findings = dpdp["pii_findings"]
    email_findings = [f for f in findings if f["pii_type"] == "email"]
    assert len(email_findings) >= 1
    assert email_findings[0]["column"] == "contact"
    assert email_findings[0]["sample_count"] == 3
    assert email_findings[0]["exposure_pct"] == 100.0
    assert email_findings[0]["risk_level"] == "High"


def test_pii_phone_detection():
    """Detects Indian phone numbers."""
    df = pd.DataFrame({
        "customer": ["A", "B", "C"],
        "phone": ["+91 9876543210", "9123456789", "8765432100"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    phone_findings = [f for f in dpdp["pii_findings"] if f["pii_type"] == "phone"]
    assert len(phone_findings) >= 1
    assert phone_findings[0]["column"] == "phone"
    assert phone_findings[0]["sample_count"] >= 2


def test_pii_aadhaar_detection():
    """Detects Aadhaar numbers (12-digit with optional spaces)."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "id_number": ["2345 6789 0123", "3456 7890 1234"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    aadhaar_findings = [f for f in dpdp["pii_findings"] if f["pii_type"] == "aadhaar"]
    assert len(aadhaar_findings) >= 1
    assert aadhaar_findings[0]["column"] == "id_number"
    assert aadhaar_findings[0]["sample_count"] == 2


def test_pii_pan_detection():
    """Detects PAN numbers (ABCDE1234F format)."""
    df = pd.DataFrame({
        "employee": ["Alice", "Bob"],
        "pan": ["ABCDE1234F", "ZYXWV9876A"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    pan_findings = [f for f in dpdp["pii_findings"] if f["pii_type"] == "pan"]
    assert len(pan_findings) >= 1
    assert pan_findings[0]["column"] == "pan"
    assert pan_findings[0]["sample_count"] == 2


def test_pii_credit_card_detection():
    """Detects credit card numbers."""
    df = pd.DataFrame({
        "customer": ["A", "B"],
        "card_number": ["4111111111111111", "5500000000000004"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    cc_findings = [f for f in dpdp["pii_findings"] if f["pii_type"] == "credit_card"]
    assert len(cc_findings) >= 1
    assert cc_findings[0]["column"] == "card_number"


def test_pii_ip_address_detection():
    """Detects IPv4 addresses."""
    df = pd.DataFrame({
        "user": ["A", "B", "C"],
        "login_ip": ["192.168.1.1", "10.0.0.255", "172.16.0.1"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    ip_findings = [f for f in dpdp["pii_findings"] if f["pii_type"] == "ip_address"]
    assert len(ip_findings) >= 1
    assert ip_findings[0]["column"] == "login_ip"
    assert ip_findings[0]["sample_count"] == 3


def test_no_pii_clean_data():
    """Clean data with no PII -> Low risk, LLM-safe."""
    df = pd.DataFrame({
        "product": ["Widget A", "Widget B", "Widget C"],
        "quantity": [10, 20, 30],
        "price": [100.0, 200.0, 300.0],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    assert dpdp["risk_level"] == "Low"
    assert dpdp["total_pii_columns"] == 0
    assert dpdp["total_pii_values"] == 0
    assert dpdp["llm_api_safe"] is True
    assert len(dpdp["pii_findings"]) == 0


# ===================================================================
# DPDP Risk Level Tests
# ===================================================================


def test_dpdp_risk_critical():
    """PII in >3 columns -> Critical risk."""
    df = pd.DataFrame({
        "email": ["a@test.com", "b@test.com"],
        "phone": ["+91 9876543210", "9123456789"],
        "aadhaar": ["2345 6789 0123", "3456 7890 1234"],
        "pan": ["ABCDE1234F", "ZYXWV9876A"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    assert dpdp["risk_level"] == "Critical"
    assert dpdp["total_pii_columns"] == 4
    assert dpdp["llm_api_safe"] is False


def test_dpdp_risk_high():
    """PII in 2-3 columns -> High risk."""
    df = pd.DataFrame({
        "email": ["a@test.com", "b@test.com"],
        "phone": ["+91 9876543210", "9123456789"],
        "product": ["Widget A", "Widget B"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    assert dpdp["risk_level"] == "High"
    assert dpdp["total_pii_columns"] == 2


def test_dpdp_risk_medium():
    """PII in exactly 1 column -> Medium risk."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "email": ["alice@example.com", "bob@example.com"],
        "amount": [100.0, 200.0],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    dpdp = body["dpdp_compliance"]
    assert dpdp["risk_level"] == "Medium"
    assert dpdp["total_pii_columns"] == 1


# ===================================================================
# Quality Pass & LLM Safety Tests
# ===================================================================


def test_quality_pass_above_threshold():
    """Data quality score >= 0.85 -> quality_pass = True."""
    df = pd.DataFrame({"x": range(5000)})  # large dataset, no issues
    steps = [
        {"step_number": 1, "description": "auto", "actor": "System", "step_type": "Automated"},
        {"step_number": 2, "description": "auto", "actor": "System", "step_type": "Automated"},
    ]
    sid = _make_session(
        df,
        tools=["Salesforce", "QuickBooks"],
        workflow_steps=steps,
        documents_provided=["sales_inquiries", "expenses", "org_chart"],
    )
    body = _post_quality(sid).json()

    assert body["data_quality_score"] >= 0.85
    assert body["quality_pass"] is True


def test_quality_pass_below_threshold():
    """Data quality score < 0.85 -> quality_pass = False."""
    df = pd.DataFrame({"x": [1, 2, 3]})  # tiny dataset, no tools, no workflow
    sid = _make_session(df, tools=[], documents_provided=[])
    body = _post_quality(sid).json()

    assert body["data_quality_score"] < 0.85
    assert body["quality_pass"] is False


def test_llm_api_safe_with_critical_pii():
    """Aadhaar data -> llm_api_safe = False."""
    df = pd.DataFrame({
        "employee": ["Alice", "Bob"],
        "aadhaar": ["2345 6789 0123", "3456 7890 1234"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    assert body["dpdp_compliance"]["llm_api_safe"] is False


def test_llm_api_safe_with_credit_card():
    """Credit card data -> llm_api_safe = False."""
    df = pd.DataFrame({
        "customer": ["Alice"],
        "card": ["4111111111111111"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    assert body["dpdp_compliance"]["llm_api_safe"] is False


# ===================================================================
# Column Quality with PII Annotations
# ===================================================================


def test_pii_types_in_column_quality():
    """Column quality includes pii_types annotation."""
    df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "email": ["alice@example.com", "bob@example.com"],
        "amount": [100.0, 200.0],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    col_map = {c["name"]: c for c in body["column_quality"]}
    assert "email" in col_map["email"].get("pii_types", [])
    assert col_map["amount"].get("pii_types", []) == []


# ===================================================================
# Multi-DataFrame Scan Tests
# ===================================================================


def test_multi_dataframe_quality():
    """Multiple startup DataFrames are all scanned for quality and PII."""
    org_df = pd.DataFrame({
        "name": ["Alice", "Bob"],
        "email": ["alice@co.com", "bob@co.com"],
        "role": ["CEO", "CTO"],
    })
    expenses_df = pd.DataFrame({
        "date": ["2025-01-01", "2025-01-02"],
        "amount": [5000.0, 3000.0],
        "vendor": ["AWS", "Figma"],
    })
    sales_df = pd.DataFrame({
        "lead": ["Company A", "Company B"],
        "phone": ["+91 9876543210", "9123456789"],
        "value": [50000.0, 75000.0],
    })

    sid = session_store.create(
        org_chart_df=org_df,
        expenses_df=expenses_df,
        sales_inquiries_df=sales_df,
        company_metadata={"tools_used": ["Salesforce"]},
        data_issues=[],
    )
    body = _post_quality(sid).json()

    assert body["row_count"] > 0
    dpdp = body["dpdp_compliance"]
    # Should find email in org_chart and phone in sales_inquiries
    pii_types_found = {f["pii_type"] for f in dpdp["pii_findings"]}
    assert "email" in pii_types_found
    assert "phone" in pii_types_found
    assert dpdp["total_pii_columns"] >= 2


# ===================================================================
# Compliance Warnings in Recommendations
# ===================================================================


def test_dpdp_warnings_in_recommendations():
    """DPDP warnings appear in top_recommendations when PII is High/Critical."""
    df = pd.DataFrame({
        "email": ["a@b.com", "c@d.com"],
        "phone": ["+91 9876543210", "9123456789"],
        "aadhaar": ["2345 6789 0123", "3456 7890 1234"],
        "pan": ["ABCDE1234F", "ZYXWV9876A"],
    })
    sid = _make_session(df)
    body = _post_quality(sid).json()

    recs_text = " ".join(body["top_recommendations"]).lower()
    assert "dpdp" in recs_text or "anonymise" in recs_text or "pii" in recs_text


def test_backward_compat_ai_readiness_score():
    """ai_readiness_score is an alias for data_quality_score."""
    df = pd.DataFrame({"x": [1, 2, 3]})
    sid = _make_session(df)
    body = _post_quality(sid).json()

    assert body["ai_readiness_score"] == body["data_quality_score"]
