"""Tests for Module 1 — Data Ingestion."""

from __future__ import annotations

import io
import json
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas.ingestion import IssueType, WorkflowDiagram, WorkflowStep

client = TestClient(app)

SAMPLE_METADATA = json.dumps(
    {"industry": "Retail", "num_employees": 25, "tools_used": ["Excel", "WhatsApp"]}
)

WORKFLOW_TEXT = """
1. Customer places order via phone
2. Sales manager writes order in notebook
3. Admin enters order into Excel
4. Warehouse checks stock manually
5. Delivery scheduled via WhatsApp
"""

# Fake WorkflowDiagram returned by the mocked LLM so tests don't hit the API
_MOCK_DIAGRAM = WorkflowDiagram(
    steps=[
        WorkflowStep(step_number=1, description="Customer places order", actor="Customer", step_type="Manual"),
        WorkflowStep(step_number=2, description="Entry into Excel", actor="Admin", step_type="Manual", tool_used="Excel"),
    ],
    mermaid_diagram="flowchart TD\n    A[Customer places order] --> B[Admin: Entry into Excel]",
    summary="A fully manual order processing workflow with no automation.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def _excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _sample_df() -> pd.DataFrame:
    """DataFrame with intentionally messy column names (spaces, mixed case)."""
    return pd.DataFrame(
        {
            "Order ID": [1, 2, 3],
            "Customer Name": ["Alice", "Bob", "Charlie"],
            "Order Date": ["15/01/2025", "20/01/2025", "25/01/2025"],
            "Amount": [100.5, 200.0, 150.75],
        }
    )


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
# Test: Successful CSV upload — raw columns preserved, issues flagged
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_csv_success(mock_llm):
    df = _sample_df()
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    assert resp.status_code == 200
    body = resp.json()

    # Row/column counts correct
    assert body["row_count"] == 3
    assert body["column_count"] == 4
    assert body["session_id"]

    # Column NAMES preserved as-is (no renaming in Module 1)
    col_names = [c["name"] for c in body["columns"]]
    assert "Order ID" in col_names
    assert "Customer Name" in col_names

    # missing_pct present
    for col in body["columns"]:
        assert "missing_pct" in col

    # Company metadata intact
    assert body["company_metadata"]["industry"] == "Retail"


# ---------------------------------------------------------------------------
# Test: Successful Excel upload
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_excel_success(mock_llm):
    df = _sample_df()
    xlsx_data = _excel_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.xlsx", xlsx_data, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    assert resp.status_code == 200
    assert resp.json()["row_count"] == 3


# ---------------------------------------------------------------------------
# Test: Unsupported file type → 400
# ---------------------------------------------------------------------------

def test_ingest_unsupported_file_type():
    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("data.json", b'{"a":1}', "application/json")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    assert resp.status_code == 400
    assert "Unsupported file type" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Test: Missing workflow_text → 422
# ---------------------------------------------------------------------------

def test_ingest_missing_workflow_text():
    df = _sample_df()
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"company_metadata": SAMPLE_METADATA},
    )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: Inconsistent column names are flagged (not renamed)
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_flags_inconsistent_column_names(mock_llm):
    df = _sample_df()  # has "Order ID", "Customer Name" — spaces + mixed case
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    body = resp.json()
    issue_types = [i["issue_type"] for i in body["data_issues"]]
    assert "inconsistent_column_names" in issue_types

    # Columns are still the originals — NOT renamed
    col_names = [c["name"] for c in body["columns"]]
    assert "Order ID" in col_names


# ---------------------------------------------------------------------------
# Test: Missing values are flagged with severity
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_flags_missing_values(mock_llm):
    df = _sample_df()
    df.loc[0, "Amount"] = None  # introduce a missing value
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    body = resp.json()
    missing_issues = [
        i for i in body["data_issues"] if i["issue_type"] == "missing_values"
    ]
    assert missing_issues, "Expected at least one missing_values issue"
    assert missing_issues[0]["severity"] in ("low", "medium", "high")


# ---------------------------------------------------------------------------
# Test: Date-like column stored as string is flagged, NOT parsed
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_flags_unparsed_date_columns(mock_llm):
    df = _sample_df()  # "Order Date" is a string column
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    body = resp.json()

    # "Order Date" column dtype must still be object (string) — not datetime
    date_col = next(c for c in body["columns"] if c["name"] == "Order Date")
    assert "datetime" not in date_col["dtype"], "Module 1 must NOT parse dates"

    # But the issue should be flagged
    unparsed = [i for i in body["data_issues"] if i["issue_type"] == "unparsed_dates"]
    assert unparsed, "Expected unparsed_dates issue for 'Order Date' column"


# ---------------------------------------------------------------------------
# Test: Workflow analysis returned from LLM
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_workflow_analysis_returned(mock_llm):
    df = _sample_df()
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    body = resp.json()
    wa = body["workflow_analysis"]
    assert wa is not None
    assert len(wa["steps"]) == 2
    assert wa["mermaid_diagram"].startswith("flowchart")
    assert wa["summary"]


# ---------------------------------------------------------------------------
# Test: LLM failure is non-fatal — response still succeeds
# ---------------------------------------------------------------------------

@patch("app.routers.ingestion.analyse_workflow", side_effect=RuntimeError("No API key"))
def test_ingest_succeeds_without_llm(mock_llm):
    df = _sample_df()
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": SAMPLE_METADATA},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["workflow_analysis"] is None  # gracefully absent


# ---------------------------------------------------------------------------
# Test: Invalid metadata JSON → 422
# ---------------------------------------------------------------------------

def test_ingest_invalid_metadata_json():
    df = _sample_df()
    csv_data = _csv_bytes(df)

    resp = client.post(
        "/api/v1/ingest/tabular",
        files={"file": ("orders.csv", csv_data, "text/csv")},
        data={"workflow_text": WORKFLOW_TEXT, "company_metadata": "not-json"},
    )

    assert resp.status_code == 422
