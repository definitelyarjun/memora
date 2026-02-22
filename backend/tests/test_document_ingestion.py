"""Tests for Document Ingestion endpoint — POST /api/v1/ingest/document."""

from __future__ import annotations

import io
import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.schemas.ingestion import WorkflowDiagram, WorkflowStep
from app.schemas.document_ingestion import InvoiceData, LineItem

client = TestClient(app)

SAMPLE_METADATA = json.dumps(
    {"industry": "Retail", "num_employees": 25, "tools_used": ["Tally", "WhatsApp"]}
)

SOP_TEXT = b"""Order Processing SOP

1. Customer calls or WhatsApps with order details.
2. Sales Manager checks stock availability manually in the godown.
3. If stock available, order is confirmed verbally.
4. Admin writes the order in the register and later enters it into Excel.
5. Accountant enters the invoice details into Tally the next morning.
6. Delivery is arranged via WhatsApp to the driver.
"""

_MOCK_DIAGRAM = WorkflowDiagram(
    steps=[
        WorkflowStep(step_number=1, description="Customer places order", actor="Customer", step_type="Manual"),
        WorkflowStep(step_number=2, description="Stock check", actor="Sales Manager", step_type="Manual"),
    ],
    mermaid_diagram="flowchart TD\n    A[Customer order] --> B[Sales Manager: stock check]",
    summary="Fully manual order processing with no automation.",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _docx_bytes(text: str) -> bytes:
    """Create a minimal .docx file in memory containing the given text."""
    from docx import Document
    buf = io.BytesIO()
    doc = Document()
    for line in text.strip().splitlines():
        doc.add_paragraph(line)
    doc.save(buf)
    return buf.getvalue()


def _post_document(file_bytes: bytes, filename: str, content_type: str, doc_type: str) -> dict:
    resp = client.post(
        "/api/v1/ingest/document",
        files={"file": (filename, file_bytes, content_type)},
        data={"document_type": doc_type, "company_metadata": SAMPLE_METADATA},
    )
    return resp


# ---------------------------------------------------------------------------
# Test: TXT SOP upload — workflow analysis triggered
# ---------------------------------------------------------------------------

@patch("app.routers.document_ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_txt_sop(mock_llm):
    resp = _post_document(SOP_TEXT, "sop.txt", "text/plain", "sop")

    assert resp.status_code == 200
    body = resp.json()

    assert body["session_id"]
    assert body["file_type"] == "txt"
    assert body["document_type"] == "sop"
    assert body["is_scanned"] is False
    assert body["page_count"] is None
    assert "Customer" in body["extracted_text"] or "order" in body["extracted_text"].lower()
    assert body["word_count"] > 0

    # LLM should have been called and result returned
    wa = body["workflow_analysis"]
    assert wa is not None
    assert wa["mermaid_diagram"].startswith("flowchart")
    mock_llm.assert_called_once()


# ---------------------------------------------------------------------------
# Test: DOCX SOP upload — text extracted, workflow generated
# ---------------------------------------------------------------------------

@patch("app.routers.document_ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_docx_sop(mock_llm):
    docx_data = _docx_bytes(SOP_TEXT.decode())
    resp = _post_document(
        docx_data,
        "process.docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "sop",
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["file_type"] == "docx"
    assert body["extracted_text"].strip()
    assert body["workflow_analysis"] is not None


# ---------------------------------------------------------------------------
# Test: Invoice TXT — LLM NOT called (non-SOP document)
# ---------------------------------------------------------------------------

@patch("app.routers.document_ingestion.analyse_workflow", return_value=_MOCK_DIAGRAM)
def test_ingest_invoice_no_llm(mock_llm):
    invoice_text = b"Invoice #1234\nDate: 01/01/2025\nTotal: 50000\nGST: 9000"
    resp = _post_document(invoice_text, "invoice.txt", "text/plain", "invoice")

    assert resp.status_code == 200
    body = resp.json()
    assert body["document_type"] == "invoice"
    assert body["workflow_analysis"] is None  # LLM not called for invoices
    mock_llm.assert_not_called()


# ---------------------------------------------------------------------------
# Test: Unsupported file type → 400
# ---------------------------------------------------------------------------

def test_ingest_document_unsupported_type():
    resp = client.post(
        "/api/v1/ingest/document",
        files={"file": ("data.xlsx", b"fake", "application/vnd.ms-excel")},
        data={"document_type": "other", "company_metadata": SAMPLE_METADATA},
    )
    assert resp.status_code == 400
    assert "Unsupported document type" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Test: Missing document_type → 422
# ---------------------------------------------------------------------------

def test_ingest_document_missing_type():
    resp = client.post(
        "/api/v1/ingest/document",
        files={"file": ("sop.txt", SOP_TEXT, "text/plain")},
        data={"company_metadata": SAMPLE_METADATA},
        # document_type intentionally omitted
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: LLM failure is non-fatal for document endpoint
# ---------------------------------------------------------------------------

@patch("app.routers.document_ingestion.analyse_workflow", side_effect=RuntimeError("No key"))
def test_ingest_document_llm_failure_non_fatal(mock_llm):
    resp = _post_document(SOP_TEXT, "sop.txt", "text/plain", "sop")

    assert resp.status_code == 200
    body = resp.json()
    assert body["workflow_analysis"] is None
    assert any("skipped" in w.lower() or "failed" in w.lower() for w in body["warnings"])


# ---------------------------------------------------------------------------
# Test: Invalid metadata JSON → 422
# ---------------------------------------------------------------------------

def test_ingest_document_invalid_metadata():
    resp = client.post(
        "/api/v1/ingest/document",
        files={"file": ("sop.txt", SOP_TEXT, "text/plain")},
        data={"document_type": "sop", "company_metadata": "not-valid-json"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Test: PDF invoice — Gemini Vision extracts structured invoice data
# ---------------------------------------------------------------------------

_MOCK_INVOICE_DATA = InvoiceData(
    invoice_number="INV-2025-001",
    invoice_date="15/01/2025",
    seller_name="ABC Suppliers Pvt Ltd",
    seller_gstin="27AABCU9603R1ZX",
    buyer_name="XYZ Retail",
    buyer_gstin="27AABCU1234R1ZY",
    line_items=[
        LineItem(description="Widget A", quantity=10, unit="pcs", rate=500.0, amount=5000.0),
        LineItem(description="Widget B", quantity=5, unit="pcs", rate=800.0, amount=4000.0),
    ],
    subtotal=9000.0,
    tax_amount=1620.0,
    total_amount=10620.0,
    currency="INR",
    raw_extraction_notes=None,
)

_FAKE_PDF_BYTES = b"%PDF-1.4 fake-pdf-content"
_FAKE_PAGE_IMAGES = [b"\x89PNG\r\nfake-png-image"]


@patch("app.routers.document_ingestion.analyse_invoice_pdf", return_value=_MOCK_INVOICE_DATA)
@patch("app.routers.document_ingestion.render_pdf_pages", return_value=_FAKE_PAGE_IMAGES)
@patch(
    "app.routers.document_ingestion.process_document",
    return_value=("Invoice #INV-2025-001", [], 1, False, []),
)
def test_ingest_pdf_invoice_gemini(mock_process, mock_render, mock_extract):
    """PDF invoice triggers Gemini Vision extraction; invoice_data returned."""
    resp = _post_document(_FAKE_PDF_BYTES, "invoice.pdf", "application/pdf", "invoice")

    assert resp.status_code == 200
    body = resp.json()

    assert body["document_type"] == "invoice"
    assert body["workflow_analysis"] is None  # workflow NOT generated for invoices

    inv = body["invoice_data"]
    assert inv is not None
    assert inv["invoice_number"] == "INV-2025-001"
    assert inv["seller_name"] == "ABC Suppliers Pvt Ltd"
    assert inv["total_amount"] == 10620.0
    assert len(inv["line_items"]) == 2
    assert inv["line_items"][0]["description"] == "Widget A"

    mock_render.assert_called_once()
    mock_extract.assert_called_once()


@patch("app.routers.document_ingestion.analyse_invoice_pdf", side_effect=RuntimeError("No key"))
@patch("app.routers.document_ingestion.render_pdf_pages", return_value=_FAKE_PAGE_IMAGES)
@patch(
    "app.routers.document_ingestion.process_document",
    return_value=("Invoice #INV-2025-001", [], 1, False, []),
)
def test_ingest_pdf_invoice_llm_failure_non_fatal(mock_process, mock_render, mock_extract):
    """Gemini Vision failure for invoice PDF is non-fatal; returns 200 with warning."""
    resp = _post_document(_FAKE_PDF_BYTES, "invoice.pdf", "application/pdf", "invoice")

    assert resp.status_code == 200
    body = resp.json()

    assert body["invoice_data"] is None
    assert any("skipped" in w.lower() or "failed" in w.lower() for w in body["warnings"])


@patch("app.routers.document_ingestion.analyse_invoice_pdf", return_value=_MOCK_INVOICE_DATA)
@patch("app.routers.document_ingestion.render_pdf_pages", return_value=_FAKE_PAGE_IMAGES)
@patch(
    "app.routers.document_ingestion.process_document",
    return_value=("", [], 1, True, ["Scanned PDF detected."]),
)
def test_ingest_pdf_invoice_scanned_skips_gemini(mock_process, mock_render, mock_extract):
    """Scanned invoice PDF skips Gemini Vision (no text-renderable content)."""
    resp = _post_document(_FAKE_PDF_BYTES, "invoice.pdf", "application/pdf", "invoice")

    assert resp.status_code == 200
    body = resp.json()

    # Scanned PDF → is_scanned=True → Gemini path not triggered
    assert body["is_scanned"] is True
    assert body["invoice_data"] is None
    mock_render.assert_not_called()
    mock_extract.assert_not_called()
