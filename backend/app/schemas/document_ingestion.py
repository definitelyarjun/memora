"""Pydantic models for the Document Ingestion endpoint."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.schemas.ingestion import CompanyMetadata, WorkflowDiagram


DocumentType = Literal["sop", "invoice", "ledger", "other"]


# ---------------------------------------------------------------------------
# Invoice extraction models (populated via Gemini multimodal for PDF invoices)
# ---------------------------------------------------------------------------

class LineItem(BaseModel):
    """A single line item on an invoice."""

    description: str = ""
    quantity: float | None = None
    unit: str | None = None
    rate: float | None = None
    amount: float | None = None


class InvoiceData(BaseModel):
    """Structured invoice data extracted by Gemini from a PDF invoice/ledger."""

    invoice_number: str | None = None
    invoice_date: str | None = None
    seller_name: str | None = None
    seller_gstin: str | None = None
    buyer_name: str | None = None
    buyer_gstin: str | None = None
    line_items: list[LineItem] = Field(default_factory=list)
    subtotal: float | None = None
    tax_amount: float | None = None
    total_amount: float | None = None
    currency: str = "INR"
    raw_extraction_notes: str | None = Field(
        None,
        description="Any caveats or uncertainties flagged by Gemini during extraction",
    )


class ExtractedTable(BaseModel):
    """A single table extracted from a PDF page."""

    page: int
    row_count: int
    column_count: int
    headers: list[str]
    preview_rows: list[list[str]] = Field(
        default_factory=list,
        description="First 3 rows for preview purposes",
    )


class DocumentIngestionResponse(BaseModel):
    """Response returned after a successful document ingestion run."""

    session_id: str = Field(
        ..., description="Pass this to subsequent module endpoints."
    )
    filename: str
    file_type: str = Field(..., description="Detected extension: .pdf, .docx, or .txt")
    document_type: DocumentType = Field(
        ..., description="What kind of document was uploaded"
    )
    page_count: int | None = Field(None, description="Page count (PDFs only)")
    is_scanned: bool = Field(
        False,
        description="True if the PDF appears to be a scanned/image-only document",
    )
    extracted_text: str = Field(
        ..., description="Full extracted text content of the document"
    )
    extracted_tables: list[ExtractedTable] = Field(
        default_factory=list,
        description="Tables found in the document (PDFs only)",
    )
    word_count: int
    company_metadata: CompanyMetadata
    workflow_analysis: WorkflowDiagram | None = Field(
        None,
        description="LLM-generated workflow structure — populated for SOP documents",
    )
    invoice_data: InvoiceData | None = Field(
        None,
        description="Structured invoice fields extracted by Gemini — populated for invoice/ledger PDFs",
    )
    warnings: list[str] = Field(default_factory=list)
