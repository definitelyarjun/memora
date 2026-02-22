"""API router for Document Ingestion.

Endpoint:
    POST /api/v1/ingest/document
        - Accepts a document file (.pdf, .docx, .txt)
        - Document types: sop, invoice, ledger, other
        - Accepts company_metadata as a JSON string (form field)
        - Extracts text (and tables for PDFs)
        - For SOP documents: sends text to Gemini for workflow analysis
        - Returns DocumentIngestionResponse with session_id

What to upload here vs /ingest/tabular:

    /ingest/tabular  →  Sales Register.xlsx, Inventory.csv, Tally export.csv
    /ingest/document →  Invoice.pdf, SOP.docx, Process description.txt, GST bill.pdf
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.core.session_store import session_store
from app.schemas.document_ingestion import DocumentIngestionResponse, DocumentType
from app.schemas.ingestion import CompanyMetadata
from app.services.document_ingestion import DocumentIngestionError, process_document, render_pdf_pages
from app.services.llm import analyse_workflow, analyse_invoice_pdf

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["Ingestion — Documents"])


@router.post("/document", response_model=DocumentIngestionResponse)
async def ingest_document(
    file: UploadFile = File(
        ...,
        description="PDF, Word (.docx), or plain text (.txt) document",
    ),
    document_type: DocumentType = Form(
        ...,
        description=(
            "sop — Standard Operating Procedure / workflow description\n"
            "invoice — Invoice, GST bill, receipt\n"
            "ledger — Tally or accounting report in document form\n"
            "other — Anything else"
        ),
    ),
    company_metadata: str = Form(
        ...,
        description='JSON string, e.g. {"industry":"Retail","num_employees":50,"tools_used":["Tally","WhatsApp"]}',
    ),
) -> DocumentIngestionResponse:
    """Extract and analyse a business document.

    - .txt / .docx → full text extracted
    - .pdf (digital) → text + tables extracted
    - .pdf (scanned) → flagged as unreadable, OCR not supported
    - SOP documents → LLM generates structured workflow + Mermaid diagram
    - Invoice/ledger PDFs → Gemini Vision extracts structured invoice fields
    - Other → text extracted only, no LLM call
    """

    # --- Parse & validate metadata ----------------------------------------
    try:
        meta_dict = json.loads(company_metadata)
        meta = CompanyMetadata(**meta_dict)
    except (json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid company_metadata JSON: {exc}",
        )

    # --- Read file bytes --------------------------------------------------
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # --- Extract document content -----------------------------------------
    try:
        extracted_text, tables, page_count, is_scanned, warnings = process_document(
            file_content=content,
            filename=file.filename or "unknown",
        )
    except DocumentIngestionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # --- LLM analysis --------------------------------------------------------
    workflow_analysis = None
    invoice_data = None

    file_ext = (file.filename or "").rsplit(".", 1)[-1].lower() if file.filename and "." in (file.filename or "") else ""

    # SOP: text-based workflow analysis
    if document_type == "sop" and extracted_text.strip():
        try:
            workflow_analysis = analyse_workflow(extracted_text)
        except RuntimeError as exc:
            logger.warning("LLM analysis skipped: %s", exc)
            warnings.append("Workflow analysis skipped — GEMINI_API_KEY not set.")
        except Exception as exc:
            logger.error("LLM analysis failed: %s", exc)
            warnings.append(f"Workflow analysis failed: {exc}")

    elif document_type == "sop" and not extracted_text.strip():
        warnings.append(
            "Document type is 'sop' but no text could be extracted. "
            "Workflow analysis was skipped."
        )

    # Invoice / ledger PDF: multimodal extraction via Gemini Vision
    elif document_type in ("invoice", "ledger") and file_ext == "pdf" and not is_scanned:
        try:
            page_images = render_pdf_pages(content)
            invoice_data = analyse_invoice_pdf(page_images)
        except RuntimeError as exc:
            logger.warning("Invoice extraction skipped: %s", exc)
            warnings.append("Invoice extraction skipped — GEMINI_API_KEY not set.")
        except Exception as exc:
            logger.error("Invoice extraction failed: %s", exc)
            warnings.append(f"Invoice extraction failed: {exc}")

    # --- Store in session -------------------------------------------------
    session_id = session_store.create(
        raw_dataframe=None,           # no tabular data for documents
        workflow_text=extracted_text,
        company_metadata=meta.model_dump(),
        data_issues=[],
        workflow_analysis=workflow_analysis,
    )

    return DocumentIngestionResponse(
        session_id=session_id,
        filename=file.filename or "unknown",
        file_type=file.filename.rsplit(".", 1)[-1].lower() if file.filename and "." in file.filename else "unknown",
        document_type=document_type,
        page_count=page_count,
        is_scanned=is_scanned,
        extracted_text=extracted_text,
        extracted_tables=tables,
        word_count=len(extracted_text.split()),
        company_metadata=meta,
        workflow_analysis=workflow_analysis,
        invoice_data=invoice_data,
        warnings=warnings,
    )
