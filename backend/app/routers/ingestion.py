"""API router for Tabular Data Ingestion.

Endpoint:
    POST /api/v1/ingest/tabular
        - Accepts a CSV or Excel file (primary: sales / transactions)
        - Optionally accepts supplementary files: invoices, payroll, inventory
          (presence is recorded for data coverage scoring; content not yet parsed)
        - Accepts workflow_text (form field)
        - Accepts company_metadata as a JSON string (form field)
        - Returns IngestionResponse with session_id, column summary,
          data issue flags, LLM workflow analysis, and documents_provided list
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.core.session_store import session_store
from app.schemas.ingestion import (
    ColumnInfo,
    CompanyMetadata,
    IngestionResponse,
)
from app.services.ingestion import IngestionError, process_ingestion
from app.services.llm import analyse_workflow

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["Ingestion — Tabular"])


@router.post("/tabular", response_model=IngestionResponse)
async def ingest(
    file: UploadFile = File(..., description="Primary CSV or Excel file (sales / transactions)"),
    workflow_text: str = Form(..., description="Free-text description of the company workflow"),
    company_metadata: str = Form(
        ...,
        description='JSON string, e.g. {"industry":"Retail","num_employees":50,"tools_used":["Excel"]}',
    ),
    invoice_file: Optional[UploadFile] = File(
        None, description="Optional: supplier invoice records (PDF, CSV, or Excel)"
    ),
    payroll_file: Optional[UploadFile] = File(
        None, description="Optional: staff payroll or attendance sheet (CSV or Excel)"
    ),
    inventory_file: Optional[UploadFile] = File(
        None, description="Optional: inventory or stock log (CSV or Excel)"
    ),
) -> IngestionResponse:
    """Ingest raw company data and workflow text.

    - Loads the file as-is (no modification)
    - Flags data quality issues without touching the data
    - Sends workflow text to Gemini via LangChain to produce:
        * Structured workflow steps
        * Mermaid flowchart diagram
        * Executive summary
    - Stores everything in the session store keyed by session_id
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

    # --- Run ingestion pipeline (observe, do not modify) ------------------
    try:
        df, clean_text, meta_out, data_issues = process_ingestion(
            file_content=content,
            filename=file.filename or "unknown",
            workflow_text=workflow_text,
            company_metadata=meta.model_dump(),
        )
    except IngestionError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # --- Analyse workflow with LLM ----------------------------------------
    workflow_analysis = None
    try:
        workflow_analysis = analyse_workflow(clean_text)
    except RuntimeError as exc:
        # API key missing — continue without LLM output, log the warning
        logger.warning("LLM analysis skipped: %s", exc)
    except Exception as exc:
        # LLM call failed (network, quota, etc.) — non-fatal
        logger.error("LLM analysis failed: %s", exc)

    # --- Store raw artefacts in session -----------------------------------
    session_id = session_store.create(
        raw_dataframe=df,
        workflow_text=clean_text,
        company_metadata=meta_out,
        data_issues=data_issues,
        workflow_analysis=workflow_analysis,
    )

    # --- Build column summaries (raw dtypes, null counts) -----------------
    total_rows = len(df)
    columns = [
        ColumnInfo(
            name=str(col),
            dtype=str(df[col].dtype),
            non_null_count=int(df[col].notna().sum()),
            null_count=int(df[col].isna().sum()),
            missing_pct=round(df[col].isna().sum() / total_rows * 100, 2) if total_rows else 0.0,
        )
        for col in df.columns
    ]

    return IngestionResponse(
        session_id=session_id,
        row_count=total_rows,
        column_count=len(df.columns),
        columns=columns,
        data_issues=data_issues,
        workflow_text=clean_text,
        workflow_analysis=workflow_analysis,
        company_metadata=meta,
    )
