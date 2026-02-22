"""API router for Module 1 — Startup Ingestion & Profiling.

FoundationIQ 3.0 (Startup Edition)

Endpoint:
    POST /api/v1/ingest/startup
        - Accepts startup onboarding form fields (8 questions)
        - Accepts up to 3 CSV files: org_chart, expenses, sales_inquiries
        - Runs data quality checks on each CSV
        - Sends profile to Gemini for startup analysis
        - Returns IngestionResponse with session_id, per-file summaries,
          and LLM-generated profile analysis
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.core.session_store import session_store
from app.schemas.ingestion import (
    ColumnInfo,
    FileIngestionSummary,
    IngestionResponse,
    StartupProfile,
)
from app.services.ingestion import IngestionError, process_single_csv
from app.services.llm import analyse_startup_profile

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["Ingestion — Startup"])


# ---------------------------------------------------------------------------
# Helper: build FileIngestionSummary from a processed CSV
# ---------------------------------------------------------------------------

def _build_file_summary(
    df, issues, filename: str
) -> FileIngestionSummary:
    """Create a FileIngestionSummary from a parsed DataFrame and issues list."""
    total_rows = len(df)
    columns = [
        ColumnInfo(
            name=str(col),
            dtype=str(df[col].dtype),
            non_null_count=int(df[col].notna().sum()),
            null_count=int(df[col].isna().sum()),
            missing_pct=round(
                df[col].isna().sum() / total_rows * 100, 2
            ) if total_rows else 0.0,
        )
        for col in df.columns
    ]
    return FileIngestionSummary(
        filename=filename,
        row_count=total_rows,
        column_count=len(df.columns),
        columns=columns,
        data_issues=issues,
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.post("/startup", response_model=IngestionResponse)
async def ingest_startup(
    # --- Onboarding form fields (8 questions) ---
    company_name: str = Form(..., description="Startup name"),
    sub_type: str = Form(
        ..., description="Startup vertical: EdTech, FinTech, SaaS, or E-commerce"
    ),
    mrr_last_3_months: str = Form(
        ...,
        description='JSON array of 3 floats, e.g. [80000, 95000, 110000]',
    ),
    monthly_growth_goal_pct: float = Form(
        ..., description="Target MoM growth %"
    ),
    patience_months: int = Form(
        ..., description="Months willing to wait for ROI"
    ),
    current_tech_stack: str = Form(
        "",
        description="Comma-separated list of tools, e.g. 'Stripe, Zapier, Freshdesk'",
    ),
    num_employees: int = Form(..., description="Team size"),
    industry: str = Form("Technology", description="Industry label"),
    # --- CSV file uploads (all optional but at least one recommended) ---
    org_chart_file: Optional[UploadFile] = File(
        None, description="org_chart.csv — roles, departments, salaries"
    ),
    expenses_file: Optional[UploadFile] = File(
        None, description="expenses.csv — monthly software / operational costs"
    ),
    sales_inquiries_file: Optional[UploadFile] = File(
        None, description="sales_inquiries.csv — inquiry_date, payment_date, repeat_customer"
    ),
) -> IngestionResponse:
    """Ingest startup onboarding data and optional CSV files.

    - Parses the 8-question onboarding form into a StartupProfile
    - Validates and loads each uploaded CSV
    - Flags data quality issues per file (missing columns, nulls, etc.)
    - Sends profile to Gemini for startup analysis
    - Stores everything in session for downstream modules
    """

    # --- Parse MRR array --------------------------------------------------
    try:
        mrr_list = json.loads(mrr_last_3_months)
        if not isinstance(mrr_list, list) or len(mrr_list) != 3:
            raise ValueError("Must be a JSON array of exactly 3 numbers")
        mrr_list = [float(x) for x in mrr_list]
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid mrr_last_3_months: {exc}. Expected JSON array of 3 numbers.",
        )

    # --- Parse tech stack -------------------------------------------------
    tech_stack_list = [
        t.strip() for t in current_tech_stack.split(",") if t.strip()
    ]

    # --- Build & validate StartupProfile ----------------------------------
    try:
        profile = StartupProfile(
            company_name=company_name,
            sub_type=sub_type,  # type: ignore[arg-type]
            mrr_last_3_months=mrr_list,
            monthly_growth_goal_pct=monthly_growth_goal_pct,
            patience_months=patience_months,
            current_tech_stack=tech_stack_list,
            num_employees=num_employees,
            industry=industry,
        )
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"Invalid profile data: {exc}")

    # --- Process each CSV file --------------------------------------------
    org_chart_df = None
    expenses_df = None
    sales_inquiries_df = None
    org_chart_summary = None
    expenses_summary = None
    sales_inquiries_summary = None
    all_issues = []
    files_uploaded = []
    total_rows = 0

    for file_key, upload, file_type in [
        ("org_chart", org_chart_file, "org_chart"),
        ("expenses", expenses_file, "expenses"),
        ("sales_inquiries", sales_inquiries_file, "sales_inquiries"),
    ]:
        if upload is None:
            continue

        content = await upload.read()
        if not content:
            continue

        try:
            df, issues = process_single_csv(
                file_content=content,
                filename=upload.filename or f"{file_type}.csv",
                file_type=file_type,
            )
        except IngestionError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Error processing {file_type} file: {exc}",
            )

        summary = _build_file_summary(df, issues, upload.filename or f"{file_type}.csv")
        all_issues.extend(issues)
        files_uploaded.append(file_type)
        total_rows += len(df)

        if file_type == "org_chart":
            org_chart_df = df
            org_chart_summary = summary
        elif file_type == "expenses":
            expenses_df = df
            expenses_summary = summary
        elif file_type == "sales_inquiries":
            sales_inquiries_df = df
            sales_inquiries_summary = summary

    # --- LLM startup profile analysis -------------------------------------
    profile_analysis = None
    try:
        profile_analysis = analyse_startup_profile(profile.model_dump())
    except RuntimeError as exc:
        logger.warning("Startup profile analysis skipped: %s", exc)
    except Exception as exc:
        logger.error("Startup profile analysis failed: %s", exc)

    # --- Store in session -------------------------------------------------
    session_id = session_store.create(
        startup_profile=profile.model_dump(),
        org_chart_df=org_chart_df,
        expenses_df=expenses_df,
        sales_inquiries_df=sales_inquiries_df,
        profile_analysis=profile_analysis,
        data_issues=all_issues,
        # Legacy compat — company_metadata mirrors startup_profile
        company_metadata=profile.model_dump(),
        documents_provided=files_uploaded,
    )

    return IngestionResponse(
        session_id=session_id,
        startup_profile=profile,
        profile_analysis=profile_analysis,
        org_chart=org_chart_summary,
        expenses=expenses_summary,
        sales_inquiries=sales_inquiries_summary,
        files_uploaded=files_uploaded,
        total_issues=len(all_issues),
        total_rows=total_rows,
    )
