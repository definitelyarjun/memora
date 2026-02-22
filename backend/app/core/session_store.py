"""In-memory session store for raw DataFrames and ingestion artefacts.

Each ingestion run produces a session_id → SessionEntry mapping that downstream
modules (data quality, automation detector, etc.) can retrieve without the
client re-uploading the file.

NOTE: Single-process in-memory store. Replace with Redis or a database when
horizontal scaling or persistence is required.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field

import pandas as pd

from app.core.config import settings


@dataclass
class SessionEntry:
    """All artefacts produced across all module runs for one session.

    FoundationIQ 3.0 — Startup Edition fields:
      startup_profile   – 8-question onboarding answers (dict)
      org_chart_df      – org_chart.csv as DataFrame
      expenses_df       – expenses.csv as DataFrame
      sales_inquiries_df– sales_inquiries.csv as DataFrame
    Legacy fields kept for backward compat until remaining modules migrate.
    """

    # --- Module 1: Startup Ingestion & Profiling ---
    startup_profile: dict = field(default_factory=dict)
    org_chart_df: pd.DataFrame | None = None
    expenses_df: pd.DataFrame | None = None
    sales_inquiries_df: pd.DataFrame | None = None
    profile_analysis: object | None = None  # StartupProfileAnalysis | None

    # Aggregate data issues across all uploaded CSVs
    data_issues: list = field(default_factory=list)

    # Legacy — kept so downstream modules don't break during migration
    raw_dataframe: pd.DataFrame | None = None
    workflow_text: str = ""
    company_metadata: dict = field(default_factory=dict)
    workflow_analysis: object | None = None  # WorkflowDiagram | None

    # --- Module 2+: downstream reports ---
    quality_report: object | None = None
    benchmark_report: object | None = None
    automation_report: object | None = None
    financial_report: object | None = None
    retention_report: object | None = None
    roi_report: object | None = None
    documents_provided: list = field(default_factory=list)
    supplementary_doc_stats: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class SessionStore:
    """Thread-safe (GIL-protected) in-memory store with TTL expiry."""

    def __init__(self, ttl_minutes: int | None = None) -> None:
        self._store: dict[str, SessionEntry] = {}
        self._ttl_seconds = (ttl_minutes or settings.session_ttl_minutes) * 60

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create(
        self,
        startup_profile: dict | None = None,
        org_chart_df: pd.DataFrame | None = None,
        expenses_df: pd.DataFrame | None = None,
        sales_inquiries_df: pd.DataFrame | None = None,
        profile_analysis: object | None = None,
        data_issues: list | None = None,
        # Legacy params — kept for backward compat during migration
        raw_dataframe: pd.DataFrame | None = None,
        workflow_text: str = "",
        company_metadata: dict | None = None,
        workflow_analysis: object | None = None,
        documents_provided: list | None = None,
        supplementary_doc_stats: dict | None = None,
    ) -> str:
        """Store ingestion artefacts and return a session_id."""
        self._evict_expired()
        session_id = uuid.uuid4().hex
        self._store[session_id] = SessionEntry(
            startup_profile=startup_profile or {},
            org_chart_df=org_chart_df,
            expenses_df=expenses_df,
            sales_inquiries_df=sales_inquiries_df,
            profile_analysis=profile_analysis,
            data_issues=data_issues or [],
            raw_dataframe=raw_dataframe,
            workflow_text=workflow_text,
            company_metadata=company_metadata or startup_profile or {},
            workflow_analysis=workflow_analysis,
            documents_provided=documents_provided or [],
            supplementary_doc_stats=supplementary_doc_stats or {},
        )
        return session_id

    def get(self, session_id: str) -> SessionEntry | None:
        """Return the session entry if it exists and hasn't expired."""
        self._evict_expired()
        return self._store.get(session_id)

    def patch(self, session_id: str, **kwargs) -> bool:  # type: ignore[override]
        """Update one or more fields on an existing session entry.

        Used by downstream modules to write their output back into the session.
        Returns True if the session existed, False if it had expired / never existed.
        """
        entry = self._store.get(session_id)
        if entry is None:
            return False
        for key, value in kwargs.items():
            if hasattr(entry, key):
                setattr(entry, key, value)
        return True

    def delete(self, session_id: str) -> bool:
        """Explicitly remove a session. Returns True if it existed."""
        return self._store.pop(session_id, None) is not None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_expired(self) -> None:
        now = time.time()
        expired = [
            sid
            for sid, entry in self._store.items()
            if now - entry.created_at > self._ttl_seconds
        ]
        for sid in expired:
            del self._store[sid]


# Global singleton used by the rest of the application.
session_store = SessionStore()
