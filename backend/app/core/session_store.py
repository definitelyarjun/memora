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
    """All artefacts produced across all module runs for one session."""

    raw_dataframe: pd.DataFrame | None    # None for document-type sessions
    workflow_text: str                   # Whitespace-normalised workflow description
    company_metadata: dict
    data_issues: list                    # list[DataIssue] — avoid circular import
    workflow_analysis: object | None     # WorkflowDiagram | None  (Module 1/3)
    quality_report: object | None = None   # QualityReport | None    (Module 2)
    benchmark_report: object | None = None  # BenchmarkReport | None  (Module 3)
    automation_report: object | None = None  # AutomationReport | None (Module 4)
    consolidation_report: object | None = None  # ConsolidationReport | None (Module 5)
    roi_report: object | None = None  # ROIReport | None (Module 6)
    # Which supplementary document types were uploaded alongside the sales data
    # Values: "sales" | "invoices" | "payroll" | "inventory"
    documents_provided: list = field(default_factory=list)
    # Basic stats for each supplementary doc: {"invoices": {"readable": True, "row_count": 150, ...}}
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
        raw_dataframe: pd.DataFrame | None,
        workflow_text: str,
        company_metadata: dict,
        data_issues: list,
        workflow_analysis: object | None = None,
        documents_provided: list | None = None,
        supplementary_doc_stats: dict | None = None,
    ) -> str:
        """Store ingestion artefacts and return a session_id."""
        self._evict_expired()
        session_id = uuid.uuid4().hex
        self._store[session_id] = SessionEntry(
            raw_dataframe=raw_dataframe,
            workflow_text=workflow_text,
            company_metadata=company_metadata,
            data_issues=data_issues,
            workflow_analysis=workflow_analysis,
            documents_provided=documents_provided or ["sales"],
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
