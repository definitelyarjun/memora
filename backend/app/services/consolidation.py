"""Module 5 — Data Consolidation Recommendation Engine (rule-based, no LLM).

Analyses scattered tools, data flows, and redundancies across a company's
operations and recommends a concrete unification strategy.

Pipeline
--------
1. **Silo discovery** — map each tool/medium to a DataSilo with tier, data
   types, users, and weaknesses.
2. **Data flow detection** — scan workflow steps for manual hand-offs between
   silos (re-entry, copy-paste, verbal transfers).
3. **Redundancy detection** — find silos that store the same kind of data.
4. **Unified schema generation** — propose table definitions based on the
   uploaded DataFrame columns + detected data types.
5. **Migration plan** — ordered, effort-rated, role-aware action steps.
6. **Consolidation score** — single 0–1 metric of how unified the current
   landscape is.

Everything is deterministic and explainable.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.consolidation import (
    ConsolidationReport,
    DataFlow,
    DataSilo,
    MigrationStep,
    RedundancyFlag,
    UnifiedSchemaColumn,
    UnifiedSchemaRecommendation,
)


# ═══════════════════════════════════════════════════════════════════════════
# Tool classification (reuses tiers from quality.py but adds more detail)
# ═══════════════════════════════════════════════════════════════════════════

_TIER3_TOOLS: dict[str, dict] = {
    # key: canonical name (lowercase), value: metadata
    "sap":           {"category": "ERP",     "tier": "Enterprise"},
    "oracle":        {"category": "ERP",     "tier": "Enterprise"},
    "netsuite":      {"category": "ERP",     "tier": "Enterprise"},
    "dynamics":      {"category": "ERP",     "tier": "Enterprise"},
    "odoo":          {"category": "ERP",     "tier": "Enterprise"},
    "erp":           {"category": "ERP",     "tier": "Enterprise"},
    "salesforce":    {"category": "CRM",     "tier": "Enterprise"},
    "hubspot":       {"category": "CRM",     "tier": "Enterprise"},
    "zoho crm":      {"category": "CRM",     "tier": "Enterprise"},
    "crm":           {"category": "CRM",     "tier": "Enterprise"},
    "quickbooks":    {"category": "Accounting", "tier": "Enterprise"},
    "xero":          {"category": "Accounting", "tier": "Enterprise"},
    "freshbooks":    {"category": "Accounting", "tier": "Enterprise"},
    "pos":           {"category": "POS",     "tier": "Enterprise"},
    "square":        {"category": "POS",     "tier": "Enterprise"},
    "shopify":       {"category": "Ecommerce", "tier": "Enterprise"},
    "woocommerce":   {"category": "Ecommerce", "tier": "Enterprise"},
    "jira":          {"category": "PM",      "tier": "Enterprise"},
    "asana":         {"category": "PM",      "tier": "Enterprise"},
    "stripe":        {"category": "Payments", "tier": "Enterprise"},
    "razorpay":      {"category": "Payments", "tier": "Enterprise"},
    "paypal":        {"category": "Payments", "tier": "Enterprise"},
    "power bi":      {"category": "BI",      "tier": "Enterprise"},
    "tableau":       {"category": "BI",      "tier": "Enterprise"},
    "mysql":         {"category": "Database", "tier": "Enterprise"},
    "postgresql":    {"category": "Database", "tier": "Enterprise"},
    "mongodb":       {"category": "Database", "tier": "Enterprise"},
}

_TIER2_TOOLS: dict[str, dict] = {
    "excel":         {"category": "Spreadsheet", "tier": "Productivity"},
    "google sheets": {"category": "Spreadsheet", "tier": "Productivity"},
    "airtable":      {"category": "Spreadsheet", "tier": "Productivity"},
    "tally":         {"category": "Accounting",  "tier": "Productivity"},
    "google forms":  {"category": "Forms",       "tier": "Productivity"},
    "google drive":  {"category": "Storage",     "tier": "Productivity"},
    "dropbox":       {"category": "Storage",     "tier": "Productivity"},
    "onedrive":      {"category": "Storage",     "tier": "Productivity"},
    "gmail":         {"category": "Email",       "tier": "Productivity"},
    "outlook":       {"category": "Email",       "tier": "Productivity"},
    "google pay":    {"category": "Payments",    "tier": "Productivity"},
    "phonepe":       {"category": "Payments",    "tier": "Productivity"},
    "paytm":         {"category": "Payments",    "tier": "Productivity"},
    "upi":           {"category": "Payments",    "tier": "Productivity"},
    "zoom":          {"category": "Communication", "tier": "Productivity"},
}

_TIER1_TOOLS: dict[str, dict] = {
    "whatsapp":      {"category": "Messaging",  "tier": "Informal"},
    "sms":           {"category": "Messaging",  "tier": "Informal"},
    "phone":         {"category": "Communication", "tier": "Informal"},
    "paper":         {"category": "Paper",      "tier": "Informal"},
    "pen":           {"category": "Paper",      "tier": "Informal"},
    "calculator":    {"category": "Paper",      "tier": "Informal"},
    "diary":         {"category": "Paper",      "tier": "Informal"},
    "logbook":       {"category": "Paper",      "tier": "Informal"},
    "ledger":        {"category": "Paper",      "tier": "Informal"},
    "notebook":      {"category": "Paper",      "tier": "Informal"},
    "notepad":       {"category": "Paper",      "tier": "Informal"},
    "sticky note":   {"category": "Paper",      "tier": "Informal"},
}


def _classify_tool_detail(tool_name: str) -> dict:
    """Return {canonical, category, tier} for a tool string."""
    t = tool_name.lower().strip()
    words = set(t.replace("-", " ").replace("_", " ").split())

    for key, meta in _TIER3_TOOLS.items():
        kw_words = set(key.split())
        if kw_words.issubset(words) or key == t:
            return {"canonical": key, **meta}
    for key, meta in _TIER2_TOOLS.items():
        kw_words = set(key.split())
        if kw_words.issubset(words) or key == t:
            return {"canonical": key, **meta}
    for key, meta in _TIER1_TOOLS.items():
        kw_words = set(key.split())
        if kw_words.issubset(words) or key == t:
            return {"canonical": key, **meta}

    # Unknown → treat as Productivity by default
    return {"canonical": t, "category": "Other", "tier": "Productivity"}


# Weaknesses by tool category
_SILO_WEAKNESSES: dict[str, list[str]] = {
    "Paper":         ["No backup or version history", "Not searchable", "Prone to physical loss", "Cannot be shared remotely"],
    "Messaging":     ["No structured storage", "Conversations lost in chat history", "No reporting capability"],
    "Communication": ["Data shared verbally is error-prone", "No audit trail"],
    "Spreadsheet":   ["Single user at a time (Excel)", "No real-time sync", "Formulas break silently", "No access controls"],
    "Storage":       ["Files are unstructured", "No query capability", "Version conflicts"],
    "Email":         ["Attachments get buried", "No structured reporting"],
    "Forms":         ["Data trapped in form responses", "Limited analytics"],
    "Payments":      ["Transaction data siloed from accounting"],
    "Accounting":    [],
    "ERP":           [],
    "CRM":           [],
    "POS":           [],
    "BI":            [],
    "Database":      [],
    "Ecommerce":     [],
    "PM":            [],
    "Other":         [],
}


# ═══════════════════════════════════════════════════════════════════════════
# Keywords that hint what data type a workflow step involves
# ═══════════════════════════════════════════════════════════════════════════

_DATA_TYPE_KEYWORDS: dict[str, list[str]] = {
    "Sales":      ["sale", "revenue", "transaction", "order", "receipt", "bill", "pos", "customer payment"],
    "Invoices":   ["invoice", "supplier", "vendor", "procurement", "purchase order"],
    "Payroll":    ["salary", "payroll", "wage", "staff", "employee", "hr"],
    "Inventory":  ["stock", "inventory", "ingredient", "supply", "warehouse", "storage"],
    "Accounting": ["ledger", "reconcile", "expense", "tally", "accountant", "audit", "balance", "p&l", "tax"],
    "Scheduling": ["reservation", "booking", "schedule", "roster", "shift", "appointment", "diary"],
    "Reporting":  ["report", "dashboard", "summary", "analysis", "forecast"],
}


def _detect_data_types(text: str) -> list[str]:
    """Return data types that the text likely involves."""
    text_l = text.lower()
    found: list[str] = []
    for dtype, keywords in _DATA_TYPE_KEYWORDS.items():
        if any(kw in text_l for kw in keywords):
            found.append(dtype)
    return found


# ═══════════════════════════════════════════════════════════════════════════
# Implicit silo detection from workflow step descriptions
# ═══════════════════════════════════════════════════════════════════════════

_IMPLICIT_MEDIUM_PATTERNS: list[tuple[str, str]] = [
    # (regex pattern, silo name)
    (r"paper|printout|hard copy|binder|folder|slip",    "Paper records"),
    (r"ledger book|ledger",                               "Paper ledger"),
    (r"logbook|log book",                                 "Paper logbook"),
    (r"diary|notebook|notepad",                           "Paper diary/notebook"),
    (r"sticky note",                                      "Sticky notes"),
    (r"whiteboard",                                       "Whiteboard"),
    (r"verbal(?:ly)?|word of mouth|call(?:s|ed|ing)?\b",  "Verbal communication"),
    (r"whatsapp|sms|text message",                        "WhatsApp/SMS"),
    (r"excel|spreadsheet|\.xlsx?|\.csv",                  "Excel/Spreadsheets"),
    (r"calculator",                                       "Calculator"),
    (r"google sheets",                                    "Google Sheets"),
    (r"tally",                                            "Tally"),
]


def _detect_implicit_silos(text: str) -> list[str]:
    """Find media/tools mentioned in description text (not in metadata)."""
    text_l = text.lower()
    found: list[str] = []
    for pattern, silo_name in _IMPLICIT_MEDIUM_PATTERNS:
        if re.search(pattern, text_l):
            found.append(silo_name)
    return found


# ═══════════════════════════════════════════════════════════════════════════
# Transfer / hand-off detection between steps
# ═══════════════════════════════════════════════════════════════════════════

_TRANSFER_KEYWORDS: list[tuple[str, str]] = [
    # (regex, method label)
    (r"transfer|copy|copies|move|forward|send|pass",   "Manual re-entry"),
    (r"verbal|phone|call|tell|inform|word of mouth",   "Verbal hand-off"),
    (r"hand\s*(over|off)|deliver|give|submit|bring",   "Paper hand-off"),
    (r"enter|re-?enter|type|key\s+in|input",           "Manual re-entry"),
    (r"email|attach|forward",                           "Email attachment"),
]


def _detect_transfer_method(text: str) -> str | None:
    """Return the most likely manual transfer method from a step description."""
    text_l = text.lower()
    for pattern, method in _TRANSFER_KEYWORDS:
        if re.search(pattern, text_l):
            return method
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Migration recommendation engine
# ═══════════════════════════════════════════════════════════════════════════

# What to replace each informal/low tool with, based on data type
_REPLACEMENT_MAP: dict[str, dict[str, str]] = {
    # key = silo category, value = {data_type: recommended replacement}
    "Paper": {
        "Sales":      "POS system (e.g. Square, Lightspeed)",
        "Invoices":   "Cloud accounting (e.g. Zoho Invoice, QuickBooks)",
        "Payroll":    "Payroll software (e.g. Gusto, GreytHR)",
        "Inventory":  "Inventory management module (e.g. Zoho Inventory, inFlow)",
        "Accounting": "Accounting software (e.g. Tally, QuickBooks, Zoho Books)",
        "Scheduling": "Online booking system (e.g. Google Calendar, Calendly, OpenTable)",
        "Reporting":  "Dashboard tool (e.g. Google Sheets → Looker, Power BI)",
        "_default":   "Digital form or spreadsheet to start digitising records",
    },
    "Messaging": {
        "Sales":      "CRM with order tracking (e.g. Zoho CRM, HubSpot Free)",
        "Invoices":   "Cloud accounting with supplier portal",
        "_default":   "Dedicated communication channel with audit trail (e.g. Slack, Teams)",
    },
    "Communication": {
        "_default":   "Structured order/notification system (e.g. API integration or forms)",
    },
    "Spreadsheet": {
        "Sales":      "Cloud-synced spreadsheet (Google Sheets) or POS",
        "Accounting": "Accounting software (e.g. Tally, QuickBooks)",
        "Reporting":  "BI dashboard (e.g. Power BI, Google Looker Studio)",
        "_default":   "Cloud spreadsheet with shared access (Google Sheets, Airtable)",
    },
}


def _suggest_replacement(silo_category: str, data_types: list[str]) -> str:
    """Return a specific replacement tool recommendation."""
    cat_map = _REPLACEMENT_MAP.get(silo_category, {})
    for dt in data_types:
        if dt in cat_map:
            return cat_map[dt]
    return cat_map.get("_default", "Evaluate cloud-based alternatives for this workflow area")


# ═══════════════════════════════════════════════════════════════════════════
# Unified schema generator
# ═══════════════════════════════════════════════════════════════════════════

_DTYPE_MAP: dict[str, str] = {
    "int64":    "integer",
    "float64":  "decimal",
    "object":   "text",
    "bool":     "boolean",
    "datetime64[ns]": "datetime",
    "category": "text",
}

_DATE_HINT_WORDS = {"date", "time", "created", "updated", "timestamp", "day", "month", "year"}
_AMOUNT_HINT_WORDS = {"amount", "price", "cost", "total", "subtotal", "tax", "fee", "salary", "wage", "revenue", "profit"}
_ID_HINT_WORDS = {"id", "code", "number", "no", "ref", "reference", "sku", "barcode"}


def _infer_semantic_type(col_name: str, pandas_dtype: str) -> str:
    """Infer a more useful schema type from column name + dtype."""
    name_l = col_name.lower().replace("_", " ").replace("-", " ")
    words = set(name_l.split())

    if words & _DATE_HINT_WORDS:
        return "datetime"
    if words & _AMOUNT_HINT_WORDS:
        return "decimal"
    if words & _ID_HINT_WORDS:
        return "text (identifier)"
    return _DTYPE_MAP.get(pandas_dtype, "text")


def _build_unified_schemas(
    df: pd.DataFrame | None,
    data_types_seen: set[str],
    documents_provided: list[str],
) -> list[UnifiedSchemaRecommendation]:
    """Propose one or more unified table schemas from the uploaded data."""
    schemas: list[UnifiedSchemaRecommendation] = []

    # Primary schema: from the actual DataFrame columns
    if df is not None and not df.empty:
        cols: list[UnifiedSchemaColumn] = []
        for col_name in df.columns:
            dtype_str = str(df[col_name].dtype)
            semantic = _infer_semantic_type(col_name, dtype_str)
            cols.append(UnifiedSchemaColumn(
                name=_normalise_col_name(col_name),
                source=f"Uploaded CSV/Excel column '{col_name}'",
                dtype=semantic,
                notes=f"Original dtype: {dtype_str}",
            ))

        schemas.append(UnifiedSchemaRecommendation(
            table_name="core_transactions",
            purpose="Central transactional record — single source of truth for daily operations",
            columns=cols,
        ))

    # Supplementary schemas based on detected data types not in the main file
    supp_tables: dict[str, tuple[str, list[tuple[str, str, str]]]] = {
        "Invoices": (
            "supplier_invoices",
            [
                ("invoice_id", "text (identifier)", "Unique invoice reference"),
                ("supplier_name", "text", "Vendor / supplier"),
                ("invoice_date", "datetime", "Date of invoice"),
                ("amount", "decimal", "Invoice total"),
                ("status", "text", "Paid / Pending / Overdue"),
                ("linked_transaction_id", "text (identifier)", "FK to core_transactions"),
            ],
        ),
        "Payroll": (
            "staff_payroll",
            [
                ("employee_id", "text (identifier)", "Unique employee reference"),
                ("employee_name", "text", "Staff member name"),
                ("role", "text", "Job title / role"),
                ("pay_period", "text", "Month or pay cycle"),
                ("gross_salary", "decimal", "Gross amount"),
                ("deductions", "decimal", "Tax + deductions"),
                ("net_salary", "decimal", "Take-home pay"),
                ("payment_date", "datetime", "When paid"),
            ],
        ),
        "Inventory": (
            "inventory_log",
            [
                ("item_id", "text (identifier)", "Unique item / SKU"),
                ("item_name", "text", "Description"),
                ("quantity_on_hand", "integer", "Current stock level"),
                ("reorder_threshold", "integer", "Minimum before reorder"),
                ("unit_cost", "decimal", "Cost per unit"),
                ("last_restocked", "datetime", "Most recent restock date"),
                ("supplier", "text", "Primary supplier"),
            ],
        ),
        "Scheduling": (
            "bookings",
            [
                ("booking_id", "text (identifier)", "Unique reservation ref"),
                ("customer_name", "text", "Customer"),
                ("datetime", "datetime", "Date and time of booking"),
                ("party_size", "integer", "Number of guests"),
                ("status", "text", "Confirmed / Cancelled / No-show"),
                ("notes", "text", "Special requests"),
            ],
        ),
    }

    for dtype, (table_name, col_defs) in supp_tables.items():
        if dtype in data_types_seen:
            columns = [
                UnifiedSchemaColumn(
                    name=cname, source=f"Recommended for {dtype} tracking",
                    dtype=ctype, notes=cnote,
                )
                for cname, ctype, cnote in col_defs
            ]
            schemas.append(UnifiedSchemaRecommendation(
                table_name=table_name,
                purpose=f"Centralised {dtype.lower()} records — currently scattered or paper-based",
                columns=columns,
            ))

    return schemas


def _normalise_col_name(name: str) -> str:
    """Normalise a column name to snake_case."""
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


# ═══════════════════════════════════════════════════════════════════════════
# Consolidation score
# ═══════════════════════════════════════════════════════════════════════════

def _consolidation_score(
    silos: list[DataSilo],
    flows: list[DataFlow],
    redundancies: list[RedundancyFlag],
) -> float:
    """Compute a 0–1 score: 1.0 = fully consolidated, 0.0 = total fragmentation.

    Factors:
      1. Silo diversity penalty   — more silos = worse
      2. Informal ratio penalty   — more Informal tools = worse
      3. Manual flow penalty      — each manual hand-off is risky
      4. Redundancy penalty       — duplicate data across silos
    """
    if not silos:
        return 1.0  # No data detected — vacuously consolidated

    total_silos = len(silos)
    informal = sum(1 for s in silos if s.tier == "Informal")
    enterprise = sum(1 for s in silos if s.tier == "Enterprise")

    # Base: start from 1.0 and subtract penalties
    score = 1.0

    # Penalty for fragmentation (many silos)
    # 1 silo = 0 penalty, each additional silo = -0.06
    score -= max(0, (total_silos - 1)) * 0.06

    # Penalty for informal tools (paper, WhatsApp, verbal)
    # Each informal silo = -0.10
    score -= informal * 0.10

    # Bonus for having enterprise tools (they unify data)
    score += min(0.15, enterprise * 0.05)

    # Penalty for manual data flows
    high_risk_flows = sum(1 for f in flows if f.risk == "High")
    med_risk_flows = sum(1 for f in flows if f.risk == "Medium")
    score -= high_risk_flows * 0.08
    score -= med_risk_flows * 0.04

    # Penalty for redundancy
    score -= len(redundancies) * 0.06

    return round(max(0.0, min(1.0, score)), 2)


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def compute_consolidation_report(
    session_id: str,
    entry: SessionEntry,
) -> ConsolidationReport:
    """Analyse a session's tools, data flows, and structure, then recommend
    a concrete consolidation strategy.

    Raises:
        ValueError: if no tools and no workflow are available.
    """
    tools_used: list[str] = entry.company_metadata.get("tools_used", [])
    workflow = entry.workflow_analysis
    df = entry.raw_dataframe
    documents_provided = entry.documents_provided or []

    if not tools_used and workflow is None:
        raise ValueError(
            "Cannot generate consolidation recommendations without either "
            "tool information (company_metadata.tools_used) or workflow analysis. "
            "Run Module 1a first."
        )

    # ── 1. Discover silos from metadata tools ─────────────────────────────
    silo_map: dict[str, DataSilo] = {}  # canonical name → DataSilo

    for tool_name in tools_used:
        detail = _classify_tool_detail(tool_name)
        canonical = detail["canonical"]
        if canonical not in silo_map:
            weaknesses = _SILO_WEAKNESSES.get(detail["category"], [])[:3]
            silo_map[canonical] = DataSilo(
                name=tool_name,
                tier=detail["tier"],
                data_types=[],
                used_by=[],
                workflow_steps=[],
                weaknesses=weaknesses,
            )

    # ── 2. Enrich silos from workflow steps ────────────────────────────────
    all_data_types_seen: set[str] = set()
    data_flows: list[DataFlow] = []
    step_silo_pairs: list[tuple[int, str]] = []  # (step_number, silo_canonical)

    if workflow and hasattr(workflow, "steps"):
        for step in workflow.steps:
            desc = step.description
            actor = step.actor
            combined = f"{desc} {actor}"
            if step.tool_used:
                combined += f" {step.tool_used}"

            # Detect data types for this step
            step_data_types = _detect_data_types(combined)
            all_data_types_seen.update(step_data_types)

            # Find which silo this step uses
            step_silos: list[str] = []

            # a) Explicit tool_used from the step
            if step.tool_used:
                detail = _classify_tool_detail(step.tool_used)
                canonical = detail["canonical"]
                if canonical not in silo_map:
                    weaknesses = _SILO_WEAKNESSES.get(detail["category"], [])[:3]
                    silo_map[canonical] = DataSilo(
                        name=step.tool_used,
                        tier=detail["tier"],
                        data_types=[],
                        used_by=[],
                        workflow_steps=[],
                        weaknesses=weaknesses,
                    )
                step_silos.append(canonical)

            # b) Implicit media mentioned in description
            for implicit_name in _detect_implicit_silos(desc):
                impl_detail = _classify_tool_detail(implicit_name)
                impl_canonical = impl_detail["canonical"]
                if impl_canonical not in silo_map:
                    weaknesses = _SILO_WEAKNESSES.get(impl_detail["category"], [])[:3]
                    silo_map[impl_canonical] = DataSilo(
                        name=implicit_name,
                        tier=impl_detail["tier"],
                        data_types=[],
                        used_by=[],
                        workflow_steps=[],
                        weaknesses=weaknesses,
                    )
                if impl_canonical not in step_silos:
                    step_silos.append(impl_canonical)

            # Update silo metadata
            for sc in step_silos:
                silo = silo_map[sc]
                if step.step_number not in silo.workflow_steps:
                    silo.workflow_steps.append(step.step_number)
                if actor and actor not in silo.used_by:
                    silo.used_by.append(actor)
                for dt in step_data_types:
                    if dt not in silo.data_types:
                        silo.data_types.append(dt)
                step_silo_pairs.append((step.step_number, sc))

            # Detect data flows (manual transfers)
            if step.step_type in ("Manual", "Unknown"):
                transfer = _detect_transfer_method(desc)
                if transfer and len(step_silos) >= 1:
                    # Infer from→to: the step's silo is the destination
                    # The previous step's silo is the source
                    prev_silos = [
                        sc for sn, sc in step_silo_pairs
                        if sn == step.step_number - 1
                    ]
                    if prev_silos and prev_silos[0] != step_silos[0]:
                        risk = "High" if transfer in ("Verbal hand-off", "Manual re-entry") else "Medium"
                        data_flows.append(DataFlow(
                            from_silo=silo_map[prev_silos[0]].name,
                            to_silo=silo_map[step_silos[0]].name,
                            method=transfer,
                            step_number=step.step_number,
                            description=f"Step {step.step_number}: {desc[:80]}",
                            risk=risk,
                        ))

    # Also add data types from documents_provided
    doc_to_dtype = {"sales": "Sales", "invoices": "Invoices", "payroll": "Payroll", "inventory": "Inventory"}
    for doc in documents_provided:
        dtype = doc_to_dtype.get(doc.lower())
        if dtype:
            all_data_types_seen.add(dtype)

    silos = list(silo_map.values())

    # ── 3. Redundancy detection ────────────────────────────────────────────
    redundancies: list[RedundancyFlag] = []
    # Group silos by data type → find overlapping silos
    dtype_to_silos: dict[str, list[str]] = defaultdict(list)
    for silo in silos:
        for dt in silo.data_types:
            dtype_to_silos[dt].append(silo.name)

    seen_pairs: set[tuple[str, str]] = set()
    for dtype, silo_names in dtype_to_silos.items():
        if len(silo_names) >= 2:
            for i, a in enumerate(silo_names):
                for b in silo_names[i + 1:]:
                    pair = tuple(sorted((a, b)))
                    if pair not in seen_pairs:
                        seen_pairs.add(pair)
                        # Recommend the higher-tier silo as source of truth
                        silo_a = silo_map.get(
                            next((k for k, v in silo_map.items() if v.name == a), ""),
                        )
                        silo_b = silo_map.get(
                            next((k for k, v in silo_map.items() if v.name == b), ""),
                        )
                        tier_rank = {"Enterprise": 3, "Productivity": 2, "Informal": 1}
                        if silo_a and silo_b:
                            winner = a if tier_rank.get(silo_a.tier, 0) >= tier_rank.get(silo_b.tier, 0) else b
                        else:
                            winner = a
                        redundancies.append(RedundancyFlag(
                            silo_a=a,
                            silo_b=b,
                            overlapping_data=dtype,
                            recommendation=f"Use '{winner}' as the single source of truth for {dtype} data",
                        ))

    # ── 4. Unified schemas ─────────────────────────────────────────────────
    unified_schemas = _build_unified_schemas(df, all_data_types_seen, documents_provided)

    # ── 5. Migration plan ──────────────────────────────────────────────────
    migration_steps: list[MigrationStep] = []
    priority_counter = 0

    # Sort silos: informal first (most urgent), then productivity
    tier_priority = {"Informal": 0, "Productivity": 1, "Enterprise": 2}
    sorted_silos = sorted(silos, key=lambda s: tier_priority.get(s.tier, 2))

    for silo in sorted_silos:
        if silo.tier == "Enterprise":
            continue  # Enterprise tools are already good

        detail = _classify_tool_detail(silo.name)
        replacement = _suggest_replacement(detail["category"], silo.data_types)

        # Effort: Informal → Low (just start digitising), Productivity → Medium (migration)
        effort = "Low" if silo.tier == "Informal" else "Medium"

        # Data at risk
        data_at_risk = ", ".join(silo.data_types) if silo.data_types else "operational records"

        priority_counter += 1
        migration_steps.append(MigrationStep(
            priority=priority_counter,
            action=f"Replace '{silo.name}' with {replacement}",
            from_tool=silo.name,
            to_tool=replacement,
            rationale=_build_migration_rationale(silo, detail, data_flows),
            effort=effort,
            affected_roles=silo.used_by[:5],
            data_at_risk=data_at_risk,
        ))

    # ── 6. Score ───────────────────────────────────────────────────────────
    score = _consolidation_score(silos, data_flows, redundancies)

    # ── 7. Executive summary + recommendations ─────────────────────────────
    informal_count = sum(1 for s in silos if s.tier == "Informal")
    manual_flow_count = len(data_flows)

    exec_summary = _build_executive_summary(
        silos, data_flows, redundancies, score, informal_count,
    )

    top_recs = _build_top_recommendations(
        silos, data_flows, redundancies, migration_steps, score,
    )

    return ConsolidationReport(
        session_id=session_id,
        silos=silos,
        data_flows=data_flows,
        redundancies=redundancies,
        unified_schemas=unified_schemas,
        migration_steps=migration_steps,
        total_silos=len(silos),
        informal_silos=informal_count,
        manual_flows=manual_flow_count,
        consolidation_score=score,
        executive_summary=exec_summary,
        top_recommendations=top_recs,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Narrative builders
# ═══════════════════════════════════════════════════════════════════════════

def _build_migration_rationale(
    silo: DataSilo,
    detail: dict,
    flows: list[DataFlow],
) -> str:
    """Build a specific rationale string for migrating away from a silo."""
    parts: list[str] = []

    if silo.weaknesses:
        parts.append(f"Current problems: {'; '.join(silo.weaknesses[:2])}")

    # Check if this silo is source/destination of manual flows
    related_flows = [
        f for f in flows
        if f.from_silo == silo.name or f.to_silo == silo.name
    ]
    if related_flows:
        parts.append(
            f"This tool is involved in {len(related_flows)} manual data "
            f"transfer(s) that risk data loss or errors"
        )

    if silo.data_types:
        parts.append(f"Currently stores: {', '.join(silo.data_types)}")

    if not parts:
        parts.append("Upgrading reduces manual effort and improves data reliability")

    return ". ".join(parts) + "."


def _build_executive_summary(
    silos: list[DataSilo],
    flows: list[DataFlow],
    redundancies: list[RedundancyFlag],
    score: float,
    informal_count: int,
) -> str:
    """Two to three sentence summary."""
    total = len(silos)
    parts: list[str] = []

    if total <= 1:
        parts.append("Operations appear to use a single data source")
    else:
        parts.append(
            f"Data is currently spread across **{total} separate tools/media**"
        )

    if informal_count > 0:
        parts.append(
            f"**{informal_count}** of these are informal (paper, verbal, "
            f"messaging) — creating data loss risk and zero reporting capability"
        )

    if flows:
        high_risk = sum(1 for f in flows if f.risk == "High")
        if high_risk:
            parts.append(
                f"**{high_risk}** high-risk manual data transfers were detected "
                f"where information moves between tools via re-entry or verbal hand-off"
            )

    if redundancies:
        parts.append(
            f"**{len(redundancies)}** instance(s) of the same data stored in "
            f"multiple places, creating reconciliation overhead"
        )

    label = (
        "well-consolidated" if score >= 0.75 else
        "partially fragmented" if score >= 0.50 else
        "significantly fragmented" if score >= 0.25 else
        "critically fragmented"
    )
    parts.append(f"Overall consolidation: **{score*100:.0f}%** ({label})")

    return ". ".join(parts) + "."


def _build_top_recommendations(
    silos: list[DataSilo],
    flows: list[DataFlow],
    redundancies: list[RedundancyFlag],
    migrations: list[MigrationStep],
    score: float,
) -> list[str]:
    """Build up to 6 prioritised recommendations."""
    recs: list[str] = []

    # 1. Eliminate informal silos first
    informal = [s for s in silos if s.tier == "Informal"]
    if informal:
        names = ", ".join(s.name for s in informal[:3])
        recs.append(
            f"🔴 **Digitise informal tools immediately** — {names} have no backup, "
            f"no search capability, and no reporting. Start with the tool that holds "
            f"the most critical data."
        )

    # 2. Eliminate high-risk flows
    high_risk_flows = [f for f in flows if f.risk == "High"]
    if high_risk_flows:
        recs.append(
            f"🟠 **Eliminate {len(high_risk_flows)} high-risk data transfer(s)** — "
            f"manual re-entry and verbal hand-offs are the #1 cause of data errors. "
            f"Automate these connections or use a shared system."
        )

    # 3. Resolve redundancies
    if redundancies:
        recs.append(
            f"🟡 **Resolve {len(redundancies)} data redundancy issue(s)** — "
            f"the same data in multiple places causes version conflicts. "
            f"Designate a single source of truth for each data type."
        )

    # 4. Top migration actions
    for mig in migrations[:3]:
        recs.append(
            f"📋 **{mig.action}** — {mig.rationale[:100]}"
            f"{'…' if len(mig.rationale) > 100 else ''}"
            f" (Effort: {mig.effort})"
        )

    # 5. If score is high, acknowledge
    if score >= 0.75:
        recs.append(
            "✅ **Good foundation** — your data infrastructure is relatively "
            "well-consolidated. Focus on refining integrations between existing tools."
        )

    return recs[:6]
