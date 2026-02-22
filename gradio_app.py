"""FoundationIQ — Gradio Testing UI

Calls the FastAPI backend running on localhost:8000.
Start the backend first:  cd backend && uvicorn app.main:app --reload
Then run this file:       python gradio_app.py
"""

from __future__ import annotations

import json
import io

import requests
import gradio as gr

API_BASE = "http://localhost:8000/api/v1"


# ---------------------------------------------------------------------------
# Mermaid diagram renderer
# ---------------------------------------------------------------------------

def _mermaid_html(diagram: str) -> str:
    """Render a Mermaid diagram using a sandboxed iframe + CDN.

    Gradio strips <script> tags from gr.HTML, so we embed a full HTML
    document in an iframe srcdoc instead — scripts execute fine there.
    """
    import html as _html
    import re as _re

    # --- Normalise the diagram so Mermaid can always parse it ---
    # Replace literal \n escape sequences (if LLM returned them unresolved)
    diagram = diagram.replace('\\n', '\n')

    # If the whole diagram is still one line, reformat it:
    # insert a newline before every Mermaid keyword / statement starter
    if diagram.count('\n') < 2:
        keywords = (
            r'(?<=[^\n])'
            r'(?=(?:subgraph|end(?=\s|$)|flowchart|graph|classDef|class |click |'
            r'style |linkStyle |[A-Za-z_][A-Za-z0-9_]*(?:\[|\{|\(|\>|\.))'
            r')'
        )
        diagram = _re.sub(keywords, '\n    ', diagram)
        # Also break on bare arrows between nodes
        diagram = _re.sub(r'(?<=[\]\}\)])\s*(-->|--|-.->|==|~~~)', r'\n    \1', diagram)

    # Escape the diagram for safe embedding inside HTML text content
    safe_diagram = _html.escape(diagram)
    inner = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
</head>
<body style="margin:0;padding:12px;background:#f8f9fa;font-family:sans-serif;">
<div class="mermaid">{safe_diagram}</div>
<script>mermaid.initialize({{startOnLoad:true,theme:'neutral',securityLevel:'loose'}});</script>
</body>
</html>"""
    # Escape for use as an HTML attribute value (double-quotes → &quot;)
    srcdoc = inner.replace('&', '&amp;').replace('"', '&quot;')
    return (
        f'<iframe srcdoc="{srcdoc}" '
        f'style="width:100%;min-height:420px;border:1px solid #e0e0e0;'
        f'border-radius:8px;background:#f8f9fa;" '
        f'sandbox="allow-scripts"></iframe>'
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _fmt_json(data: dict) -> str:
    return json.dumps(data, indent=2)


def _badge(level: str) -> str:
    colours = {
        "High": "🟢", "Moderate": "🟡", "Low": "🟠", "Critical": "🔴",
        "Below Market": "🔵", "Competitive": "🟢",
        "Premium": "🟣", "Uncompetitive": "🔴",
    }
    return f"{colours.get(level, '⚪')} **{level}**"


def _meta_dict(industry: str, employees: str, tools: str) -> str:
    try:
        n = int(employees)
    except ValueError:
        n = 0
    tool_list = [t.strip() for t in tools.split(",") if t.strip()]
    return json.dumps({"industry": industry, "num_employees": n, "tools_used": tool_list})


# ---------------------------------------------------------------------------
# Module 1a — Tabular Ingestion
# ---------------------------------------------------------------------------

def run_tabular_ingest(file, workflow_text, industry, employees, tools,
                       invoice_file, payroll_file, inventory_file):
    if file is None:
        return "❌ Please upload a CSV or Excel file.", "", "", "", ""

    meta = _meta_dict(industry, employees, tools)

    # Gradio 6 returns a string path; older versions return an object with .name
    filepath = file if isinstance(file, str) else file.name

    with open(filepath, "rb") as f:
        file_bytes = f.read()

    filename = filepath.split("\\")[-1].split("/")[-1]
    ext = filename.rsplit(".", 1)[-1].lower()
    mime = "text/csv" if ext == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    files_payload = {"file": (filename, io.BytesIO(file_bytes), mime)}

    def _add_optional(key, upload):
        if upload is not None:
            up_path = upload if isinstance(upload, str) else upload.name
            up_name = up_path.split("\\")[-1].split("/")[-1]
            up_ext = up_name.rsplit(".", 1)[-1].lower()
            up_mime_map = {
                "pdf": "application/pdf",
                "csv": "text/csv",
                "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "xls": "application/vnd.ms-excel",
            }
            up_mime = up_mime_map.get(up_ext, "application/octet-stream")
            with open(up_path, "rb") as f:
                up_bytes = f.read()
            files_payload[key] = (up_name, io.BytesIO(up_bytes), up_mime)

    _add_optional("invoice_file", invoice_file)
    _add_optional("payroll_file", payroll_file)
    _add_optional("inventory_file", inventory_file)

    try:
        resp = requests.post(
            f"{API_BASE}/ingest/tabular",
            files=files_payload,
            data={"workflow_text": workflow_text, "company_metadata": meta},
            timeout=60,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is `uvicorn app.main:app --reload` running?", "", "", "", ""

    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", "", ""

    body = resp.json()
    sid = body["session_id"]

    # --- Summary card
    docs_provided = body.get("documents_provided", ["sales"])
    docs_str = ", ".join(d.capitalize() for d in docs_provided)
    summary = f"""## ✅ Ingestion Successful

**Session ID** (copy this for Modules 2 & 3):
```
{sid}
```
| | |
|---|---|
| Rows | {body['row_count']} |
| Columns | {body['column_count']} |
| Issues found | {len(body['data_issues'])} |
| Documents uploaded | {docs_str} |
"""

    # --- Issues
    if body["data_issues"]:
        issues_md = "## 🔍 Data Issues Detected\n\n"
        severity_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}
        for issue in body["data_issues"]:
            icon = severity_icon.get(issue["severity"], "⚪")
            col = f" → `{issue['column']}`" if issue.get("column") else ""
            issues_md += f"- {icon} **{issue['issue_type'].replace('_',' ').title()}**{col}: {issue['description']}\n"
    else:
        issues_md = "## ✅ No Data Issues Found\n\nYour dataset looks clean!"

    # --- Columns table
    col_rows = ""
    for c in body["columns"]:
        completeness = (100 - c["missing_pct"]) / 100  # ColumnInfo has missing_pct (0–100)
        bar = "█" * int(completeness * 10) + "░" * (10 - int(completeness * 10))
        col_rows += f"| `{c['name']}` | {c['dtype']} | {c['non_null_count']}/{c['null_count']+c['non_null_count']} | {bar} {completeness*100:.0f}% |\n"

    cols_md = f"""## 📊 Column Summary

| Column | Type | Non-Null | Completeness |
|---|---|---|---|
{col_rows}"""

    # --- Workflow
    wa = body.get("workflow_analysis")
    mermaid_html_out = ""
    if wa:
        wf_md = f"## 🔄 Workflow Analysis\n\n**Summary:** {wa['summary']}\n\n"
        wf_md += "### Steps\n"
        for step in wa["steps"]:
            icon = "🤖" if step.get("step_type", "").lower() == "automated" else "👤"
            tool = f" *(via {step['tool_used']})*" if step.get("tool_used") else ""
            wf_md += f"{step['step_number']}. {icon} **{step['actor']}** — {step['description']}{tool}\n"
        if wa.get("mermaid_diagram"):
            mermaid_html_out = _mermaid_html(wa["mermaid_diagram"])
    else:
        wf_md = "## 🔄 Workflow Analysis\n\n⚠️ LLM analysis was skipped (check GEMINI_API_KEY)."

    return summary, issues_md, cols_md, wf_md, mermaid_html_out


# ---------------------------------------------------------------------------
# Module 1b — Document Ingestion
# ---------------------------------------------------------------------------

def run_document_ingest(file, doc_type, industry, employees, tools):
    if file is None:
        return "❌ Please upload a PDF, DOCX, or TXT file.", "", ""

    meta = _meta_dict(industry, employees, tools)

    # Gradio 6 returns a string path; older versions return an object with .name
    filepath = file if isinstance(file, str) else file.name

    with open(filepath, "rb") as f:
        file_bytes = f.read()

    filename = filepath.split("\\")[-1].split("/")[-1]
    ext = filename.rsplit(".", 1)[-1].lower()
    mime_map = {
        "pdf": "application/pdf",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "txt": "text/plain",
    }
    mime = mime_map.get(ext, "application/octet-stream")

    try:
        resp = requests.post(
            f"{API_BASE}/ingest/document",
            files={"file": (filename, io.BytesIO(file_bytes), mime)},
            data={"document_type": doc_type, "company_metadata": meta},
            timeout=60,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", ""

    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", ""

    body = resp.json()
    sid = body["session_id"]

    summary = f"""## ✅ Document Ingested

**Session ID:**
```
{sid}
```
| | |
|---|---|
| File | {body['filename']} |
| Type | {body['document_type'].upper()} |
| Pages | {body['page_count'] or 'N/A'} |
| Words | {body['word_count']} |
| Scanned | {'Yes ⚠️' if body['is_scanned'] else 'No'} |
"""
    if body.get("warnings"):
        summary += "\n**Warnings:**\n" + "\n".join(f"- {w}" for w in body["warnings"])

    # --- Text preview
    text = body.get("extracted_text", "")
    preview = text[:2000] + ("\n\n...*(truncated)*" if len(text) > 2000 else "")
    text_md = f"## 📄 Extracted Text Preview\n\n```\n{preview}\n```"

    # --- Workflow or Invoice
    wa = body.get("workflow_analysis")
    inv = body.get("invoice_data")

    if wa:
        analysis_md = f"## 🔄 Workflow (SOP)\n\n**Summary:** {wa['summary']}\n\n"
        for step in wa["steps"]:
            icon = "🤖" if step.get("step_type", "").lower() == "automated" else "👤"
            analysis_md += f"{step['step_number']}. {icon} **{step['actor']}** — {step['description']}\n"
        if wa.get("mermaid_diagram"):
            analysis_md += f"\n```\n{wa['mermaid_diagram']}\n```"

    elif inv:
        analysis_md = f"""## 🧾 Invoice Data Extracted

| Field | Value |
|---|---|
| Invoice # | {inv.get('invoice_number') or 'N/A'} |
| Date | {inv.get('invoice_date') or 'N/A'} |
| Seller | {inv.get('seller_name') or 'N/A'} |
| Seller GSTIN | {inv.get('seller_gstin') or 'N/A'} |
| Buyer | {inv.get('buyer_name') or 'N/A'} |
| Buyer GSTIN | {inv.get('buyer_gstin') or 'N/A'} |
| Subtotal | {inv.get('currency','INR')} {inv.get('subtotal') or 'N/A'} |
| Tax | {inv.get('currency','INR')} {inv.get('tax_amount') or 'N/A'} |
| **Total** | **{inv.get('currency','INR')} {inv.get('total_amount') or 'N/A'}** |
"""
        if inv.get("line_items"):
            analysis_md += "\n### Line Items\n| Description | Qty | Rate | Amount |\n|---|---|---|---|\n"
            for item in inv["line_items"]:
                analysis_md += f"| {item['description']} | {item.get('quantity','—')} | {item.get('rate','—')} | {item.get('amount','—')} |\n"
    else:
        analysis_md = "## ℹ️ No LLM Analysis\n\nNo workflow or invoice extraction was triggered for this document type / file."

    return summary, text_md, analysis_md


# ---------------------------------------------------------------------------
# Module 2 — Data Quality & AI Readiness
# ---------------------------------------------------------------------------

def run_quality(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/quality",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend.", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired. Re-run Module 1a to get a fresh session_id.", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", ""

    body = resp.json()

    def score_bar(score: float) -> str:
        filled = int(score * 20)
        return "█" * filled + "░" * (20 - filled) + f"  {score*100:.1f}%"

    docs_provided = body.get('documents_provided', [])
    docs_str = ', '.join(d.capitalize() for d in docs_provided) if docs_provided else 'Sales only'
    summary = f"""## {_badge(body['readiness_level'])} AI Readiness Score: **{body['ai_readiness_score']*100:.1f}%**

### 📋 Data Quality (48% weight)

| Dimension | Score | Bar |
|---|---|---|
| Completeness (17%) | {body['completeness_score']*100:.1f}% | {score_bar(body['completeness_score'])} |
| Deduplication (12%) | {body['deduplication_score']*100:.1f}% | {score_bar(body['deduplication_score'])} |
| Consistency (11%) | {body['consistency_score']*100:.1f}% | {score_bar(body['consistency_score'])} |
| Structural Integrity (8%) | {body['structural_integrity_score']*100:.1f}% | {score_bar(body['structural_integrity_score'])} |

### 🔧 Operational Readiness (37% weight)

| Dimension | Score | Bar |
|---|---|---|
| Process Digitisation (25%) | {body['process_digitisation_score']*100:.1f}% | {score_bar(body['process_digitisation_score'])} |
| Tool Maturity (12%) | {body['tool_maturity_score']*100:.1f}% | {score_bar(body['tool_maturity_score'])} |

### 📂 Data Coverage (15% weight)

| Dimension | Score | Bar |
|---|---|---|
| Data Coverage (15%) | {body.get('data_coverage_score', 0)*100:.1f}% | {score_bar(body.get('data_coverage_score', 0))} |

**Documents uploaded:** {docs_str}
**Workflow:** {body.get('total_workflow_steps', 0)} steps analysed · {body.get('automated_steps', 0)} automated · {body.get('manual_steps', 0)} manual
**Tools detected:** {', '.join(body.get('tools_detected', [])) or 'None'}
**Dataset:** {body['row_count']} rows · {body['column_count']} columns · {body['missing_cells']} missing cells · {body['duplicate_rows']} duplicate rows
"""

    recs_md = "## 💡 Recommendations\n\n"
    if body["top_recommendations"]:
        for i, rec in enumerate(body["top_recommendations"], 1):
            recs_md += f"**{i}.** {rec}\n\n"
    else:
        recs_md += "✅ No critical issues to address."

    col_md = "## 📊 Per-Column Quality\n\n| Column | Type | Completeness | Issues |\n|---|---|---|---|\n"
    for col in body["column_quality"]:
        issues = ", ".join(col["issue_types"]) if col["issue_types"] else "—"
        bar = "█" * int(col["completeness"] * 10) + "░" * (10 - int(col["completeness"] * 10))
        col_md += f"| `{col['name']}` | {col['dtype']} | {bar} {col['completeness']*100:.0f}% | {issues} |\n"

    return summary, recs_md, col_md


# ---------------------------------------------------------------------------
# Module 4 — Automation Opportunity Detector
# ---------------------------------------------------------------------------

def run_automation(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/automation",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'Missing prerequisites.')}", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()
    s = body["summary"]

    # --- Summary card
    coverage_pct = s["automation_coverage"] * 100
    coverage_bar = "█" * int(coverage_pct / 5) + "░" * (20 - int(coverage_pct / 5))
    level = body["readiness_level"]

    summary = f"""## ⚙️ Automation Report — {_badge(level)} AI Readiness

| Metric | Value |
|---|---|
| Total Steps | {s['total_steps']} |
| Automatable | **{s['automatable_steps']}** |
| Already Automated | {s['already_automated']} |
| Not Recommended | {s['not_recommended']} |
| Coverage | `{coverage_bar}` **{coverage_pct:.0f}%** |
| Avg Confidence | {s['avg_confidence']*100:.0f}% |
| AI Readiness | {body['ai_readiness_score']*100:.0f}% |
"""

    if s["by_type"]:
        summary += "\n**By Automation Type:**\n"
        type_icons = {"RPA": "🤖", "Digital Form": "📝", "API Integration": "🔌",
                      "AI/ML": "🧠", "Decision Engine": "⚖️", "Not Recommended": "❌"}
        for t, count in sorted(s["by_type"].items(), key=lambda x: -x[1]):
            summary += f"- {type_icons.get(t, '•')} {t}: **{count}**\n"

    if s["by_priority"]:
        summary += "\n**By Priority:**\n"
        priority_icons = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}
        for p in ("Critical", "High", "Medium", "Low"):
            if p in s["by_priority"]:
                summary += f"- {priority_icons.get(p, '•')} {p}: **{s['by_priority'][p]}**\n"

    # --- Per-step table
    steps_md = "## 📋 Step-by-Step Classification\n\n"
    steps_md += "| # | Description | Actor | Type | Automation | Confidence | Effort | Priority |\n"
    steps_md += "|---|---|---|---|---|---|---|---|\n"

    for c in body["candidates"]:
        conf_bar = "█" * int(c["confidence"] * 5) + "░" * (5 - int(c["confidence"] * 5))
        candidate_icon = "✅" if c["is_candidate"] else ("⏭️" if c["current_step_type"] == "Automated" else "❌")
        desc = c["description"][:45] + "…" if len(c["description"]) > 45 else c["description"]
        steps_md += (
            f"| {c['step_number']} | {desc} | {c['actor']} | {c['current_step_type']} "
            f"| {candidate_icon} {c['automation_type']} | {conf_bar} {c['confidence']*100:.0f}% "
            f"| {c['estimated_effort']} | {c['priority']} |\n"
        )

    # Reasoning details (collapsible-like)
    steps_md += "\n### 💡 Reasoning\n"
    for c in body["candidates"]:
        if c["is_candidate"]:
            steps_md += f"- **Step {c['step_number']}:** {c['reasoning']}\n"

    # --- Recommendations
    recs_md = "## 🎯 Top Recommendations\n\n"
    if body["top_recommendations"]:
        for i, rec in enumerate(body["top_recommendations"], 1):
            recs_md += f"{i}. {rec}\n\n"
    else:
        recs_md += "No specific recommendations at this time."

    # --- Quick Wins
    qw_md = "## ⚡ Quick Wins\n\n"
    if body["quick_wins"]:
        qw_md += "These steps can be automated with **minimal effort** and **high confidence:**\n\n"
        for win in body["quick_wins"]:
            qw_md += f"- 🏃 {win}\n"
    else:
        qw_md += "No quick wins identified — all automatable steps require moderate to high effort."

    return summary, steps_md, recs_md, qw_md


# ---------------------------------------------------------------------------
# Module 5 — Data Consolidation Recommendations
# ---------------------------------------------------------------------------

def run_consolidation(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/consolidation",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'Missing prerequisites.')}", "", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", "", ""

    body = resp.json()

    # --- Summary card
    score = body["consolidation_score"]
    score_bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))
    label = (
        "Well-consolidated" if score >= 0.75 else
        "Partially fragmented" if score >= 0.50 else
        "Significantly fragmented" if score >= 0.25 else
        "Critically fragmented"
    )

    summary = f"""## 🧩 Data Consolidation Report

**Consolidation Score:** `{score_bar}` **{score*100:.0f}%** ({label})

| Metric | Value |
|---|---|
| Total data silos | **{body['total_silos']}** |
| Informal silos | **{body['informal_silos']}** (paper, verbal, messaging) |
| Manual data flows | **{body['manual_flows']}** |
| Redundancies | **{len(body['redundancies'])}** |

{body['executive_summary']}
"""

    # --- Silos table
    silos_md = "## 📦 Detected Data Silos\n\n"
    silos_md += "| Tool / Medium | Tier | Data Types | Used By | Weaknesses |\n"
    silos_md += "|---|---|---|---|---|\n"
    tier_icon = {"Enterprise": "🟢", "Productivity": "🟡", "Informal": "🔴"}
    for s in body["silos"]:
        dtypes = ", ".join(s["data_types"][:3]) or "—"
        users = ", ".join(s["used_by"][:3]) or "—"
        weak = "; ".join(s["weaknesses"][:2]) or "—"
        icon = tier_icon.get(s["tier"], "⚪")
        silos_md += f"| {icon} {s['name']} | {s['tier']} | {dtypes} | {users} | {weak} |\n"

    # Data flows
    if body["data_flows"]:
        silos_md += "\n### 🔀 Manual Data Flows\n\n"
        silos_md += "| From | To | Method | Risk | Step |\n|---|---|---|---|---|\n"
        risk_icon = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
        for f in body["data_flows"]:
            silos_md += f"| {f['from_silo']} | {f['to_silo']} | {f['method']} | {risk_icon.get(f['risk'], '⚪')} {f['risk']} | {f.get('step_number', '—')} |\n"

    # Redundancies
    if body["redundancies"]:
        silos_md += "\n### ⚠️ Data Redundancies\n\n"
        for r in body["redundancies"]:
            silos_md += f"- **{r['silo_a']}** ↔ **{r['silo_b']}** — overlapping: {r['overlapping_data']}. {r['recommendation']}\n"

    # --- Migration plan
    mig_md = "## 🛤️ Migration Plan\n\n"
    if body["migration_steps"]:
        mig_md += "| # | Action | From | To | Effort | Affected Roles |\n"
        mig_md += "|---|---|---|---|---|---|\n"
        effort_icon = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}
        for m in body["migration_steps"]:
            roles = ", ".join(m["affected_roles"][:3]) or "—"
            mig_md += f"| {m['priority']} | {m['action'][:70]} | {m['from_tool']} | {m['to_tool'][:50]} | {effort_icon.get(m['effort'], '⚪')} {m['effort']} | {roles} |\n"

        mig_md += "\n### Rationale\n"
        for m in body["migration_steps"]:
            mig_md += f"- **Step {m['priority']}:** {m['rationale']}\n"
    else:
        mig_md += "✅ No migration steps needed — your tools are already well-consolidated."

    # --- Unified schema
    schema_md = "## 📐 Recommended Unified Schemas\n\n"
    if body["unified_schemas"]:
        for schema in body["unified_schemas"]:
            schema_md += f"### `{schema['table_name']}` — {schema['purpose']}\n\n"
            schema_md += "| Column | Type | Source | Notes |\n|---|---|---|---|\n"
            for col in schema["columns"]:
                schema_md += f"| `{col['name']}` | {col['dtype']} | {col['source'][:50]} | {col['notes']} |\n"
            schema_md += "\n"
    else:
        schema_md += "No schema recommendations generated."

    # --- Recommendations
    recs_md = "## 🎯 Top Recommendations\n\n"
    if body["top_recommendations"]:
        for i, rec in enumerate(body["top_recommendations"], 1):
            recs_md += f"{i}. {rec}\n\n"
    else:
        recs_md += "No specific recommendations at this time."

    return summary, silos_md, mig_md, schema_md, recs_md


# ---------------------------------------------------------------------------
# Module 6 — ROI Estimator
# ---------------------------------------------------------------------------

def run_roi(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/roi",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'Missing prerequisites. Run Module 4 and/or 5 first.')}", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()
    s = body["summary"]

    # --- Summary card ---
    payback_str = f"{s['overall_payback_months']:.1f} months" if s['overall_payback_months'] else "N/A"
    y1_sign = "+" if s['net_first_year_benefit'] >= 0 else ""
    y3_sign = "+" if s['three_year_net_benefit'] >= 0 else ""
    y1_icon = "🟢" if s['net_first_year_benefit'] >= 0 else "🔴"
    y3_icon = "🟢" if s['three_year_net_benefit'] >= 0 else "🔴"

    summary_md = f"""## 💰 ROI Estimate

| Metric | Value |
|---|---|
| Hours wasted/week (current) | **{s['total_current_hours_per_week']:.1f} hrs** |
| Hours saved/week (projected) | **{s['total_hours_saved_per_week']:.1f} hrs** |
| Annual hours saved | **{s['total_annual_hours_saved']:,.0f} hrs** |
| Annual cost saved | **₹{s['total_annual_cost_saved']:,.0f}** |
| Implementation cost (one-time) | **₹{s['total_implementation_cost']:,.0f}** |
| {y1_icon} Net first-year benefit | **{y1_sign}₹{s['net_first_year_benefit']:,.0f}** |
| {y3_icon} 3-year net benefit | **{y3_sign}₹{s['three_year_net_benefit']:,.0f}** |
| Payback period | **{payback_str}** |
| Annual ROI | **{s['roi_percentage']:.0f}%** |

{body['executive_summary']}
"""

    # --- Assumptions ---
    assumptions_md = "## 📋 Assumptions\n\n"
    assumptions_md += "| Assumption | Value | Source |\n|---|---|---|\n"
    for a in body["assumptions"]:
        assumptions_md += f"| {a['label']} | {a['value']} | {a['source']} |\n"
    assumptions_md += "\n*All estimates are conservative (low-end of industry benchmarks). Actual savings may be higher.*\n"

    # --- Line items ---
    lines_md = ""

    if body["automation_lines"]:
        lines_md += "## 🤖 Automation Savings (per step)\n\n"
        lines_md += "| Step | Type | Hrs Saved/wk | Annual Saved | Impl. Cost | Payback | Priority |\n"
        lines_md += "|---|---|---|---|---|---|---|\n"
        for l in body["automation_lines"]:
            pb = f"{l['payback_months']:.0f} mo" if l['payback_months'] else "—"
            priority_icon = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}.get(l['priority'], "⚪")
            lines_md += (
                f"| {l['step_number']}. {l['description'][:45]} "
                f"| {l['automation_type']} "
                f"| {l['hours_saved_per_week']:.1f} "
                f"| ₹{l['annual_cost_saved']:,.0f} "
                f"| ₹{l['implementation_cost']:,.0f} "
                f"| {pb} "
                f"| {priority_icon} {l['priority']} |\n"
            )

    if body["consolidation_lines"]:
        lines_md += "\n## 📦 Consolidation Savings (per migration)\n\n"
        lines_md += "| From → To | Overhead/wk | Saved/wk | Annual Saved | Impl. Cost | Payback |\n"
        lines_md += "|---|---|---|---|---|---|\n"
        for l in body["consolidation_lines"]:
            pb = f"{l['payback_months']:.0f} mo" if l['payback_months'] else "—"
            lines_md += (
                f"| {l['from_tool']} → {l['to_tool'][:30]} "
                f"| {l['current_overhead_hours_per_week']:.1f} hrs "
                f"| {l['hours_saved_per_week']:.1f} hrs "
                f"| ₹{l['annual_cost_saved']:,.0f} "
                f"| ₹{l['implementation_cost']:,.0f} "
                f"| {pb} |\n"
            )

    # --- Recommendations ---
    recs_md = "## 🎯 ROI-Based Recommendations\n\n"
    if body["top_recommendations"]:
        for i, rec in enumerate(body["top_recommendations"], 1):
            recs_md += f"{i}. {rec}\n\n"
    else:
        recs_md += "No specific recommendations at this time."

    return summary_md, assumptions_md, lines_md, recs_md


# ---------------------------------------------------------------------------
# Module 7 — Strategic Verdict Generator
# ---------------------------------------------------------------------------

def run_verdict(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/verdict",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'No modules have been run yet.')}", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()

    # --- Overall verdict card ---
    score = body["overall_readiness_score"]
    verdict = body["verdict"]
    verdict_icons = {
        "AI-Ready": "🟢", "Partially Ready": "🟡",
        "Significant Gaps": "🟠", "Not Ready": "🔴",
    }
    v_icon = verdict_icons.get(verdict, "⚪")
    score_bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))

    verdict_md = f"""## {v_icon} Strategic Verdict: {verdict}

**Overall AI Readiness:** {score*100:.0f}%  `{score_bar}`

{body['verdict_summary']}
"""

    # --- Scorecard ---
    status_icons = {"Strong": "🟢", "Adequate": "🟡", "Weak": "🟠", "Critical": "🔴", "Not Run": "⚪"}
    scorecard_md = "## 📊 Module Scorecard\n\n"
    scorecard_md += "| Module | Status | Score | Headline |\n|---|---|---|---|\n"
    for sc in body["scorecard"]:
        icon = status_icons.get(sc["status"], "⚪")
        sc_score = f"{sc['score']*100:.0f}%" if sc["score"] is not None else "—"
        scorecard_md += f"| {sc['module_number']}. {sc['module']} | {icon} {sc['status']} | {sc_score} | {sc['headline']} |\n"

    # Key metrics
    km = body.get("key_metrics", {})
    if km:
        scorecard_md += "\n### Key Metrics\n\n| Metric | Value |\n|---|---|\n"
        for k, v in km.items():
            scorecard_md += f"| {k} | **{v}** |\n"

    # Strengths & Weaknesses
    sw_md = ""
    if body.get("strengths"):
        sw_md += "## ✅ Strengths\n\n"
        for s in body["strengths"]:
            sw_md += f"- {s}\n"
        sw_md += "\n"
    if body.get("weaknesses"):
        sw_md += "## ⚠️ Areas for Improvement\n\n"
        for w in body["weaknesses"]:
            sw_md += f"- {w}\n"
        sw_md += "\n"

    # Risks
    if body.get("risks"):
        severity_icons = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}
        sw_md += "## 🛡️ Risk Register\n\n"
        sw_md += "| Severity | Area | Risk | Mitigation |\n|---|---|---|---|\n"
        for r in body["risks"]:
            ri = severity_icons.get(r["severity"], "⚪")
            sw_md += f"| {ri} {r['severity']} | {r['area']} | {r['description'][:70]} | {r['mitigation'][:70]} |\n"

    # Action plan
    actions_md = ""
    if body.get("action_plan"):
        actions_md += "## 🗺️ Implementation Roadmap\n\n"
        actions_md += "| # | Action | Module | Effort | Timeframe |\n|---|---|---|---|---|\n"
        for a in body["action_plan"]:
            effort_icon = {"Low": "🟢", "Medium": "🟡", "High": "🔴"}.get(a["effort"], "⚪")
            actions_md += (
                f"| {a['priority']} | {a['action'][:65]} "
                f"| {a['source_module']} "
                f"| {effort_icon} {a['effort']} "
                f"| {a['timeframe']} |\n"
            )
        actions_md += "\n"
        for a in body["action_plan"]:
            actions_md += f"**{a['priority']}. {a['action']}**\n"
            actions_md += f"> *Impact:* {a['impact']}\n\n"

    return verdict_md, scorecard_md, sw_md, actions_md


# ---------------------------------------------------------------------------
# Module 3 — Industry Benchmarking
# ---------------------------------------------------------------------------

def run_benchmark(session_id: str, product_name: str, price: float,
                  currency: str, features: str, category: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1a first.", "", "", ""
    if not product_name.strip():
        return "❌ Enter a product name.", "", "", ""

    feature_list = [f.strip() for f in features.split(",") if f.strip()]
    if not feature_list:
        return "❌ Enter at least one feature.", "", "", ""

    payload = {
        "session_id": session_id.strip(),
        "product_name": product_name,
        "price": price,
        "currency": currency,
        "features": feature_list,
        "category": category,
    }

    try:
        resp = requests.post(f"{API_BASE}/analyze/benchmark", json=payload, timeout=60)
    except requests.ConnectionError:
        return "❌ Cannot reach backend.", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()
    stats = body["market_stats"]

    def price_gap_label(gap: float) -> str:
        if gap < 0:
            return f"🔵 {abs(gap):.1f}% **below** market average"
        elif gap > 0:
            return f"🔴 {gap:.1f}% **above** market average"
        return "🟢 **exactly at** market average"

    summary = f"""## {_badge(body['price_position'])} Market Position

| Metric | Value |
|---|---|
| Your Price | {body['currency']} {body['user_price']:,.0f} |
| Market Average | {body['currency']} {stats['avg_price']:,.0f} |
| Market Range | {body['currency']} {stats['min_price']:,.0f} — {body['currency']} {stats['max_price']:,.0f} |
| Price Gap | {price_gap_label(body['price_gap_pct'])} |
| Price Percentile | Cheaper than **{body['price_percentile']:.0f}%** of competitors |
| Feature Match | **{body['feature_match_score']:.0f}%** keyword overlap |
| Competitors Sampled | {stats['sample_size']} |
"""
    if body.get("warnings"):
        summary += "\n**⚠️ Warnings:**\n" + "\n".join(f"- {w}" for w in body["warnings"])

    # --- Competitors table
    comp_md = "## 🏆 Closest Competitors\n\n| Competitor | Product | Price | Rating | Features |\n|---|---|---|---|---|\n"
    for c in body["top_competitors"]:
        rating = f"⭐ {c['rating']}" if c.get("rating") else "—"
        feats = c["features"][:50] + "…" if len(c["features"]) > 50 else c["features"]
        comp_md += f"| {c['competitor_name']} | {c['product_name']} | {body['currency']} {c['price']:,.0f} | {rating} | {feats} |\n"

    # --- LLM strategy
    if body.get("competitiveness_score") is not None:
        score = body["competitiveness_score"]
        score_bar = "█" * (score // 5) + "░" * (20 - score // 5)
        sp = body.get("suggested_price")
        sp_text = f"{body['currency']} {sp:,.0f}" if sp is not None else "N/A"
        strategy_md = f"""## 🤖 Gemini Strategy Analysis

**Competitiveness Score:** {score}/100  `{score_bar}`
**Confidence:** {body.get('llm_confidence', '—')}

### Recommendation
{body.get('strategic_recommendation', '—')}

**Suggested Optimal Price:** {sp_text}

### Key Insights
"""
        for insight in body.get("key_insights", []):
            strategy_md += f"- {insight}\n"
    else:
        strategy_md = "## 🤖 Gemini Strategy\n\n⚠️ LLM analysis was skipped (check GEMINI_API_KEY or too few data points)."

    return summary, comp_md, strategy_md


# ---------------------------------------------------------------------------
# Build the Gradio UI
# ---------------------------------------------------------------------------

with gr.Blocks(title="FoundationIQ — Test Console", theme=gr.themes.Soft()) as demo:

    gr.Markdown("""
# 🧠 FoundationIQ — Test Console
**AI Readiness & Automation Diagnostic Platform for SMEs**

> Backend must be running: `cd backend && uvicorn app.main:app --reload`
>
> **Flow:** Run Module 1a → copy the Session ID → use it in Modules 2, 3, 4, and 5
    """)

    # -----------------------------------------------------------------------
    # Module 1a — Tabular Ingestion
    # -----------------------------------------------------------------------
    with gr.Tab("📂 Module 1a — Tabular Ingestion"):
        gr.Markdown("Upload a CSV or Excel file + describe the company's workflow.")

        with gr.Row():
            with gr.Column(scale=1):
                t_file = gr.File(label="Upload CSV / Excel (Sales Data) *required*", file_types=[".csv", ".xlsx", ".xls"])
                t_workflow = gr.Textbox(
                    label="Workflow Description",
                    placeholder="Describe how this data is used in the business...\ne.g. Customer orders come via WhatsApp. Admin enters into Excel...",
                    lines=5,
                )
                with gr.Group():
                    gr.Markdown("**Supplementary Documents** *(optional — improves Data Coverage score)*")
                    t_invoice_file   = gr.File(label="Invoice File (PDF/CSV/Excel)",   file_types=[".pdf", ".csv", ".xlsx", ".xls"])
                    t_payroll_file   = gr.File(label="Payroll File (PDF/CSV/Excel)",   file_types=[".pdf", ".csv", ".xlsx", ".xls"])
                    t_inventory_file = gr.File(label="Inventory File (PDF/CSV/Excel)", file_types=[".pdf", ".csv", ".xlsx", ".xls"])
                with gr.Group():
                    gr.Markdown("**Company Metadata**")
                    t_industry = gr.Textbox(label="Industry", value="Retail")
                    t_employees = gr.Textbox(label="Number of Employees", value="50")
                    t_tools = gr.Textbox(label="Tools Used (comma-separated)", value="Excel, WhatsApp, Tally")
                t_submit = gr.Button("🚀 Run Ingestion", variant="primary")

            with gr.Column(scale=2):
                t_summary   = gr.Markdown(label="Summary")
                t_issues    = gr.Markdown(label="Issues")
                t_cols      = gr.Markdown(label="Columns")
                t_workflow_out = gr.Markdown(label="Workflow Analysis")
                t_mermaid   = gr.HTML(label="Workflow Diagram")

        t_submit.click(
            run_tabular_ingest,
            inputs=[t_file, t_workflow, t_industry, t_employees, t_tools,
                    t_invoice_file, t_payroll_file, t_inventory_file],
            outputs=[t_summary, t_issues, t_cols, t_workflow_out, t_mermaid],
        )

    # -----------------------------------------------------------------------
    # Module 1b — Document Ingestion
    # -----------------------------------------------------------------------
    with gr.Tab("📄 Module 1b — Document Ingestion"):
        gr.Markdown("Upload a PDF, Word (.docx), or TXT file.")

        with gr.Row():
            with gr.Column(scale=1):
                d_file = gr.File(label="Upload Document", file_types=[".pdf", ".docx", ".txt"])
                d_type = gr.Dropdown(
                    label="Document Type",
                    choices=["sop", "invoice", "ledger", "other"],
                    value="sop",
                )
                with gr.Group():
                    gr.Markdown("**Company Metadata**")
                    d_industry  = gr.Textbox(label="Industry", value="Retail")
                    d_employees = gr.Textbox(label="Number of Employees", value="50")
                    d_tools     = gr.Textbox(label="Tools Used (comma-separated)", value="Excel, WhatsApp")
                d_submit = gr.Button("🚀 Run Ingestion", variant="primary")

            with gr.Column(scale=2):
                d_summary  = gr.Markdown()
                d_text     = gr.Markdown()
                d_analysis = gr.Markdown()

        d_submit.click(
            run_document_ingest,
            inputs=[d_file, d_type, d_industry, d_employees, d_tools],
            outputs=[d_summary, d_text, d_analysis],
        )

    # -----------------------------------------------------------------------
    # Module 2 — Quality Analysis
    # -----------------------------------------------------------------------
    with gr.Tab("📊 Module 2 — Data Quality & AI Readiness"):
        gr.Markdown(
            "Paste the **Session ID** from Module 1a to score this dataset's AI readiness."
        )

        with gr.Row():
            with gr.Column(scale=1):
                q_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id from Module 1a…")
                q_submit = gr.Button("🔍 Analyze Quality", variant="primary")

            with gr.Column(scale=2):
                q_summary = gr.Markdown()
                q_recs    = gr.Markdown()
                q_cols    = gr.Markdown()

        q_submit.click(
            run_quality,
            inputs=[q_sid],
            outputs=[q_summary, q_recs, q_cols],
        )

    # -----------------------------------------------------------------------
    # Module 4 — Automation Opportunity Detector
    # -----------------------------------------------------------------------
    with gr.Tab("🤖 Module 4 — Automation Detector"):
        gr.Markdown(
            "Analyse workflow steps for automation opportunities.\n\n"
            "**Prerequisites:** Run **Module 1a** (workflow) + **Module 2** (quality) first."
        )

        with gr.Row():
            with gr.Column(scale=1):
                a_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                a_submit = gr.Button("⚙️ Detect Automation Opportunities", variant="primary")

            with gr.Column(scale=2):
                a_summary   = gr.Markdown()
                a_steps     = gr.Markdown()
                a_recs      = gr.Markdown()
                a_quickwins = gr.Markdown()

        a_submit.click(
            run_automation,
            inputs=[a_sid],
            outputs=[a_summary, a_steps, a_recs, a_quickwins],
        )

    # -----------------------------------------------------------------------
    # Module 5 — Data Consolidation
    # -----------------------------------------------------------------------
    with gr.Tab("🧩 Module 5 — Data Consolidation"):
        gr.Markdown(
            "Analyse scattered tools and data sources, then get a concrete "
            "consolidation strategy.\n\n"
            "**Prerequisites:** Run **Module 1a** (workflow + metadata)."
        )

        with gr.Row():
            with gr.Column(scale=1):
                c_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                c_submit = gr.Button("🧩 Analyse & Recommend", variant="primary")

            with gr.Column(scale=2):
                c_summary  = gr.Markdown()
                c_silos    = gr.Markdown()
                c_migration = gr.Markdown()
                c_schema   = gr.Markdown()
                c_recs     = gr.Markdown()

        c_submit.click(
            run_consolidation,
            inputs=[c_sid],
            outputs=[c_summary, c_silos, c_migration, c_schema, c_recs],
        )

    # -----------------------------------------------------------------------
    # Module 6 — ROI Estimator
    # -----------------------------------------------------------------------
    with gr.Tab("💰 Module 6 — ROI Estimator"):
        gr.Markdown(
            "Estimate time saved, cost saved, and annual savings from automation "
            "and consolidation recommendations.\n\n"
            "**Prerequisites:** Run **Module 4** (Automation) and/or **Module 5** (Consolidation)."
        )

        with gr.Row():
            with gr.Column(scale=1):
                r_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                r_submit = gr.Button("💰 Estimate ROI", variant="primary")

            with gr.Column(scale=2):
                r_summary     = gr.Markdown()
                r_assumptions = gr.Markdown()
                r_lines       = gr.Markdown()
                r_recs        = gr.Markdown()

        r_submit.click(
            run_roi,
            inputs=[r_sid],
            outputs=[r_summary, r_assumptions, r_lines, r_recs],
        )

    # -----------------------------------------------------------------------
    # Module 7 — Strategic Verdict
    # -----------------------------------------------------------------------
    with gr.Tab("📋 Module 7 — Strategic Verdict"):
        gr.Markdown(
            "Aggregate all module outputs into one executive diagnostic report.\n\n"
            "**Prerequisites:** Run at least one of Modules 2–6. "
            "The more modules you run, the richer the verdict."
        )

        with gr.Row():
            with gr.Column(scale=1):
                v_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                v_submit = gr.Button("📋 Generate Verdict", variant="primary")

            with gr.Column(scale=2):
                v_verdict   = gr.Markdown()
                v_scorecard = gr.Markdown()
                v_sw        = gr.Markdown()
                v_actions   = gr.Markdown()

        v_submit.click(
            run_verdict,
            inputs=[v_sid],
            outputs=[v_verdict, v_scorecard, v_sw, v_actions],
        )

    # -----------------------------------------------------------------------
    # Module 3 — Benchmarking
    # -----------------------------------------------------------------------
    with gr.Tab("🏆 Module 3 — Industry Benchmarking"):
        gr.Markdown(
            "Compare your product against the market. Paste a Session ID from Module 1a for company context."
        )
        gr.Markdown(
            "**Supported categories:** `hotel` · `restaurant` · `electronics` · `apparel` · `saas` · `consulting`"
        )

        with gr.Row():
            with gr.Column(scale=1):
                b_sid      = gr.Textbox(label="Session ID", placeholder="Paste session_id from Module 1a…")
                b_product  = gr.Textbox(label="Product / Service Name", placeholder="e.g. Sunrise Boutique Hotel")
                with gr.Row():
                    b_price    = gr.Number(label="Your Price", value=2800)
                    b_currency = gr.Textbox(label="Currency", value="INR")
                b_features = gr.Textbox(
                    label="Your Features (comma-separated)",
                    placeholder="e.g. AC, WiFi, breakfast, parking, rooftop",
                )
                b_category = gr.Dropdown(
                    label="Category",
                    choices=["hotel", "restaurant", "electronics", "apparel", "saas", "consulting"],
                    value="hotel",
                )
                b_submit = gr.Button("📈 Run Benchmark", variant="primary")

            with gr.Column(scale=2):
                b_summary   = gr.Markdown()
                b_comps     = gr.Markdown()
                b_strategy  = gr.Markdown()

        b_submit.click(
            run_benchmark,
            inputs=[b_sid, b_product, b_price, b_currency, b_features, b_category],
            outputs=[b_summary, b_comps, b_strategy],
        )


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
