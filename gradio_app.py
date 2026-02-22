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
# Module 1 — Startup Ingestion & Profiling
# ---------------------------------------------------------------------------

def run_startup_ingest(company_name, sub_type, mrr_m1, mrr_m2, mrr_m3,
                       growth_goal, patience, tech_stack, employees, industry,
                       org_chart_file, expenses_file, sales_inquiries_file):
    if not company_name or not company_name.strip():
        return "❌ Please enter your startup name.", "", "", ""

    # Build MRR array
    try:
        mrr_list = [float(mrr_m1), float(mrr_m2), float(mrr_m3)]
    except (ValueError, TypeError):
        return "❌ MRR values must be numbers.", "", "", ""

    mrr_json = json.dumps(mrr_list)
    tech_str = tech_stack if isinstance(tech_stack, str) else ""

    try:
        emp = int(employees)
    except (ValueError, TypeError):
        emp = 1

    # Build form data
    form_data = {
        "company_name": company_name.strip(),
        "sub_type": sub_type,
        "mrr_last_3_months": mrr_json,
        "monthly_growth_goal_pct": float(growth_goal),
        "patience_months": int(patience),
        "current_tech_stack": tech_str,
        "num_employees": emp,
        "industry": industry or "Technology",
    }

    # Build files payload
    files_payload = {}

    def _add_file(key, upload):
        if upload is not None:
            up_path = upload if isinstance(upload, str) else upload.name
            up_name = up_path.split("\\")[-1].split("/")[-1]
            up_ext = up_name.rsplit(".", 1)[-1].lower()
            up_mime = "text/csv" if up_ext == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            with open(up_path, "rb") as f:
                up_bytes = f.read()
            files_payload[key] = (up_name, io.BytesIO(up_bytes), up_mime)

    _add_file("org_chart_file", org_chart_file)
    _add_file("expenses_file", expenses_file)
    _add_file("sales_inquiries_file", sales_inquiries_file)

    try:
        resp = requests.post(
            f"{API_BASE}/ingest/startup",
            files=files_payload if files_payload else None,
            data=form_data,
            timeout=90,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is `uvicorn app.main:app --reload` running?", "", "", ""

    if resp.status_code != 200:
        detail = resp.json().get("detail", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        return f"❌ Error {resp.status_code}: {detail}", "", "", ""

    body = resp.json()
    sid = body["session_id"]
    profile = body.get("startup_profile", {})
    files_up = body.get("files_uploaded", [])
    total_issues = body.get("total_issues", 0)
    total_rows = body.get("total_rows", 0)

    # --- Summary card
    mrr_vals = profile.get("mrr_last_3_months", [0, 0, 0])
    summary = f"""## ✅ Startup Ingestion Successful

**Session ID** (copy this for Modules 2-7):
```
{sid}
```
| | |
|---|---|
| Company | {profile.get('company_name', 'N/A')} |
| Vertical | {profile.get('sub_type', 'N/A')} |
| MRR Trend | ₹{mrr_vals[0]:,.0f} → ₹{mrr_vals[1]:,.0f} → ₹{mrr_vals[2]:,.0f} |
| Growth Goal | {profile.get('monthly_growth_goal_pct', 0)}% MoM |
| Patience | {profile.get('patience_months', 0)} months |
| Team Size | {profile.get('num_employees', 0)} |
| Tech Stack | {', '.join(profile.get('current_tech_stack', [])) or 'None'} |
| Files Uploaded | {', '.join(f.replace('_', ' ').title() for f in files_up) or 'None'} |
| Total Rows | {total_rows:,} |
| Total Issues | {total_issues} |
"""

    # --- Per-file issues
    issues_md = ""
    severity_icon = {"high": "🔴", "medium": "🟡", "low": "🟢"}
    for file_key in ["org_chart", "expenses", "sales_inquiries"]:
        file_data = body.get(file_key)
        if file_data is None:
            continue
        file_issues = file_data.get("data_issues", [])
        label = file_key.replace("_", " ").title()
        issues_md += f"\n### 📁 {label} ({file_data['filename']})\n"
        issues_md += f"Rows: {file_data['row_count']} | Columns: {file_data['column_count']}\n\n"

        if file_issues:
            for issue in file_issues:
                icon = severity_icon.get(issue["severity"], "⚪")
                col = f" → `{issue['column']}`" if issue.get("column") else ""
                issues_md += f"- {icon} **{issue['issue_type'].replace('_',' ').title()}**{col}: {issue['description']}\n"
        else:
            issues_md += "- ✅ No issues found\n"

    if not issues_md:
        issues_md = "## ℹ️ No CSV files were uploaded\n\nUpload org_chart.csv, expenses.csv, or sales_inquiries.csv for data quality checks."
    else:
        issues_md = f"## 🔍 File Quality Report\n{issues_md}"

    # --- Column details per file
    cols_md = ""
    for file_key in ["org_chart", "expenses", "sales_inquiries"]:
        file_data = body.get(file_key)
        if file_data is None:
            continue
        label = file_key.replace("_", " ").title()
        cols_md += f"\n### 📊 {label} Columns\n\n"
        cols_md += "| Column | Type | Non-Null | Completeness |\n|---|---|---|---|\n"
        for c in file_data.get("columns", []):
            completeness = (100 - c["missing_pct"]) / 100
            bar = "█" * int(completeness * 10) + "░" * (10 - int(completeness * 10))
            total = c["non_null_count"] + c["null_count"]
            cols_md += f"| `{c['name']}` | {c['dtype']} | {c['non_null_count']}/{total} | {bar} {completeness*100:.0f}% |\n"

    if not cols_md:
        cols_md = ""
    else:
        cols_md = f"## 📊 Column Summary{cols_md}"

    # --- Profile analysis
    pa = body.get("profile_analysis")
    if pa:
        analysis_md = f"""## 🧠 Startup Profile Analysis

| Metric | Value |
|---|---|
| MRR Trend | **{pa.get('mrr_trend', 'N/A')}** |
| Avg MoM Growth | {pa.get('mrr_mom_growth_pct', 0):.1f}% |
| Growth Gap | {pa.get('growth_gap', 'N/A')} |
| Tech Stack Maturity | **{pa.get('tech_stack_maturity', 'N/A')}** |

### Key Observations
"""
        for obs in pa.get("key_observations", []):
            analysis_md += f"- {obs}\n"

        analysis_md += "\n### Recommended Focus Areas\n"
        for area in pa.get("recommended_focus_areas", []):
            analysis_md += f"- 🎯 {area}\n"

        analysis_md += f"\n### Executive Summary\n{pa.get('executive_summary', '')}"
    else:
        analysis_md = "## 🧠 Profile Analysis\n\n⚠️ LLM analysis was skipped (check GEMINI_API_KEY)."

    return summary, issues_md, cols_md, analysis_md


# ---------------------------------------------------------------------------
# Module 2 — Data Quality & DPDP Compliance Scanner
# ---------------------------------------------------------------------------

def run_quality(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/quality",
            data={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend.", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired. Re-run Module 1 to get a fresh session_id.", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()

    def score_bar(score: float) -> str:
        filled = int(score * 20)
        return "█" * filled + "░" * (20 - filled) + f"  {score*100:.1f}%"

    dq_score = body.get('data_quality_score', body.get('ai_readiness_score', 0))
    quality_pass = body.get('quality_pass', dq_score >= 0.85)
    pass_badge = "✅ PASS" if quality_pass else "⚠️ CLEANUP NEEDED"

    docs_provided = body.get('documents_provided', [])
    docs_str = ', '.join(d.replace('_', ' ').title() for d in docs_provided) if docs_provided else 'None'

    summary = f"""## {_badge(body['readiness_level'])} Data Quality Score: **{dq_score*100:.1f}%** {pass_badge}

### 📋 Data Quality Dimensions

| Dimension | Weight | Score | Bar |
|---|---|---|---|
| Completeness | 25% | {body['completeness_score']*100:.1f}% | {score_bar(body['completeness_score'])} |
| Deduplication | 20% | {body['deduplication_score']*100:.1f}% | {score_bar(body['deduplication_score'])} |
| Consistency | 15% | {body['consistency_score']*100:.1f}% | {score_bar(body['consistency_score'])} |
| Structural Integrity | 10% | {body['structural_integrity_score']*100:.1f}% | {score_bar(body['structural_integrity_score'])} |

### 🔧 Operational Dimensions

| Dimension | Weight | Score | Bar |
|---|---|---|---|
| Process Digitisation | 15% | {body['process_digitisation_score']*100:.1f}% | {score_bar(body['process_digitisation_score'])} |
| Tool Maturity | 5% | {body['tool_maturity_score']*100:.1f}% | {score_bar(body['tool_maturity_score'])} |
| Data Coverage | 10% | {body.get('data_coverage_score', 0)*100:.1f}% | {score_bar(body.get('data_coverage_score', 0))} |

**CSV files uploaded:** {docs_str}
**Workflow:** {body.get('total_workflow_steps', 0)} steps · {body.get('automated_steps', 0)} automated · {body.get('manual_steps', 0)} manual
**Tools detected:** {', '.join(body.get('tools_detected', [])) or 'None'}
**Dataset:** {body['row_count']} rows · {body['column_count']} columns · {body['missing_cells']} missing cells · {body['duplicate_rows']} duplicate rows
"""

    recs_md = "## 💡 Recommendations\n\n"
    if body["top_recommendations"]:
        for i, rec in enumerate(body["top_recommendations"], 1):
            recs_md += f"**{i}.** {rec}\n\n"
    else:
        recs_md += "✅ No critical issues to address."

    # --- DPDP Compliance section ---
    dpdp = body.get("dpdp_compliance", {})
    dpdp_risk = dpdp.get("risk_level", "Low")
    risk_colors = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}
    risk_icon = risk_colors.get(dpdp_risk, "⚪")
    llm_safe = dpdp.get("llm_api_safe", True)
    llm_badge = "✅ Safe for LLM APIs" if llm_safe else "🚫 NOT safe for LLM APIs — anonymise first"

    dpdp_md = f"""## 🛡️ DPDP Compliance Scan (Metric 6)

### {risk_icon} Risk Level: **{dpdp_risk}** | {llm_badge}

**PII columns found:** {dpdp.get('total_pii_columns', 0)} | **PII values detected:** {dpdp.get('total_pii_values', 0)}

"""

    findings = dpdp.get("pii_findings", [])
    if findings:
        dpdp_md += "### PII Findings\n\n"
        dpdp_md += "| Column | PII Type | Matches | Exposure | Risk | Recommendation |\n"
        dpdp_md += "|---|---|---|---|---|---|\n"
        for f in findings:
            dpdp_md += (
                f"| `{f['column']}` | {f['pii_type']} | {f['sample_count']} "
                f"| {f['exposure_pct']:.1f}% | {f['risk_level']} "
                f"| {f['recommendation'][:80]}… |\n"
            )
        dpdp_md += "\n"

    warnings = dpdp.get("compliance_warnings", [])
    if warnings:
        dpdp_md += "### Compliance Warnings\n\n"
        for w in warnings:
            dpdp_md += f"⚠️ {w}\n\n"

    col_md = "## 📊 Per-Column Quality\n\n| Column | Type | Completeness | Issues | PII |\n|---|---|---|---|---|\n"
    for col in body["column_quality"]:
        issues = ", ".join(col["issue_types"]) if col["issue_types"] else "—"
        pii = ", ".join(col.get("pii_types", [])) if col.get("pii_types") else "—"
        bar = "█" * int(col["completeness"] * 10) + "░" * (10 - int(col["completeness"] * 10))
        col_md += f"| `{col['name']}` | {col['dtype']} | {bar} {col['completeness']*100:.0f}% | {issues} | {pii} |\n"

    return summary, recs_md, dpdp_md, col_md


# ---------------------------------------------------------------------------
# Module 4 — Organizational Role & Automation Auditor
# ---------------------------------------------------------------------------

def run_role_audit(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/role-audit",
            json={"session_id": session_id.strip()},
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
    rpe  = body["rpe_metrics"]

    # ── Summary card ──────────────────────────────────────────────────────
    vuln_bar = (
        f"🔴 High: **{body['high_vulnerability_count']}**  |  "
        f"🟡 Medium: **{body['medium_vulnerability_count']}**  |  "
        f"🟢 Low: **{body['low_vulnerability_count']}**"
    )
    summary = f"""## 🧑‍💼 Role & Automation Audit

| Metric | Value |
|---|---|
| Employees Audited | {body['total_employees']} |
| Avg Role Automation % | **{body['avg_automation_pct']:.0f}%** (Metric 3) |
| Top Automatable Role | {body['top_automatable_role']} ({body['top_automatable_pct']:.0f}%) |
| Total Hours Freed/Week | **{body['total_hours_saved_per_week']:.0f}h** |
| Vulnerability Split | {vuln_bar} |
| Current RPE | ₹{rpe['current_rpe_monthly']:,.0f} / employee / month |
| Projected RPE | ₹{rpe['projected_rpe_monthly']:,.0f} / employee / month |
| RPE Lift | **+{rpe['rpe_lift_pct']:.0f}%** in {rpe['growth_months_used']} months (Metric 8) |
| RPE Lift (INR) | +₹{rpe['rpe_lift_inr']:,.0f} per employee / month |
"""
    if body.get("warnings"):
        summary += "\n**⚠️ Warnings:**\n" + "\n".join(f"- {w}" for w in body["warnings"])

    # ── Role vulnerability matrix ─────────────────────────────────────────
    roles_md = "## 📊 Role Vulnerability Matrix (Metric 3)\n\n"
    roles_md += "| Employee | Job Title | Dept | Automation % | Vulnerability | Hours Saved/Wk | Upskilling |\n"
    roles_md += "|---|---|---|---|---|---|---|\n"
    vuln_icons = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
    for r in body["roles"]:
        icon  = vuln_icons.get(r["vulnerability_level"], "⚪")
        tasks = ", ".join(r["automatable_tasks"][:2])
        skill = r["upskilling_rec"].split(",")[0].strip()
        roles_md += (
            f"| {r['name']} | {r['job_title']} | {r['department']} "
            f"| **{r['automation_pct']:.0f}%** | {icon} {r['vulnerability_level']} "
            f"| {r['hours_saved_per_week']:.0f}h | {skill} |\n"
        )
    roles_md += "\n### 🔧 Automatable Tasks (sample per role)\n"
    for r in body["roles"]:
        if r["vulnerability_level"] in ("High", "Medium"):
            tasks_str = " · ".join(r["automatable_tasks"][:3])
            roles_md += f"- **{r['job_title']}:** {tasks_str}\n"

    # ── Recommendations ───────────────────────────────────────────────────
    recs_md = "## 🎯 Recommendations\n\n"
    if body["recommendations"]:
        for rec in body["recommendations"]:
            recs_md += f"{rec}\n\n"
    else:
        recs_md += "No recommendations at this time."

    # ── Mermaid vulnerability chart ───────────────────────────────────────
    mermaid = body.get("mermaid_chart", "")
    mermaid_md = f"## 🗺️ Org Vulnerability Map\n\n```mermaid\n{mermaid}\n```" if mermaid else ""

    return summary, roles_md, recs_md, mermaid_md



# ---------------------------------------------------------------------------
# Module 5 — Financial Impact & ROI Simulator
# ---------------------------------------------------------------------------

def run_financial_impact(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/financial-impact",
            json={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'Run Module 4 first.')}", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()

    # --- Before / After dashboard ---
    dashboard_md = f"""## 💰 Financial Impact Report

{body.get('headline', '')}

| Metric | Before | After | Delta |
|---|---|---|---|
"""
    for row in body.get("before_after", []):
        dashboard_md += (
            f"| {row['icon']} **{row['metric']}** "
            f"| {row['before_value']} "
            f"| {row['after_value']} "
            f"| {row['delta']} |\n"
        )

    dashboard_md += f"""

### P&L Summary

| Item | Value |
|---|---|
| Current MRR | ₹{body['current_mrr']:,.0f} |
| Total Monthly Costs | ₹{body['total_monthly_costs_inr']:,.0f} |
| Payroll | ₹{body['total_payroll_monthly_inr']:,.0f} |
| Recurring OpEx | ₹{body['total_recurring_expenses_inr']:,.0f} |
| **Net Monthly Savings (M5)** | **₹{body['net_monthly_savings_inr']:,.0f}** |
| **Annual Savings** | **₹{body['net_annual_savings_inr']:,.0f}** |
| **Margin Lift (M12)** | **+{body['gross_margin_lift_pct']:.1f}pp** |
| **Opp. Cost/Month (M7)** | **₹{body['opportunity_cost_per_month_inr']:,.0f}** |
"""
    if body.get("months_to_break_even"):
        dashboard_md += f"| Break-even | {body['months_to_break_even']:.1f} months |\n"

    # --- Employee savings breakdown ---
    savings_md = "## 🧑‍💼 Employee Savings Breakdown\n\n"
    if body.get("employee_savings"):
        savings_md += "| # | Name | Role | Salary/mo | Hrs Saved/wk | Loaded Rate | Monthly Savings |\n"
        savings_md += "|---|---|---|---|---|---|---|\n"
        for i, emp in enumerate(body["employee_savings"], 1):
            savings_md += (
                f"| {i} | {emp['name']} | {emp['job_title']} "
                f"| ₹{emp['monthly_salary_inr']:,.0f} "
                f"| {emp['hours_saved_per_week']:.1f}h "
                f"| ₹{emp['loaded_hourly_rate_inr']:,.0f}/hr "
                f"| ₹{emp['gross_monthly_savings_inr']:,.0f} |\n"
            )
        savings_md += f"\n**Total gross savings: ₹{body['gross_monthly_savings_inr']:,.0f}/month**"
    else:
        savings_md += "No high/medium vulnerability roles found — no savings to compute."

    # --- AI tools ---
    tools_md = "## 🤖 Recommended AI Tools\n\n"
    if body.get("ai_tool_recommendations"):
        tools_md += "| Tool | Purpose | For Role | Cost/mo | In Stack? |\n"
        tools_md += "|---|---|---|---|---|\n"
        for t in body["ai_tool_recommendations"]:
            in_stack = "✅ Yes (₹0)" if t["already_in_stack"] else "❌ New"
            cost_str = "₹0" if t["monthly_cost_inr"] == 0 else f"₹{t['monthly_cost_inr']:,}"
            tools_md += (
                f"| **{t['tool_name']}** | {t['purpose'][:60]} "
                f"| {t['for_role_category']} "
                f"| {cost_str} "
                f"| {in_stack} |\n"
            )
        tools_md += f"\n**New AI tool cost: ₹{body['new_ai_tools_monthly_cost_inr']:,.0f}/month**"
    else:
        tools_md += "No tool recommendations at this time."

    # --- Executive narrative ---
    narrative_md = "## 📋 Executive Summary\n\n"
    narrative_md += body.get("executive_summary", "No summary available.")
    if body.get("warnings"):
        narrative_md += "\n\n### ⚠️ Warnings\n"
        for w in body["warnings"]:
            narrative_md += f"- {w}\n"

    return dashboard_md, savings_md, tools_md, narrative_md


# ---------------------------------------------------------------------------
# Module 6 — Growth & Retention Benchmarking
# ---------------------------------------------------------------------------

def run_retention(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/retention",
            json={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend. Is uvicorn running?", "", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", "", ""
    if resp.status_code == 422:
        return f"❌ {resp.json().get('detail', 'Upload sales_inquiries.csv in Module 1 first.')}", "", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", "", ""

    body = resp.json()

    # --- Overview card ---
    churn_delta = body["current_churn_pct"] - body["projected_churn_pct"]
    nrr_status  = "🟢 above" if body["projected_nrr_pct"] >= body["nrr_benchmark_pct"] else "🟡 below"

    overview_md = f"""## 📊 Growth & Retention Benchmarking

{body.get('headline', '')}

| Metric | Current | Projected | Benchmark |
|---|---|---|---|
| 📉 **Monthly Churn (Metric 9)** | {body['current_churn_pct']:.1f}% | {body['projected_churn_pct']:.1f}% | {body['industry_avg_churn_pct']:.1f}% avg / {body['top_tier_churn_pct']:.1f}% top |
| 💹 **NRR (Metric 10)** | {body['current_nrr_pct']:.0f}% | {body['projected_nrr_pct']:.0f}% | {body['nrr_benchmark_pct']:.0f}% ({nrr_status} benchmark) |

### Sales Funnel Summary

| Metric | Value |
|---|---|
| Total Inquiries | {body['total_inquiries']} |
| Closed Won | {body['closed_won_count']} |
| Repeat Customers (of Won) | {body['repeat_customer_count']} ({body['repeat_rate_pct']:.1f}%) |
| New Customers (of Won) | {body['new_customer_count']} |
| Lost | {body['lost_count']} |
| Pending | {body['pending_count']} |
| Win Rate | **{body['win_rate_pct']:.1f}%** |
"""
    if body.get("warnings"):
        overview_md += "\n⚠️ **Warnings:**\n" + "\n".join(f"- {w}" for w in body["warnings"])

    # --- Growth radar ---
    radar_md = "## 🎯 Growth Radar (vs 2026 Industry)\n\n"
    radar_md += "| Axis | Your Score | Industry Avg | Top Tier |\n"
    radar_md += "|---|---|---|---|\n"
    for pt in body.get("radar_data", []):
        bar = "█" * int(pt["startup_value"] / 10) + "░" * (10 - int(pt["startup_value"] / 10))
        radar_md += (
            f"| **{pt['axis']}** | `{bar}` {pt['startup_value']:.0f}/100 "
            f"| {pt['industry_avg']:.0f}/100 "
            f"| {pt['top_tier']:.0f}/100 |\n"
        )

    # --- Competitor benchmarks ---
    radar_md += "\n### 🏆 Competitor Benchmarks\n\n"
    if body.get("competitor_benchmarks"):
        radar_md += "| Company | Sector | Monthly Churn | NRR |\n|---|---|---|---|\n"
        for c in body["competitor_benchmarks"]:
            nrr_str = f"{c['nrr_pct']:.0f}%" if c.get("nrr_pct") else "—"
            radar_md += f"| {c['company']} | {c['sector']} | {c['churn_pct']:.1f}% | {nrr_str} |\n"

    # --- Growth levers + sector risks ---
    insights_md = "## 🚀 Growth Levers\n\n"
    for lever in body.get("growth_levers", []):
        insights_md += f"- {lever}\n"
    insights_md += "\n## ⚠️ Sector Risks\n\n"
    for risk in body.get("sector_risks", []):
        insights_md += f"- {risk}\n"

    # --- Executive summary ---
    exec_md = "## 📋 Executive Summary\n\n"
    exec_md += body.get("executive_summary", "No summary available.")

    return overview_md, radar_md, insights_md, exec_md




# ---------------------------------------------------------------------------
# Module 7 — Strategic Verdict Generator
# ---------------------------------------------------------------------------

def run_verdict(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", "", ""

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
# Module 3 — Workflow Bottleneck & Speed Analyzer
# ---------------------------------------------------------------------------

def run_bottleneck(session_id: str):
    if not session_id.strip():
        return "❌ Paste a Session ID from Module 1 first.", "", ""

    try:
        resp = requests.post(
            f"{API_BASE}/analyze/bottleneck",
            json={"session_id": session_id.strip()},
            timeout=30,
        )
    except requests.ConnectionError:
        return "❌ Cannot reach backend.", "", ""

    if resp.status_code == 404:
        return "❌ Session not found or expired.", "", ""
    if resp.status_code != 200:
        return f"❌ Error {resp.status_code}: {resp.json().get('detail', resp.text)}", "", ""

    body = resp.json()

    # --- Summary card -------------------------------------------------------
    bn_count = body["bottleneck_count"]
    bn_pct   = body["bottleneck_pct"]
    status_icon = "🟢" if bn_count == 0 else ("🟡" if bn_pct < 30 else "🔴")
    summary_md = f"""## {status_icon} Bottleneck & Speed Report

| Metric | Value |
|---|---|
| Total Inquiries | {body['total_inquiries']} |
| Closed Inquiries | {body['closed_inquiries']} |
| Avg TAT | {body['avg_tat_hours']:.1f}h |
| Median TAT | {body['median_tat_hours']:.1f}h |
| Max TAT | {body['max_tat_hours']:.1f}h |
| Bottlenecks (>48h) | {bn_count} ({bn_pct:.0f}%) |
| TAT Improvement % (Metric 11) | {body['avg_tat_improvement_pct']:.0f}% |
| Total Hours Saved (Metric 4) | {body['total_hours_saved']:.0f}h |
| Avg Hours Saved/Inquiry | {body['avg_hours_saved_per_inquiry']:.1f}h |
"""
    if body.get("recommendations"):
        summary_md += "\n**💡 Recommendations:**\n" + "\n".join(f"- {r}" for r in body["recommendations"])
    if body.get("warnings"):
        summary_md += "\n\n**⚠️ Warnings:**\n" + "\n".join(f"- {w}" for w in body["warnings"])

    # --- Per-inquiry TAT table -----------------------------------------------
    tat_rows = body.get("inquiry_tat_list", [])
    tat_md = "## 📋 Inquiry TAT Details\n\n"
    tat_md += "| Inquiry ID | Inquiry Date | Payment Date | TAT (h) | Bottleneck? | Status |\n"
    tat_md += "|---|---|---|---|---|---|\n"
    for row in tat_rows:
        tat_h = f"{row['tat_hours']:.1f}" if row["tat_hours"] is not None else "—"
        flag  = "🔴 Yes" if row["is_bottleneck"] else ("🟢 No" if row["tat_hours"] is not None else "—")
        tat_md += (
            f"| {row['inquiry_id']} | {row['inquiry_date']} | {row['payment_date'] or '—'} "
            f"| {tat_h} | {flag} | {row['status']} |\n"
        )

    # --- Mermaid flowchart --------------------------------------------------
    mermaid = body.get("mermaid_flowchart", "")
    mermaid_md = f"## 🧭 Pipeline Flowchart\n\n```mermaid\n{mermaid}\n```" if mermaid else ""

    return summary_md, tat_md, mermaid_md


# ---------------------------------------------------------------------------
# Build the Gradio UI
# ---------------------------------------------------------------------------

with gr.Blocks(title="FoundationIQ — Test Console", theme=gr.themes.Soft()) as demo:

    gr.Markdown("""
# 🧠 FoundationIQ 3.0 — Startup Edition
**AI Readiness & Automation Diagnostic Platform for Tech Startups**

> Backend must be running: `cd backend && uvicorn app.main:app --reload`
>
> **Flow:** Run Module 1 → copy the Session ID → use it in Modules 2–7
    """)

    # -----------------------------------------------------------------------
    # Module 1 — Startup Ingestion & Profiling
    # -----------------------------------------------------------------------
    with gr.Tab("🚀 Module 1 — Startup Ingestion"):
        gr.Markdown("Complete the onboarding form and upload your CSV files to begin the diagnostic.")

        with gr.Row():
            with gr.Column(scale=1):
                with gr.Group():
                    gr.Markdown("### 📋 Startup Onboarding (8 Questions)")
                    s_name = gr.Textbox(label="1. Company Name", placeholder="e.g. Acme SaaS")
                    s_subtype = gr.Dropdown(
                        label="2. Startup Vertical",
                        choices=["EdTech", "FinTech", "SaaS", "E-commerce"],
                        value="SaaS",
                    )
                    gr.Markdown("**3. MRR — Last 3 Months (₹)**")
                    with gr.Row():
                        s_mrr1 = gr.Number(label="Month 1 (oldest)", value=80000)
                        s_mrr2 = gr.Number(label="Month 2", value=95000)
                        s_mrr3 = gr.Number(label="Month 3 (latest)", value=110000)
                    s_growth = gr.Number(label="4. Target Monthly Growth Goal (%)", value=15.0)
                    s_patience = gr.Number(label="5. Months Willing to Wait for ROI", value=6, precision=0)
                    s_stack = gr.Textbox(
                        label="6. Current Tech Stack (comma-separated)",
                        placeholder="e.g. Stripe, Zapier, Google Sheets, Freshdesk",
                        value="Stripe, Google Sheets",
                    )
                    s_employees = gr.Number(label="7. Team Size", value=12, precision=0)
                    s_industry = gr.Textbox(label="8. Industry", value="Technology")

                with gr.Group():
                    gr.Markdown("### 📁 CSV Uploads *(at least one recommended)*")
                    s_org = gr.File(label="org_chart.csv (roles, departments, salaries)", file_types=[".csv", ".xlsx", ".xls"])
                    s_exp = gr.File(label="expenses.csv (category, amount, month)", file_types=[".csv", ".xlsx", ".xls"])
                    s_sales = gr.File(label="sales_inquiries.csv (inquiry_date, payment_date, repeat_customer)", file_types=[".csv", ".xlsx", ".xls"])

                s_submit = gr.Button("🚀 Run Startup Ingestion", variant="primary")

            with gr.Column(scale=2):
                s_summary   = gr.Markdown(label="Summary")
                s_issues    = gr.Markdown(label="File Issues")
                s_cols      = gr.Markdown(label="Columns")
                s_analysis  = gr.Markdown(label="Profile Analysis")

        s_submit.click(
            run_startup_ingest,
            inputs=[s_name, s_subtype, s_mrr1, s_mrr2, s_mrr3,
                    s_growth, s_patience, s_stack, s_employees, s_industry,
                    s_org, s_exp, s_sales],
            outputs=[s_summary, s_issues, s_cols, s_analysis],
        )

    # -----------------------------------------------------------------------
    # Module 2 — Data Quality & DPDP Compliance
    # -----------------------------------------------------------------------
    with gr.Tab("📊 Module 2 — Data Quality & DPDP Scanner"):
        gr.Markdown(
            "Paste the **Session ID** from Module 1 to score data quality and run a **DPDP compliance scan** for PII."
        )

        with gr.Row():
            with gr.Column(scale=1):
                q_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id from Module 1…")
                q_submit = gr.Button("🔍 Analyze Quality & DPDP", variant="primary")

            with gr.Column(scale=2):
                q_summary = gr.Markdown()
                q_recs    = gr.Markdown()
                q_dpdp    = gr.Markdown()
                q_cols    = gr.Markdown()

        q_submit.click(
            run_quality,
            inputs=[q_sid],
            outputs=[q_summary, q_recs, q_dpdp, q_cols],
        )

    # -----------------------------------------------------------------------
    # Module 4 — Automation Opportunity Detector
    # -----------------------------------------------------------------------
    with gr.Tab("�‍💼 Module 4 — Role & Automation Auditor"):
        gr.Markdown(
            "Audit every role in the org chart for automation potential and calculate RPE lift.\n\n"
            "**Prerequisites:** Run **Module 1** with org_chart.csv uploaded."
        )

        with gr.Row():
            with gr.Column(scale=1):
                a_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                a_submit = gr.Button("🧑‍💼 Run Role Audit", variant="primary")

            with gr.Column(scale=2):
                a_summary  = gr.Markdown()
                a_roles    = gr.Markdown()
                a_recs     = gr.Markdown()
                a_mermaid  = gr.Markdown()

        a_submit.click(
            run_role_audit,
            inputs=[a_sid],
            outputs=[a_summary, a_roles, a_recs, a_mermaid],
        )

    # -----------------------------------------------------------------------
    # Module 5 — Financial Impact Simulator
    # -----------------------------------------------------------------------
    with gr.Tab("💰 Module 5 — Financial Impact Simulator"):
        gr.Markdown(
            "CFO-level proof of AI investment ROI: net monthly savings, "
            "operating margin lift, and opportunity cost of delay.\n\n"
            "**Prerequisites:** Run **Module 1** (startup profile) + **Module 4** (role audit)."
        )

        with gr.Row():
            with gr.Column(scale=1):
                f_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                f_submit = gr.Button("💰 Run Financial Simulation", variant="primary")

            with gr.Column(scale=2):
                f_dashboard = gr.Markdown()
                f_savings   = gr.Markdown()
                f_tools     = gr.Markdown()
                f_narrative = gr.Markdown()

        f_submit.click(
            run_financial_impact,
            inputs=[f_sid],
            outputs=[f_dashboard, f_savings, f_tools, f_narrative],
        )

    # -----------------------------------------------------------------------
    # Module 6 — Growth & Retention Benchmarking
    # -----------------------------------------------------------------------
    with gr.Tab("📈 Module 6 — Retention Benchmarking"):
        gr.Markdown(
            "Benchmark your startup's churn and NRR against 2026 industry standards.\n\n"
            "**Metric 9:** Churn Reduction Potential · **Metric 10:** NRR Projection\n\n"
            "**Prerequisites:** Run **Module 1** with `sales_inquiries.csv` uploaded."
        )

        with gr.Row():
            with gr.Column(scale=1):
                ret_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id…")
                ret_submit = gr.Button("📈 Run Retention Benchmarking", variant="primary")

            with gr.Column(scale=2):
                ret_overview  = gr.Markdown()
                ret_radar     = gr.Markdown()
                ret_insights  = gr.Markdown()
                ret_exec      = gr.Markdown()

        ret_submit.click(
            run_retention,
            inputs=[ret_sid],
            outputs=[ret_overview, ret_radar, ret_insights, ret_exec],
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
    # Module 3 — Workflow Bottleneck & Speed Analyzer
    # -----------------------------------------------------------------------
    with gr.Tab("⏱️ Module 3 — Bottleneck Analyzer"):
        gr.Markdown(
            "Analyse inquiry-to-payment TAT from the uploaded **sales_inquiries.csv**. "
            "Flags any inquiry taking >48h as a bottleneck and calculates hours saved by automation."
        )

        with gr.Row():
            with gr.Column(scale=1):
                b_sid    = gr.Textbox(label="Session ID", placeholder="Paste session_id from Module 1…")
                b_submit = gr.Button("🔍 Analyze Bottlenecks", variant="primary")

            with gr.Column(scale=2):
                b_summary  = gr.Markdown()
                b_table    = gr.Markdown()
                b_mermaid  = gr.Markdown()

        b_submit.click(
            run_bottleneck,
            inputs=[b_sid],
            outputs=[b_summary, b_table, b_mermaid],
        )


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False)
