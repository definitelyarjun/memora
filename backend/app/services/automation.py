"""Module 4 — Organizational Role & Automation Auditor (pure-rules, no LLM).

Takes org_chart.csv from session and maps every job title to its automation
potential using a curated role database.

Pipeline
--------
1. Parse org_chart_df, detect columns case-insensitively
2. For each row, call _classify_role() to get automation_pct + tasks
3. Aggregate Metric 3 stats (avg %, vulnerability distribution)
4. Compute Metric 8 (RPE Lift) from startup_profile MRR + headcount
5. Generate Mermaid chart (role vulnerability matrix by department)
6. Generate tech-stack-aware recommendations

Role Database logic
-------------------
The `_ROLE_DB` maps lowercase keyword strings found INSIDE a job title
to a dict of:
  pct    – automation potential percentage (0-100)
  tasks  – list of task types that automation handles for this role
  skill  – upskilling recommendation once admin work is removed

Matching: longest-key-wins (most specific match). Fallback: 40% generic.
"""

from __future__ import annotations

import math
from collections import defaultdict

from app.core.session_store import SessionEntry
from app.schemas.automation import AutomationReport, RPEMetrics, RoleAnalysis


# ---------------------------------------------------------------------------
# Column variant detection
# ---------------------------------------------------------------------------

_ID_VARIANTS      = {"employee_id", "emp_id", "id", "employee id", "emp id"}
_NAME_VARIANTS    = {"name", "employee_name", "full_name", "employee name", "full name"}
_TITLE_VARIANTS   = {"job_title", "title", "role", "position", "designation",
                     "job title", "job role"}
_DEPT_VARIANTS    = {"department", "dept", "division", "team", "business_unit",
                     "department name"}
_SALARY_VARIANTS  = {"monthly_salary_inr", "salary", "monthly_salary", "salary_inr",
                     "monthly salary", "monthly salary inr", "ctc", "pay"}
_HOURS_VARIANTS   = {"hours_per_week", "hours per week", "weekly_hours",
                     "hours/week", "work_hours", "weekly hours"}


def _find_col(df, candidates: set[str]) -> str | None:
    """Return the first DataFrame column whose lowercase form is in candidates."""
    for col in df.columns:
        if col.lower().replace(" ", "_") in candidates or col.lower() in candidates:
            return col
    return None


# ---------------------------------------------------------------------------
# Role automation database
# ---------------------------------------------------------------------------
# Key   = lowercase keyword that appears INSIDE the job title string
# Value = dict(pct, tasks, skill)
#
# Matching strategy: collect all keys whose string is found in the lowercased
# title; keep the match with the LONGEST key (most specific).

_ROLE_DB: dict[str, dict] = {
    # ── C-suite / Founders ──────────────────────────────────────────────
    "founder": {
        "pct": 5,
        "tasks": ["Meeting scheduling", "Status update aggregation"],
        "skill": "AI strategy, product vision, delegation at scale",
    },
    "ceo": {
        "pct": 5,
        "tasks": ["Meeting scheduling", "Report consumption"],
        "skill": "AI strategy, board-level communication, organisational scaling",
    },
    "coo": {
        "pct": 15,
        "tasks": ["Performance reporting", "Process monitoring dashboards"],
        "skill": "Operations strategy, AI-driven process design",
    },
    "cfo": {
        "pct": 20,
        "tasks": ["Financial reporting", "Budget dashboard monitoring"],
        "skill": "Financial strategy, AI forecasting and modelling",
    },
    "cto": {
        "pct": 10,
        "tasks": ["Status report generation", "Meeting scheduling"],
        "skill": "AI/ML strategy, engineering org scaling",
    },
    "cpo": {
        "pct": 12,
        "tasks": ["Roadmap reporting", "Analytics dashboard review"],
        "skill": "Product strategy, AI-powered product discovery",
    },

    # ── Engineering ──────────────────────────────────────────────────────
    "vp engineering": {
        "pct": 15,
        "tasks": ["Sprint reporting", "Status updates to leadership"],
        "skill": "Engineering leadership, AI integration strategy",
    },
    "engineering manager": {
        "pct": 20,
        "tasks": ["Sprint reporting", "Code review workflow automation"],
        "skill": "Technical leadership, delivery strategy",
    },
    "tech lead": {
        "pct": 20,
        "tasks": ["Code review tooling", "CI/CD monitoring", "Sprint reporting"],
        "skill": "Technical strategy, system architecture",
    },
    "senior developer": {
        "pct": 28,
        "tasks": ["CI/CD pipeline setup", "Automated testing", "Documentation generation"],
        "skill": "System architecture, AI-assisted development, ML integration",
    },
    "senior dev": {
        "pct": 28,
        "tasks": ["CI/CD pipeline setup", "Automated testing", "Documentation generation"],
        "skill": "System architecture, AI-assisted development",
    },
    "senior engineer": {
        "pct": 28,
        "tasks": ["CI/CD pipeline", "Testing automation", "Documentation"],
        "skill": "Architecture, AI/ML integration",
    },
    "junior developer": {
        "pct": 40,
        "tasks": ["Boilerplate code generation", "Unit test writing", "Documentation", "Deployment scripts"],
        "skill": "System design, AI-assisted development, code review skills",
    },
    "junior dev": {
        "pct": 40,
        "tasks": ["Boilerplate code generation", "Unit test writing", "Documentation"],
        "skill": "System design, AI-assisted coding",
    },
    "junior engineer": {
        "pct": 40,
        "tasks": ["Boilerplate code", "Test writing", "Deployment automation"],
        "skill": "System design, architectural thinking",
    },
    "developer": {
        "pct": 35,
        "tasks": ["Testing automation", "Deployment scripts", "Documentation"],
        "skill": "Architecture, AI-assisted development",
    },
    "engineer": {
        "pct": 30,
        "tasks": ["Testing automation", "CI/CD", "Documentation"],
        "skill": "System architecture, AI integration",
    },
    "qa": {
        "pct": 70,
        "tasks": ["Test case generation", "Regression testing", "Bug report logging", "Coverage reporting"],
        "skill": "Test strategy, exploratory testing, quality engineering",
    },
    "quality assurance": {
        "pct": 70,
        "tasks": ["Automated regression testing", "Test case generation", "Bug triage"],
        "skill": "Quality strategy, exploratory and acceptance testing",
    },
    "devops": {
        "pct": 50,
        "tasks": ["Pipeline automation", "Monitoring dashboards", "Deployment scripts"],
        "skill": "Infrastructure strategy, platform engineering",
    },
    "data scientist": {
        "pct": 30,
        "tasks": ["Data pipeline automation", "Report generation", "Feature engineering scripts"],
        "skill": "MLOps, advanced AI research, model productionisation",
    },
    "data analyst": {
        "pct": 50,
        "tasks": ["Dashboard updates", "Report generation", "Data cleaning pipelines", "Export automation"],
        "skill": "Data science, ML model interpretation, strategic analytics",
    },
    "data engineer": {
        "pct": 40,
        "tasks": ["ETL pipeline automation", "Data quality monitoring", "Schema migration scripts"],
        "skill": "Data architecture, real-time streaming, platform engineering",
    },

    # ── Sales ────────────────────────────────────────────────────────────
    "sales director": {
        "pct": 25,
        "tasks": ["Pipeline reporting", "CRM data entry", "Meeting scheduling"],
        "skill": "Revenue strategy, team coaching, market expansion",
    },
    "sales manager": {
        "pct": 30,
        "tasks": ["Pipeline reports", "CRM updates", "Forecast consolidation"],
        "skill": "Sales strategy, frontline coaching, deal negotiation",
    },
    "sales development rep": {
        "pct": 70,
        "tasks": ["Cold email sequences", "Lead scoring", "CRM data entry", "Follow-up scheduling", "Meeting booking"],
        "skill": "Relationship-building, strategic qualification, complex sales conversations",
    },
    "sdr": {
        "pct": 70,
        "tasks": ["Cold email sequences", "Lead scoring", "CRM data entry", "Follow-up scheduling", "Meeting booking"],
        "skill": "Relationship-building, strategic qualification, enterprise sales calls",
    },
    "bdr": {
        "pct": 68,
        "tasks": ["Lead qualification", "Cold outreach", "CRM updates", "Pipeline reporting"],
        "skill": "Business development strategy, enterprise prospecting",
    },
    "account executive": {
        "pct": 45,
        "tasks": ["Quote generation", "Contract drafting", "CRM updates", "Follow-up email sequences"],
        "skill": "Strategic selling, enterprise deal navigation, negotiation",
    },
    "account manager": {
        "pct": 40,
        "tasks": ["Renewal tracking", "CRM status updates", "NPS data collection"],
        "skill": "Strategic account growth, executive relationship management",
    },
    "inside sales": {
        "pct": 60,
        "tasks": ["Lead follow-ups", "Demo scheduling", "CRM updates", "Status emails"],
        "skill": "Complex deal strategy, value-based selling",
    },

    # ── Customer Support / Success ───────────────────────────────────────
    "customer support": {
        "pct": 65,
        "tasks": ["FAQ chatbot responses", "Ticket routing & tagging", "Status update emails", "Escalation triage"],
        "skill": "Customer experience strategy, complex issue resolution, empathy leadership",
    },
    "support exec": {
        "pct": 65,
        "tasks": ["FAQ responses", "Ticket routing", "Status emails"],
        "skill": "Customer success strategy, AI-assisted support tools",
    },
    "customer service": {
        "pct": 65,
        "tasks": ["FAQ responses", "Ticket categorisation", "Refund processing", "Order status"],
        "skill": "Experience design, escalation strategy, customer retention",
    },
    "helpdesk": {
        "pct": 70,
        "tasks": ["Ticket routing", "Password reset automation", "FAQ resolution", "Status notifications"],
        "skill": "IT service strategy, complex troubleshooting",
    },
    "customer success": {
        "pct": 55,
        "tasks": ["Onboarding email sequences", "Health score monitoring", "NPS survey dispatch", "Renewal reminders"],
        "skill": "Strategic customer success, executive stakeholder management, churn prevention",
    },
    "csm": {
        "pct": 55,
        "tasks": ["Onboarding emails", "Health score alerts", "Renewal workflows"],
        "skill": "Strategic customer success, executive relationship management",
    },

    # ── HR / People & Culture ────────────────────────────────────────────
    "hr": {
        "pct": 75,
        "tasks": ["Payroll processing", "Leave balance tracking", "Onboarding document generation", "Attendance monitoring", "Compliance checklists"],
        "skill": "People strategy, culture design, organisational development, talent planning",
    },
    "human resources": {
        "pct": 75,
        "tasks": ["Payroll processing", "Leave tracking", "Onboarding docs", "Attendance reports"],
        "skill": "People strategy, culture design, organisational development",
    },
    "payroll": {
        "pct": 80,
        "tasks": ["Payroll runs", "Statutory compliance filing", "Salary slip generation", "Tax computation"],
        "skill": "HR analytics, strategic HR business partnering, policy architecture",
    },
    "recruiter": {
        "pct": 55,
        "tasks": ["Job description generation", "Resume screening", "Interview scheduling", "Offer letter drafting"],
        "skill": "Talent strategy, executive hiring, employer branding",
    },
    "talent acquisition": {
        "pct": 55,
        "tasks": ["Resume screening automation", "Interview calendar coordination", "Offer generation"],
        "skill": "Strategic talent acquisition, employer brand, senior-level hiring",
    },

    # ── Finance / Accounts ───────────────────────────────────────────────
    "finance": {
        "pct": 70,
        "tasks": ["Invoice processing", "Expense report approval", "Bank reconciliation", "Budget variance reports"],
        "skill": "Financial strategy, FP&A, capital allocation, investor reporting",
    },
    "accountant": {
        "pct": 68,
        "tasks": ["Bookkeeping", "Bank reconciliation", "Invoice matching", "GST filing prep"],
        "skill": "Financial analysis, strategic accounting, audit readiness",
    },
    "bookkeeper": {
        "pct": 75,
        "tasks": ["Transaction categorisation", "Bank reconciliation", "Expense logging"],
        "skill": "Financial strategy, controllership",
    },
    "accounts": {
        "pct": 70,
        "tasks": ["Invoice processing", "Reconciliation", "Expense tracking"],
        "skill": "Financial analysis, strategic finance",
    },

    # ── Marketing ────────────────────────────────────────────────────────
    "marketing director": {
        "pct": 20,
        "tasks": ["Campaign performance reports", "Meeting scheduling"],
        "skill": "Brand strategy, go-to-market architecture, market expansion",
    },
    "marketing manager": {
        "pct": 40,
        "tasks": ["Campaign scheduling", "Performance reporting", "Email sequence setup"],
        "skill": "Growth strategy, data-driven marketing, brand management",
    },
    "marketing": {
        "pct": 50,
        "tasks": ["Campaign scheduling", "Social posting", "Email sequences", "Report generation", "Ad bid management"],
        "skill": "Growth strategy, data-driven marketing, brand storytelling",
    },
    "content": {
        "pct": 40,
        "tasks": ["Content distribution scheduling", "SEO tagging", "AI-assisted first drafts", "Repurposing"],
        "skill": "Content strategy, editorial direction, AI-assisted creation",
    },
    "social media": {
        "pct": 65,
        "tasks": ["Post scheduling", "Engagement tracking", "Performance reports", "Hashtag research"],
        "skill": "Social strategy, community management, brand voice development",
    },
    "growth": {
        "pct": 45,
        "tasks": ["A/B test setup", "Funnel tracking dashboards", "Email drip campaign management"],
        "skill": "Growth strategy, experimentation design, multi-channel thinking",
    },

    # ── Operations / Admin ───────────────────────────────────────────────
    "operations manager": {
        "pct": 35,
        "tasks": ["Process reporting", "SLA monitoring dashboards", "Vendor invoice tracking"],
        "skill": "Operations strategy, process design, cross-functional leadership",
    },
    "operations": {
        "pct": 60,
        "tasks": ["Reporting", "Scheduling coordination", "Data copying across systems", "Status tracking"],
        "skill": "Operations strategy, process improvement, automation design",
    },
    "admin": {
        "pct": 72,
        "tasks": ["Calendar management", "Document filing", "Data entry", "Re-keying information"],
        "skill": "Project management, executive support strategy, process design",
    },
    "executive assistant": {
        "pct": 60,
        "tasks": ["Meeting scheduling", "Travel booking", "Expense reporting", "Email triage"],
        "skill": "Chief of Staff functions, strategic project coordination",
    },
    "office manager": {
        "pct": 55,
        "tasks": ["Expense tracking", "Vendor communication", "Supplies ordering", "Meeting room booking"],
        "skill": "Facilities strategy, employee experience design",
    },

    # ── Product ──────────────────────────────────────────────────────────
    "product manager": {
        "pct": 35,
        "tasks": ["Roadmap reporting", "Analytics tracking", "Meeting notes transcription", "Feature request triage"],
        "skill": "Product strategy, AI-powered product discovery, data-driven prioritisation",
    },
    "product owner": {
        "pct": 35,
        "tasks": ["Backlog grooming automation", "Sprint reporting", "Acceptance criteria drafting"],
        "skill": "Product strategy, stakeholder management, outcome-driven delivery",
    },

    # ── Project Management ───────────────────────────────────────────────
    "project manager": {
        "pct": 45,
        "tasks": ["Status reporting", "Task assignment", "Meeting scheduling", "Time tracking", "Risk register updates"],
        "skill": "Strategic leadership, stakeholder influence, product management",
    },
    "scrum master": {
        "pct": 40,
        "tasks": ["Sprint ceremony scheduling", "Velocity reporting", "Impediment logging"],
        "skill": "Agile coaching, organisational transformation",
    },
}

# Fallback for roles not found in the database
_DEFAULT_ROLE = {
    "pct": 40,
    "tasks": ["Routine reporting", "Data entry", "Meeting scheduling"],
    "skill": "Digital literacy, AI tool adoption, process improvement",
}

# Vulnerability thresholds
_HIGH_THRESHOLD   = 60.0
_MEDIUM_THRESHOLD = 30.0


# ---------------------------------------------------------------------------
# Role classification
# ---------------------------------------------------------------------------

def _lookup_role(title: str) -> dict:
    """Find the best match in _ROLE_DB for a given job title.

    Strategy: find all keys that are substrings of the lowercased title;
    return the entry whose key is longest (most specific match).
    Fallback to _DEFAULT_ROLE if nothing matches.
    """
    t = title.lower()
    matched: list[tuple[int, dict]] = []  # (key_len, entry)
    for key, entry in _ROLE_DB.items():
        if key in t:
            matched.append((len(key), entry))
    if not matched:
        return _DEFAULT_ROLE
    matched.sort(key=lambda x: -x[0])
    return matched[0][1]


def _vulnerability(pct: float) -> str:
    if pct >= _HIGH_THRESHOLD:
        return "High"
    if pct >= _MEDIUM_THRESHOLD:
        return "Medium"
    return "Low"


def _classify_role(row: dict, _tools: list[str]) -> RoleAnalysis:
    """Return a RoleAnalysis for one org-chart row."""
    title   = str(row.get("_title", "Unknown"))
    emp_id  = str(row.get("_id", "?"))
    name    = str(row.get("_name", "?"))
    dept    = str(row.get("_dept", "?"))
    salary  = float(row.get("_salary", 0.0))
    hours   = float(row.get("_hours", 40.0))

    match = _lookup_role(title)
    pct   = float(match["pct"])
    vuln  = _vulnerability(pct)
    hours_saved = round(hours * pct / 100, 1)

    return RoleAnalysis(
        employee_id=emp_id,
        name=name,
        job_title=title,
        department=dept,
        monthly_salary_inr=salary,
        hours_per_week=hours,
        automation_pct=pct,
        automatable_tasks=list(match["tasks"]),
        vulnerability_level=vuln,
        upskilling_rec=match["skill"],
        hours_saved_per_week=hours_saved,
    )


# ---------------------------------------------------------------------------
# RPE Metrics (Metric 8)
# ---------------------------------------------------------------------------

def _compute_rpe(startup_profile: dict, headcount: int) -> tuple[RPEMetrics, list[str]]:
    """Calculate Revenue Per Employee Lift (Metric 8).

    Returns (RPEMetrics, warnings).
    """
    warnings: list[str] = []

    mrr_list = startup_profile.get("mrr_last_3_months", [])
    if not mrr_list:
        warnings.append(
            "mrr_last_3_months not found in startup profile — RPE metrics set to zero."
        )
        current_mrr = 0.0
    else:
        current_mrr = float(mrr_list[-1])

    growth_pct   = float(startup_profile.get("monthly_growth_goal_pct", 10))
    patience_mo  = int(startup_profile.get("patience_months", 12))
    growth_rate  = growth_pct / 100.0

    projected_mrr = current_mrr * math.pow(1 + growth_rate, patience_mo)

    if headcount <= 0:
        current_rpe  = 0.0
        projected_rpe = 0.0
        rpe_lift_pct  = 0.0
        rpe_lift_inr  = 0.0
    else:
        current_rpe   = round(current_mrr / headcount, 2)
        projected_rpe = round(projected_mrr / headcount, 2)
        rpe_lift_pct  = (
            round(((projected_rpe - current_rpe) / current_rpe) * 100, 2)
            if current_rpe > 0 else 0.0
        )
        rpe_lift_inr  = round(projected_rpe - current_rpe, 2)

    return RPEMetrics(
        current_mrr=round(current_mrr, 2),
        headcount=headcount,
        current_rpe_monthly=current_rpe,
        projected_mrr=round(projected_mrr, 2),
        projected_rpe_monthly=projected_rpe,
        rpe_lift_pct=rpe_lift_pct,
        rpe_lift_inr=rpe_lift_inr,
        growth_months_used=patience_mo,
        monthly_growth_rate_pct=growth_pct,
    ), warnings


# ---------------------------------------------------------------------------
# Mermaid chart — role vulnerability matrix by department
# ---------------------------------------------------------------------------

_VULN_ICON = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}


def _mermaid_chart(roles: list[RoleAnalysis]) -> str:
    """Produce a Mermaid flowchart grouping roles by department."""
    by_dept: dict[str, list[RoleAnalysis]] = defaultdict(list)
    for r in roles:
        by_dept[r.department].append(r)

    lines = ["flowchart TD"]
    node_ids: dict[str, str] = {}
    node_counter = 0

    for dept, members in sorted(by_dept.items()):
        safe_dept = dept.replace(" ", "_").replace("/", "_")
        lines.append(f'    subgraph {safe_dept}["{dept}"]')
        for m in members:
            node_counter += 1
            nid = f"N{node_counter}"
            node_ids[m.employee_id] = nid
            icon  = _VULN_ICON.get(m.vulnerability_level, "⚪")
            label = f'{icon} {m.job_title}\\n{m.automation_pct:.0f}% automatable'
            lines.append(f'        {nid}["{label}"]')
        lines.append("    end")

    # Add a legend at the bottom
    lines.append('    LEGEND["🔴 High ≥60%  |  🟡 Medium 30–59%  |  🟢 Low <30%"]')
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _build_recommendations(
    roles: list[RoleAnalysis],
    rpe: RPEMetrics,
    tools: list[str],
    total_hours_saved: float,
) -> list[str]:
    """Generate up to 6 tech-stack-aware, actionable recommendations."""
    recs: list[str] = []

    high_vuln = [r for r in roles if r.vulnerability_level == "High"]
    has_crm      = any("zoho crm" in t.lower() or "salesforce" in t.lower() or "crm" in t.lower() for t in tools)
    has_razorpay = any("razorpay" in t.lower() for t in tools)
    has_mailchimp = any("mailchimp" in t.lower() or "mail" in t.lower() for t in tools)
    has_slack    = any("slack" in t.lower() for t in tools)

    # 1. Highlight highest-automation role
    if high_vuln:
        top = max(high_vuln, key=lambda r: r.automation_pct)
        tool_hint = ""
        if "sdr" in top.job_title.lower() or "sales" in top.job_title.lower():
            tool_hint = " via Zoho CRM sequences" if has_crm else " via CRM automation"
        elif "support" in top.job_title.lower():
            tool_hint = " using an AI chat widget integrated with your support inbox"
        elif "hr" in top.job_title.lower() or "payroll" in top.job_title.lower():
            tool_hint = " using a payroll automation tool (e.g., Razorpay Payroll)" if has_razorpay else ""
        recs.append(
            f"🔴 **{top.job_title}** has the highest automation potential ({top.automation_pct:.0f}%). "
            f"Priority tasks to automate: {', '.join(top.automatable_tasks[:3])}{tool_hint}. "
            f"This frees ~{top.hours_saved_per_week:.0f}h/week for strategic work."
        )

    # 2. SDR-specific: email sequences
    sdr_roles = [r for r in roles if "sdr" in r.job_title.lower() or "sales dev" in r.job_title.lower()]
    if sdr_roles:
        tool_str = ("Zoho CRM + Mailchimp" if (has_crm and has_mailchimp)
                    else ("Mailchimp" if has_mailchimp else "a sales automation tool"))
        combined_hours = sum(r.hours_saved_per_week for r in sdr_roles)
        recs.append(
            f"📧 **{len(sdr_roles)} SDR(s)** spend ~{combined_hours:.0f}h/week on repeatable outreach. "
            f"Automate cold sequences, lead scoring, and CRM updates using {tool_str} — "
            "they should focus on high-value discovery calls only."
        )

    # 3. RPE lift messaging
    if rpe.rpe_lift_pct > 0 and rpe.current_mrr > 0:
        recs.append(
            f"📈 **RPE Lift (Metric 8):** Your team's revenue per employee could grow from "
            f"₹{rpe.current_rpe_monthly:,.0f}/mo to ₹{rpe.projected_rpe_monthly:,.0f}/mo "
            f"(+{rpe.rpe_lift_pct:.0f}%) in {rpe.growth_months_used} months at {rpe.monthly_growth_rate_pct:.0f}% "
            "monthly growth — without adding headcount. Automation unlocks this by removing bottlenecks."
        )

    # 4. Hours-saved point
    if total_hours_saved > 0:
        fte_equiv = round(total_hours_saved / 40, 1)
        recs.append(
            f"⏱️ Automating repetitive tasks across all roles would recover ~{total_hours_saved:.0f}h/week "
            f"(≈ {fte_equiv} full-time equivalent). Redirect this capacity to growth-generating work."
        )

    # 5. HR/Payroll automation
    hr_roles = [r for r in roles if any(kw in r.job_title.lower() for kw in ["hr", "payroll", "human resources"])]
    if hr_roles:
        tool_str = "Razorpay Payroll" if has_razorpay else "a payroll automation platform"
        recs.append(
            f"🏢 **HR & Payroll** ({hr_roles[0].automation_pct:.0f}% automatable): "
            f"Use {tool_str} to automate salary runs, leave tracking, and compliance filings. "
            "This eliminates manual errors and frees your HR team for culture and talent strategy."
        )

    # 6. Upskilling nudge for high-vulnerability employees
    if high_vuln:
        skills_needed = list({r.upskilling_rec.split(",")[0].strip() for r in high_vuln})[:3]
        recs.append(
            f"🎓 **Upskilling priority** for {len(high_vuln)} high-vulnerability role(s): "
            f"{', '.join(skills_needed)}. "
            "Position automation as a 'superpower' that handles admin so staff can focus on uniquely human tasks."
        )

    return recs[:6]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_automation_report(
    session_id: str,
    entry: SessionEntry,
) -> AutomationReport:
    """Produce a role automation audit from org_chart_df.

    Args:
        session_id: The session identifier.
        entry: SessionEntry with org_chart_df and startup_profile populated.

    Returns:
        AutomationReport with per-role analysis, Metric 3, Metric 8.

    Raises:
        ValueError: if org_chart_df is missing or has no rows.
    """
    df = entry.org_chart_df
    if df is None or df.empty:
        raise ValueError(
            "No org chart data found in session. "
            "Upload org_chart.csv in Module 1 before running the Role Auditor."
        )

    # ── Column detection ──────────────────────────────────────────────────
    id_col     = _find_col(df, _ID_VARIANTS)
    name_col   = _find_col(df, _NAME_VARIANTS)
    title_col  = _find_col(df, _TITLE_VARIANTS)
    dept_col   = _find_col(df, _DEPT_VARIANTS)
    salary_col = _find_col(df, _SALARY_VARIANTS)
    hours_col  = _find_col(df, _HOURS_VARIANTS)

    if title_col is None:
        raise ValueError(
            "Could not detect a job title column in org_chart.csv. "
            "Expected column names: Job_Title, Title, Role, Position, Designation."
        )

    warnings: list[str] = []
    if salary_col is None:
        warnings.append("Monthly salary column not found — salary set to 0 for all roles.")
    if hours_col is None:
        warnings.append("Hours-per-week column not found — defaulting to 40h/week for all roles.")

    # ── Row-level classification ──────────────────────────────────────────
    tools = entry.startup_profile.get("current_tech_stack", []) if entry.startup_profile else []

    roles: list[RoleAnalysis] = []
    for _, row in df.iterrows():
        row_dict = {
            "_id":     row[id_col]     if id_col     else "",
            "_name":   row[name_col]   if name_col   else "",
            "_title":  row[title_col],
            "_dept":   row[dept_col]   if dept_col   else "Unknown",
            "_salary": float(row[salary_col]) if salary_col and not _is_nan(row[salary_col]) else 0.0,
            "_hours":  float(row[hours_col])  if hours_col  and not _is_nan(row[hours_col])  else 40.0,
        }
        roles.append(_classify_role(row_dict, tools))

    if not roles:
        raise ValueError("org_chart.csv has no data rows after parsing.")

    # ── Aggregate Metric 3 ────────────────────────────────────────────────
    total_employees  = len(roles)
    avg_auto_pct     = round(sum(r.automation_pct for r in roles) / total_employees, 2)
    high_count       = sum(1 for r in roles if r.vulnerability_level == "High")
    medium_count     = sum(1 for r in roles if r.vulnerability_level == "Medium")
    low_count        = sum(1 for r in roles if r.vulnerability_level == "Low")
    top_role         = max(roles, key=lambda r: r.automation_pct)
    total_hrs_saved  = round(sum(r.hours_saved_per_week for r in roles), 2)
    automation_cov   = round(avg_auto_pct / 100.0, 4)

    # ── Metric 8: RPE ─────────────────────────────────────────────────────
    profile = entry.startup_profile or {}
    rpe_metrics, rpe_warnings = _compute_rpe(profile, total_employees)
    warnings.extend(rpe_warnings)

    # ── Mermaid chart ─────────────────────────────────────────────────────
    mermaid = _mermaid_chart(roles)

    # ── Recommendations ───────────────────────────────────────────────────
    recs = _build_recommendations(roles, rpe_metrics, tools, total_hrs_saved)

    return AutomationReport(
        session_id=session_id,
        total_employees=total_employees,
        roles=roles,
        avg_automation_pct=avg_auto_pct,
        high_vulnerability_count=high_count,
        medium_vulnerability_count=medium_count,
        low_vulnerability_count=low_count,
        top_automatable_role=top_role.job_title,
        top_automatable_pct=top_role.automation_pct,
        total_hours_saved_per_week=total_hrs_saved,
        rpe_metrics=rpe_metrics,
        automation_coverage=automation_cov,
        recommendations=recs,
        mermaid_chart=mermaid,
        warnings=warnings,
    )


def _is_nan(v) -> bool:
    try:
        import math
        return math.isnan(float(v))
    except (TypeError, ValueError):
        return False
