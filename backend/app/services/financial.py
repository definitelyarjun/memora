"""Module 5 — Financial Impact & ROI Simulator (rule-based, no LLM).

CFO-level mathematical proof of AI adoption ROI.

Pipeline
--------
1. Pull hours_saved × loaded salary from Module 4 (Role Auditor) roles.
2. Parse recurring expenses from expenses.csv (Module 1 upload).
3. Compute total cost base = payroll + recurring expenses.
4. Estimate new AI tool costs for recommended tools not in the current stack.
5. Metric 5  — Net Monthly Savings = gross_savings − new_tool_costs.
6. Metric 12 — Operating Margin Lift = new_margin − current_margin.
7. Metric 7  — Opportunity Cost of Delay = savings_foregone + MRR_at_risk.
8. Build Before → After dashboard rows.

Requires:  Module 4 (automation_report) with roles.
Optional:  Module 3 (benchmark_report) enriches Metric 7.
           Module 6 (roi_report) provides payback figure.
           expenses.csv from Module 1 for accurate margin calculation.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.financial import (
    AIToolRecommendation,
    BeforeAfterRow,
    EmployeeSavingsLine,
    FinancialReport,
)


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

_BENEFITS_OVERHEAD = 0.25    # 25% employer overhead: PF, ESI, gratuity — Indian SME
_WEEKS_PER_MONTH   = 4.33    # average weeks per calendar month


# ── Tool recommendation database (role-category → tool + monthly INR cost) ──
# Cost = 0 means "this tool is already built-in or free tier covers it".
_ROLE_TOOL_MAP: list[tuple[str, str, str, int]] = [
    # (role_keyword_substring, tool_name, purpose, monthly_cost_inr)
    ("sdr",              "Zoho CRM sequences",          "Automate cold outreach & lead scoring",      0),   # Zoho CRM already in most SaaS stacks
    ("bdr",              "Zoho CRM sequences",          "Automate cold outreach & lead scoring",      0),
    ("inside sales",     "Zoho CRM sequences",          "Automate pipeline & follow-ups",             0),
    ("hr",               "Keka HRMS",                   "Automate payroll, leave & compliance",    4500),
    ("human resource",   "Keka HRMS",                   "Automate HR workflows & reporting",       4500),
    ("payroll",          "Razorpay Payroll",             "Automate salary runs & statutory filing", 3000),
    ("customer support", "Freshdesk",                   "AI ticket routing & FAQ chatbot",         3000),
    ("customer service", "Freshdesk",                   "AI ticket routing & FAQ chatbot",         3000),
    ("helpdesk",         "Freshdesk",                   "AI ticket routing & FAQ chatbot",         3000),
    ("data analyst",     "Metabase",                    "Automated dashboards & reports",          2000),
    ("data scientist",   "Metabase + Python notebooks", "AI/ML experimentation pipeline",          3000),
    ("data engineer",    "dbt Cloud",                   "Automated data pipelines",                3500),
    ("qa",               "Playwright + Allure",         "Automated regression & reporting",        2500),
    ("quality assurance","Playwright + Allure",         "Automated regression & reporting",        2500),
    ("devops",           "GitHub Actions",              "CI/CD pipeline automation",               1500),
    ("developer",        "GitHub Copilot",              "AI code assistance",                      1500),
    ("engineer",         "GitHub Copilot",              "AI code assistance",                      1500),
    ("marketing",        "HubSpot Marketing Hub",       "Campaign automation & analytics",         3500),
    ("content",          "Jasper AI",                   "AI content drafting & SEO",               2000),
    ("social media",     "Buffer + AI captions",        "Scheduled posting & caption generation",  1500),
    ("growth",           "Amplitude + Mixpanel",        "Product analytics & funnel automation",   2500),
    ("finance",          "Zoho Books",                  "Automated invoicing & GST filing",        2000),
    ("accountant",       "Tally Prime",                 "Accounting & compliance automation",      1500),
    ("bookkeeper",       "Tally Prime",                 "Bookkeeping automation",                  1500),
    ("operations",       "Zapier",                      "Cross-tool workflow automation",           2500),
    ("admin",            "Zapier",                      "Task automation across apps",             2500),
    ("product manager",  "Notion AI",                   "PRD drafting & meeting summaries",        1500),
    ("project manager",  "Notion AI",                   "Task tracking & status reports",          1500),
    ("scrum master",     "Linear",                      "Sprint automation & standup summaries",   2000),
    ("sales director",   "Clari",                       "Revenue intelligence & forecasting",      3000),
    ("sales manager",    "Clari",                       "Pipeline inspection automation",          3000),
]

# How to detect "already in stack": check if any word in tool_name matches tech_stack entry
_STACK_PARTIAL_MATCH_KEYWORDS: dict[str, list[str]] = {
    "zoho crm sequences":    ["zoho", "zoho crm"],
    "keka hrms":             ["keka"],
    "razorpay payroll":      ["razorpay"],
    "freshdesk":             ["freshdesk"],
    "metabase":              ["metabase"],
    "github copilot":        ["github", "copilot"],
    "github actions":        ["github"],
    "notion ai":             ["notion"],
    "zoho books":            ["zoho"],
    "tally prime":           ["tally"],
    "zapier":                ["zapier"],
    "hubspot marketing hub": ["hubspot"],
    "jasper ai":             ["jasper"],
    "buffer + ai captions":  ["buffer"],
    "dbt cloud":             ["dbt"],
    "clari":                 ["clari"],
}


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _in_stack(tool_name: str, tech_stack: list[str]) -> bool:
    """Return True if tool (or its core brand) already exists in the tech stack."""
    tool_lower = tool_name.lower()
    stack_lower = [t.lower() for t in tech_stack]

    # Direct substring match
    for s in stack_lower:
        if s in tool_lower or tool_lower in s:
            return True

    # Keyword lookup table
    keywords = _STACK_PARTIAL_MATCH_KEYWORDS.get(tool_lower, [])
    for kw in keywords:
        if any(kw in s for s in stack_lower):
            return True

    return False


def _lookup_tool(job_title: str) -> tuple[str, str, str, int] | None:
    """Find the best tool recommendation for a job title using substring matching."""
    title_lower = job_title.lower()
    # Sort by keyword length descending to prefer more specific matches
    for keyword, tool, purpose, cost in sorted(_ROLE_TOOL_MAP, key=lambda x: -len(x[0])):
        if keyword in title_lower:
            return keyword, tool, purpose, cost
    return None


def _compute_employee_savings(
    roles: list,
) -> list[EmployeeSavingsLine]:
    """Compute per-employee gross monthly savings for High/Medium vulnerability roles."""
    lines: list[EmployeeSavingsLine] = []
    for role in roles:
        if role.vulnerability_level not in ("High", "Medium"):
            continue
        loaded_monthly = role.monthly_salary_inr * (1 + _BENEFITS_OVERHEAD)
        effective_hours_per_month = role.hours_per_week * _WEEKS_PER_MONTH
        if effective_hours_per_month <= 0:
            continue
        loaded_hourly = loaded_monthly / effective_hours_per_month
        monthly_hours_saved = role.hours_saved_per_week * _WEEKS_PER_MONTH
        gross_monthly_savings = round(monthly_hours_saved * loaded_hourly, 0)

        lines.append(EmployeeSavingsLine(
            employee_id=role.employee_id,
            name=role.name,
            job_title=role.job_title,
            monthly_salary_inr=role.monthly_salary_inr,
            hours_per_week=role.hours_per_week,
            hours_saved_per_week=role.hours_saved_per_week,
            loaded_hourly_rate_inr=round(loaded_hourly, 0),
            monthly_hours_saved=round(monthly_hours_saved, 1),
            gross_monthly_savings_inr=gross_monthly_savings,
        ))
    return lines


def _compute_ai_tools(
    roles: list,
    tech_stack: list[str],
) -> list[AIToolRecommendation]:
    """Build a deduplicated list of AI tool recommendations for automatable roles."""
    seen_tools: set[str] = set()
    recs: list[AIToolRecommendation] = []

    for role in roles:
        if role.vulnerability_level not in ("High", "Medium"):
            continue
        match = _lookup_tool(role.job_title)
        if match is None:
            continue
        _, tool_name, purpose, cost_inr = match

        # Deduplicate by tool name
        tool_key = tool_name.lower()
        if tool_key in seen_tools:
            continue
        seen_tools.add(tool_key)

        already = _in_stack(tool_name, tech_stack)
        effective_cost = 0 if already else cost_inr

        recs.append(AIToolRecommendation(
            tool_name=tool_name,
            purpose=purpose,
            monthly_cost_inr=effective_cost,
            replaces=f"Manual {role.job_title.lower()} tasks",
            already_in_stack=already,
            for_role_category=role.job_title,
        ))

    return recs


def _parse_recurring_expenses(expenses_df: pd.DataFrame | None) -> float:
    """Return average monthly recurring expenses in INR from expenses.csv.

    Groups by calendar month, averages across months with data.
    Returns 0.0 if expenses_df is None or empty.
    """
    if expenses_df is None or expenses_df.empty:
        return 0.0

    df = expenses_df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Detect columns
    amount_col = next(
        (c for c in df.columns if "amount" in c or c in ("cost", "value")), None
    )
    recurring_col = next(
        (c for c in df.columns if "recurring" in c or "repeat" in c), None
    )
    date_col = next(
        (c for c in df.columns if "date" in c or c in ("month", "period")), None
    )

    if amount_col is None:
        return 0.0

    # Coerce amounts to numeric
    df[amount_col] = pd.to_numeric(df[amount_col], errors="coerce").fillna(0)

    # Filter to recurring only (if column present)
    if recurring_col is not None:
        df = df[df[recurring_col].astype(str).str.strip().str.title().isin(["Yes", "True", "1", "Y"])]

    if df.empty:
        return 0.0

    # Group by month → mean across months
    if date_col is not None:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col])
        df["__period__"] = df[date_col].dt.to_period("M")
        monthly = df.groupby("__period__")[amount_col].sum()
        return float(monthly.mean())

    # No date column → just sum all rows
    return float(df[amount_col].sum())


def _build_before_after(
    mrr: float,
    current_costs: float,
    current_margin: float,
    projected_margin: float,
    gross_savings: float,
    new_ai_costs: float,
    net_savings: float,
    opp_cost: float,
) -> list[BeforeAfterRow]:
    """Build the Before vs After dashboard rows."""

    def _inr(v: float) -> str:
        if v >= 100_000:
            return f"₹{v/100_000:.1f}L"
        return f"₹{v:,.0f}"

    def _delta(v: float, positive_good: bool = True) -> str:
        sign = "+" if v >= 0 else ""
        icon = ""
        if positive_good:
            icon = "▲" if v > 0 else ("▼" if v < 0 else "—")
        else:
            icon = "▼" if v > 0 else ("▲" if v < 0 else "—")
        return f"{icon} {sign}{_inr(abs(v))}" if abs(v) >= 100 else "—"

    rows: list[BeforeAfterRow] = [
        BeforeAfterRow(
            metric="Monthly Revenue (MRR)",
            before_value=_inr(mrr),
            after_value=_inr(mrr),
            delta="No change",
            icon="💰",
        ),
        BeforeAfterRow(
            metric="Total Monthly Costs",
            before_value=_inr(current_costs),
            after_value=_inr(current_costs + new_ai_costs - gross_savings),
            delta=f"▼ {_inr(net_savings)} saved",
            icon="📉",
        ),
        BeforeAfterRow(
            metric="Labor Waste (automatable)",
            before_value=_inr(gross_savings),
            after_value="₹0",
            delta=f"▼ {_inr(gross_savings)} eliminated",
            icon="🧑",
        ),
        BeforeAfterRow(
            metric="New AI Tool Investment",
            before_value="₹0",
            after_value=_inr(new_ai_costs),
            delta=f"▲ {_inr(new_ai_costs)}" if new_ai_costs > 0 else "—",
            icon="🤖",
        ),
        BeforeAfterRow(
            metric="Net Monthly Savings (Metric 5)",
            before_value="₹0",
            after_value=_inr(net_savings),
            delta=f"▲ {_inr(net_savings)}" if net_savings > 0 else f"▼ {_inr(abs(net_savings))}",
            icon="✅" if net_savings > 0 else "⚠️",
        ),
        BeforeAfterRow(
            metric="Operating Margin (Metric 12)",
            before_value=f"{current_margin:.1f}%",
            after_value=f"{projected_margin:.1f}%",
            delta=f"▲ +{projected_margin - current_margin:.1f}pp"
            if projected_margin > current_margin
            else f"▼ {projected_margin - current_margin:.1f}pp",
            icon="📈" if projected_margin > current_margin else "📉",
        ),
        BeforeAfterRow(
            metric="Opportunity Cost / Month (Metric 7)",
            before_value=_inr(opp_cost),
            after_value="₹0",
            delta=f"▼ {_inr(opp_cost)} recovered",
            icon="⏰",
        ),
    ]
    return rows


def _build_headline(
    net_savings: float,
    margin_lift: float,
    opp_cost: float,
    mrr: float,
) -> str:
    if net_savings <= 0:
        return "AI tool costs currently exceed savings — re-evaluate tool selection."
    inr_l = net_savings / 100_000
    return (
        f"Implementing AI frees ₹{inr_l:.1f}L/month (Metric 5), "
        f"lifts operating margin by +{margin_lift:.1f}pp (Metric 12), "
        f"and costs ₹{opp_cost/100_000:.1f}L/month of delay (Metric 7)."
    )


def _build_exec_summary(
    mrr: float,
    net_savings: float,
    margin_lift: float,
    opp_cost_per_month: float,
    gross_savings: float,
    new_ai_costs: float,
    n_high: int,
    n_medium: int,
    months_be: float | None,
    rpe_lift_pct: float | None,
) -> str:
    lines: list[str] = []

    inr_l = lambda v: f"₹{v/100_000:.1f}L"

    lines.append(
        f"**{n_high} high-vulnerability role(s)** and **{n_medium} medium-vulnerability role(s)** "
        f"represent {inr_l(gross_savings)}/month in recoverable labor cost — "
        f"tasks currently performed manually that AI-native tools can handle in minutes."
    )

    if new_ai_costs > 0:
        lines.append(
            f"The required AI tools cost **{inr_l(new_ai_costs)}/month** — "
            f"a net saving of **{inr_l(net_savings)}/month** (Metric 5: ₹{net_savings:,.0f})."
        )
    else:
        lines.append(
            f"All recommended tools are already in the tech stack — "
            f"net savings of **{inr_l(net_savings)}/month** (Metric 5) with **zero new costs**."
        )

    lines.append(
        f"Operating margin improves by **+{margin_lift:.1f} percentage points** (Metric 12). "
        f"Every month of inaction costs **{inr_l(opp_cost_per_month)}/month** (Metric 7)."
    )

    if rpe_lift_pct and rpe_lift_pct > 0:
        lines.append(
            f"Paired with the +{rpe_lift_pct:.0f}% RPE lift projected in Module 4, "
            f"this is a full-stack efficiency transformation — more revenue, fewer costs, same headcount."
        )

    if months_be is not None:
        lines.append(
            f"Break-even in **{months_be:.1f} months** — "
            f"the full annual saving of {inr_l(net_savings * 12)} recurs from year 2 onwards."
        )

    return "\n\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def compute_financial_report(
    session_id: str,
    entry: SessionEntry,
) -> FinancialReport:
    """Compute the Financial Impact & ROI Simulator report.

    Requires:
        entry.automation_report — from Module 4 (Role Auditor)
    Optional:
        entry.benchmark_report  — from Module 3 (Bottleneck Analyzer)
        entry.expenses_df       — from Module 1 (expenses.csv upload)
        entry.roi_report        — from Module 6 (payback figure)

    Raises:
        ValueError: if automation_report has not been run.
    """
    ar = entry.automation_report
    if ar is None:
        raise ValueError(
            "Module 4 (Role & Automation Auditor) must be run before "
            "the Financial Impact Simulator."
        )

    br  = entry.benchmark_report   # may be None
    roi = entry.roi_report         # may be None
    profile: dict = {**entry.startup_profile, **entry.company_metadata}
    tech_stack: list[str] = profile.get("current_tech_stack", profile.get("tools_used", []))

    warnings: list[str] = []

    # ── MRR ────────────────────────────────────────────────────────────────
    mrr_list = profile.get("mrr_last_3_months", [])
    current_mrr = float(mrr_list[-1]) if mrr_list else 0.0
    if current_mrr <= 0:
        warnings.append(
            "No MRR data found in startup profile — margin figures will be inaccurate. "
            "Re-run Module 1 with a complete onboarding payload."
        )
        current_mrr = 1.0  # avoid division by zero

    # ── Total payroll from Module 4 roles ──────────────────────────────────
    total_payroll = sum(r.monthly_salary_inr for r in ar.roles)

    # ── Recurring operating expenses from expenses.csv ─────────────────────
    avg_monthly_opex = _parse_recurring_expenses(entry.expenses_df)
    if avg_monthly_opex <= 0:
        warnings.append(
            "expenses.csv not found or has no recurring entries — "
            "operating costs estimated from payroll alone."
        )

    total_monthly_costs = total_payroll + avg_monthly_opex

    # ── Employee savings (Metric 5 numerator) ─────────────────────────────
    employee_savings_lines = _compute_employee_savings(ar.roles)
    gross_monthly_savings = sum(s.gross_monthly_savings_inr for s in employee_savings_lines)

    # ── AI tool recommendations (Metric 5 denominator) ────────────────────
    ai_tools = _compute_ai_tools(ar.roles, tech_stack)
    new_ai_costs = sum(t.monthly_cost_inr for t in ai_tools)

    # ── Metric 5 — Net Monthly Savings ─────────────────────────────────────
    net_monthly_savings = gross_monthly_savings - new_ai_costs
    net_annual_savings = net_monthly_savings * 12

    # ── Metric 12 — Operating Margin Lift ──────────────────────────────────
    current_op_margin = (
        (current_mrr - total_monthly_costs) / current_mrr * 100
        if current_mrr > 0 else 0.0
    )
    projected_costs = total_monthly_costs + new_ai_costs - gross_monthly_savings
    projected_op_margin = (
        (current_mrr - projected_costs) / current_mrr * 100
        if current_mrr > 0 else 0.0
    )
    margin_lift = projected_op_margin - current_op_margin

    # ── Metric 7 — Opportunity Cost of Delay ──────────────────────────────
    # Component A: savings foregone each month of inaction
    savings_foregone = max(net_monthly_savings, 0.0)

    # Component B: MRR at risk from TAT bottlenecks (Module 3 data)
    mrr_at_risk = 0.0
    if br is not None and br.bottleneck_pct > 0 and current_mrr > 1:
        # TAT inefficiency fraction: excess time above threshold / total avg TAT
        if br.avg_tat_hours > 0:
            tat_inefficiency = max(
                0.0,
                (br.avg_tat_hours - br.bottleneck_threshold_hours) / br.avg_tat_hours,
            )
            # Conservative: 10% of inefficiency translates to MRR revenue delay/loss
            mrr_at_risk = round(current_mrr * tat_inefficiency * 0.10, 0)

    opportunity_cost_per_month = round(savings_foregone + mrr_at_risk, 0)
    opportunity_cost_per_year = opportunity_cost_per_month * 12

    if opportunity_cost_per_month > current_mrr * 0.30:
        warnings.append(
            "Opportunity cost exceeds 30% of MRR — verify Module 3 and Module 4 inputs."
        )

    # ── Break-even ──────────────────────────────────────────────────────────
    months_to_break_even: float | None = None
    if roi is not None and roi.summary.overall_payback_months is not None:
        months_to_break_even = roi.summary.overall_payback_months
    elif net_monthly_savings > 0 and new_ai_costs > 0:
        # Rough estimate: treat 3 months of tool cost as proxy setup cost
        setup_proxy = new_ai_costs * 3
        months_to_break_even = round(setup_proxy / net_monthly_savings, 1)

    # ── Before / After dashboard ──────────────────────────────────────────
    before_after = _build_before_after(
        mrr=current_mrr,
        current_costs=total_monthly_costs,
        current_margin=current_op_margin,
        projected_margin=projected_op_margin,
        gross_savings=gross_monthly_savings,
        new_ai_costs=new_ai_costs,
        net_savings=net_monthly_savings,
        opp_cost=opportunity_cost_per_month,
    )

    # ── Narrative ─────────────────────────────────────────────────────────
    headline = _build_headline(
        net_monthly_savings, margin_lift, opportunity_cost_per_month, current_mrr
    )
    rpe_lift = ar.rpe_metrics.rpe_lift_pct if ar.rpe_metrics else None
    exec_summary = _build_exec_summary(
        mrr=current_mrr,
        net_savings=net_monthly_savings,
        margin_lift=margin_lift,
        opp_cost_per_month=opportunity_cost_per_month,
        gross_savings=gross_monthly_savings,
        new_ai_costs=new_ai_costs,
        n_high=ar.high_vulnerability_count,
        n_medium=ar.medium_vulnerability_count,
        months_be=months_to_break_even,
        rpe_lift_pct=rpe_lift,
    )

    return FinancialReport(
        session_id=session_id,
        current_mrr=current_mrr,
        total_payroll_monthly_inr=total_payroll,
        total_recurring_expenses_inr=avg_monthly_opex,
        total_monthly_costs_inr=total_monthly_costs,
        headcount=ar.total_employees,
        gross_monthly_savings_inr=round(gross_monthly_savings, 0),
        new_ai_tools_monthly_cost_inr=round(new_ai_costs, 0),
        net_monthly_savings_inr=round(net_monthly_savings, 0),
        net_annual_savings_inr=round(net_annual_savings, 0),
        current_operating_margin_pct=round(current_op_margin, 2),
        projected_operating_margin_pct=round(projected_op_margin, 2),
        gross_margin_lift_pct=round(margin_lift, 2),
        opportunity_cost_per_month_inr=opportunity_cost_per_month,
        opportunity_cost_per_year_inr=opportunity_cost_per_year,
        mrr_at_risk_monthly_inr=mrr_at_risk,
        months_to_break_even=months_to_break_even,
        employee_savings=employee_savings_lines,
        ai_tool_recommendations=ai_tools,
        before_after=before_after,
        headline=headline,
        executive_summary=exec_summary,
        warnings=warnings,
    )
