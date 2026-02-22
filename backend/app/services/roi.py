"""Module 6 — ROI Estimator (rule-based, no LLM).

Computes realistic, conservative estimates of time saved, cost saved,
and annual savings from implementing the automations (Module 4) and
consolidation migrations (Module 5) recommended earlier.

Design philosophy
-----------------
* **Conservative by default** — every estimate uses the *low* end of
  industry benchmarks; businesses should be positively surprised, not
  disappointed.
* **Transparent assumptions** — every number traces to a stated assumption
  the SME owner can inspect and override mentally.
* **No vanity metrics** — implementation costs are *included*, payback
  period is shown, and negative first-year ROI is honestly reported.
* **Diminishing returns** — when multiple steps share the same automation
  type, each subsequent step gets reduced hours because activities overlap.
* **Keyword-aware estimation** — step descriptions are scanned for time
  indicators to scale the base hours (quick tasks vs lengthy tasks).
* **Smart consolidation** — vague "evaluate" recommendations get minimal
  cost; concrete migrations get real costs.  Shared targets pay the tool
  cost once.
"""

from __future__ import annotations

import re
from app.core.session_store import SessionEntry
from app.schemas.automation import RoleAnalysis, AutomationReport
from app.schemas.roi import (
    Assumption,
    AutomationROILine,
    ConsolidationROILine,
    ROIReport,
    ROISummary,
)


# ═══════════════════════════════════════════════════════════════════════════
# Default assumptions (conservative SME benchmarks)
# ═══════════════════════════════════════════════════════════════════════════

# Base hours per week for the FIRST step of each automation type.
# Subsequent steps of the same type get diminishing amounts.
_BASE_HOURS_BY_TYPE: dict[str, float] = {
    "RPA":              1.5,   # data entry / calculation — repetitive daily work
    "Digital Form":     1.0,   # paper recording — filling, filing, searching
    "API Integration":  0.75,  # manual handoffs between tools
    "AI/ML":            1.0,   # decision/reporting currently done manually
    "Decision Engine":  0.5,   # checking/reviewing/approving
    "Not Recommended":  0.0,   # physical — no automation savings
}

# Diminishing-returns multipliers for the Nth step of the same type.
# Rationale: the 1st RPA opportunity might save 1.5 hrs, the 2nd saves less
# because activities overlap (same person, same session of work).
_DIMINISHING_MULTIPLIERS = [1.0, 0.60, 0.40, 0.25, 0.20]
# Steps beyond the 5th all get 0.15
_DIMINISHING_FLOOR = 0.15

# Keywords that indicate a HEAVIER step (scale UP base hours × 1.3)
_HEAVY_KEYWORDS = re.compile(
    r"transfer|reconcil|migrate|consolidat|all\s+data|daily\s+total"
    r"|monthly|salary|payroll|audit|gst|tax|filing",
    re.IGNORECASE,
)
# Keywords that indicate a LIGHTER step (scale DOWN base hours × 0.5)
_LIGHT_KEYWORDS = re.compile(
    r"calculate\s+bill|write.*receipt|slip|note|sticky|check\s+stock"
    r"|walk|hand\s+over|verbal|call\b",
    re.IGNORECASE,
)

# Automation efficiency: what fraction of time is actually eliminated.
_EFFICIENCY_BY_TYPE: dict[str, float] = {
    "RPA":              0.80,
    "Digital Form":     0.60,
    "API Integration":  0.70,
    "AI/ML":            0.50,   # needs human-in-the-loop
    "Decision Engine":  0.65,
    "Not Recommended":  0.0,
}

# One-time implementation costs (INR, SME-scale).
# API Integration has two tiers determined by keywords.
_IMPLEMENTATION_COST_BY_TYPE: dict[str, float] = {
    "RPA":              15_000,   # ~$180  — simple bot / macro / Zapier
    "Digital Form":      5_000,   # ~$60   — Google Form / free form tool
    "API Integration":  15_000,   # ~$180  — default (simple notifications/handoff)
    "AI/ML":            60_000,   # ~$720  — model training + deployment
    "Decision Engine":  20_000,   # ~$240  — rule engine / low-code workflow
    "Not Recommended":  0,
}

# Complex API integrations (POS, ERP, payment gateway) cost more
_COMPLEX_API_KEYWORDS = re.compile(
    r"pos|payment|gateway|erp|accounting|integrat.*system|automat.*order",
    re.IGNORECASE,
)
_COMPLEX_API_COST = 35_000  # ~$420

# ── Consolidation constants ───────────────────────────────────────────────

# Overhead hours for concrete (non-vague) migrations
_CONSOL_OVERHEAD_CONCRETE: dict[str, float] = {
    "Low":    0.75,   # re-entry from paper/WhatsApp (realistic: ~45 min/week)
    "Medium": 0.35,   # reconciliation between spreadsheets
    "High":   0.20,   # enterprise tool friction
}

# Vague "Evaluate" recommendations: almost zero overhead because we can't
# quantify savings for a non-specific action.
_CONSOL_OVERHEAD_VAGUE = 0.10  # 6 min/week of extra friction

# One-time migration costs for CONCRETE recommendations
_CONSOL_IMPL_CONCRETE: dict[str, float] = {
    "Low":     3_000,  # ~$36  — adopt a free/cheap tool
    "Medium": 12_000,  # ~$144 — data migration + training
    "High":   40_000,  # ~$480 — major tool change
}

# Vague "Evaluate" gets minimal cost (it's just research time)
_CONSOL_IMPL_VAGUE = 1_000  # ~$12

# Efficiency: what fraction of overhead is eliminated
_CONSOL_EFFICIENCY: dict[str, float] = {
    "Low":    0.85,
    "Medium": 0.60,
    "High":   0.40,
}
_CONSOL_EFFICIENCY_VAGUE = 0.30  # vague → can't claim much savings

# Detect vague recommendations
_VAGUE_PATTERN = re.compile(r"evaluate|assess|consider|review\s+option", re.IGNORECASE)

# Default blended hourly wage for an SME employee (INR/hr).
_DEFAULT_HOURLY_WAGE_INR = 180

_WORKING_WEEKS_PER_YEAR = 50


def _build_assumptions(
    num_employees: int,
    hourly_wage: float,
) -> list[Assumption]:
    """Return the full list of stated assumptions."""
    return [
        Assumption(
            key="hourly_wage",
            label="Blended hourly wage",
            value=f"₹{hourly_wage:,.0f}/hr",
            source="Indian SME median for semi-skilled staff (~₹30k/month)",
        ),
        Assumption(
            key="working_weeks",
            label="Working weeks per year",
            value=str(_WORKING_WEEKS_PER_YEAR),
            source="52 weeks minus 2 weeks holidays",
        ),
        Assumption(
            key="num_employees",
            label="Number of employees",
            value=str(num_employees),
            source="Company metadata",
        ),
        Assumption(
            key="diminishing_returns",
            label="Diminishing returns",
            value="1st step 100%, 2nd 60%, 3rd 40%, 4th+ 25%",
            source="Activities of the same type overlap — each additional step adds less",
        ),
        Assumption(
            key="automation_efficiency",
            label="Automation time-saving efficiency",
            value="50–80% depending on type",
            source="Conservative industry benchmarks (RPA 80%, AI/ML 50%)",
        ),
        Assumption(
            key="implementation_costs",
            label="Implementation costs",
            value="₹1k–₹60k per item (SME scale)",
            source="Indian SME market rates for tool setup, not enterprise pricing",
        ),
    ]


# ═══════════════════════════════════════════════════════════════════════════
# Automation ROI lines — role-based (Module 4 Role Auditor output)
# ═══════════════════════════════════════════════════════════════════════════


# Implementation cost by vulnerability level (INR, one-time)
_IMPL_COST_BY_VULN: dict[str, int] = {
    "High":   20_000,   # standard tooling / RPA bot
    "Medium": 30_000,   # some customisation required
    "Low":    50_000,   # strategic/creative — complex to automate
}

# Automation type heuristics for roles
_AIML_KEYWORDS = re.compile(
    r"data analyst|data scientist|data engineer|bi analyst|analytics", re.I
)
_API_KEYWORDS = re.compile(
    r"developer|engineer|cto|devops|backend|frontend|software", re.I
)
_DIGITAL_FORM_KEYWORDS = re.compile(
    r"hr|human resource|payroll|admin|receptionist|support|helpdesk|sdr|bdr", re.I
)


def _role_automation_type(job_title: str) -> str:
    """Map a job title to the most appropriate automation type."""
    if _AIML_KEYWORDS.search(job_title):
        return "AI/ML"
    if _API_KEYWORDS.search(job_title):
        return "API Integration"
    if _DIGITAL_FORM_KEYWORDS.search(job_title):
        return "Digital Form"
    return "RPA"


def _compute_automation_lines(
    roles: list[RoleAnalysis],
    hourly_wage: float,
) -> list[AutomationROILine]:
    """Generate ROI line items from role-audit results.

    Each High/Medium vulnerability role becomes one ROI line item.
    Low-vulnerability roles (<30% automation potential) are excluded
    because the ROI is minimal and implementation is not justified.
    """
    # Only include High and Medium vulnerability roles
    actionable = [r for r in roles if r.vulnerability_level in ("High", "Medium")]
    if not actionable:
        return []

    # Sort: High vulnerability first, then by automation_pct desc
    vuln_order = {"High": 0, "Medium": 1, "Low": 2}
    actionable_sorted = sorted(
        actionable,
        key=lambda r: (vuln_order.get(r.vulnerability_level, 2), -r.automation_pct),
    )

    lines: list[AutomationROILine] = []

    for idx, role in enumerate(actionable_sorted, start=1):
        atype = _role_automation_type(role.job_title)
        saved_hours = round(role.hours_saved_per_week, 2)
        annual_hours = round(saved_hours * _WORKING_WEEKS_PER_YEAR, 1)
        # Cost saving: proportion of fully-loaded annual salary
        annual_cost = round(role.monthly_salary_inr * 12 * role.automation_pct / 100, 0)
        current_hours = round(role.hours_per_week, 2)

        impl_cost = _IMPL_COST_BY_VULN.get(role.vulnerability_level, 30_000)

        payback = None
        if impl_cost > 0 and annual_cost > 0:
            payback = round(impl_cost / (annual_cost / 12), 1)

        # Map vulnerability level → effort and priority
        effort_map = {"High": "Low", "Medium": "Medium", "Low": "High"}
        priority_map = {"High": "Critical", "Medium": "High", "Low": "Low"}

        tasks_preview = ", ".join(role.automatable_tasks[:2])
        description = f"[{role.job_title}] {tasks_preview}"[:100]

        lines.append(AutomationROILine(
            step_number=idx,
            description=description,
            automation_type=atype,
            current_hours_per_week=current_hours,
            hours_saved_per_week=saved_hours,
            annual_hours_saved=annual_hours,
            annual_cost_saved=annual_cost,
            implementation_cost=impl_cost,
            payback_months=payback,
            effort=effort_map.get(role.vulnerability_level, "Medium"),
            priority=priority_map.get(role.vulnerability_level, "High"),
        ))

    # Sort by annual savings descending (highest ROI first)
    lines.sort(key=lambda l: l.annual_cost_saved, reverse=True)
    return lines


# ═══════════════════════════════════════════════════════════════════════════
# Consolidation ROI lines — vague filtering + shared-target dedup
# ═══════════════════════════════════════════════════════════════════════════

def _is_vague_recommendation(to_tool: str) -> bool:
    """Return True if the recommendation is a non-specific 'evaluate' action."""
    return bool(_VAGUE_PATTERN.search(to_tool))


def _compute_consolidation_lines(
    migration_steps: list[MigrationStep],
    hourly_wage: float,
) -> list[ConsolidationROILine]:
    """Generate ROI line items for consolidation, filtering vague recommendations
    and applying shared-target cost deduplication."""
    lines: list[ConsolidationROILine] = []

    # Track which target tools we've already costed (shared-target dedup)
    target_seen: dict[str, bool] = {}

    for m in migration_steps:
        effort = m.effort
        is_vague = _is_vague_recommendation(m.to_tool)

        if is_vague:
            overhead = _CONSOL_OVERHEAD_VAGUE
            efficiency = _CONSOL_EFFICIENCY_VAGUE
            impl_cost = _CONSOL_IMPL_VAGUE
        else:
            overhead = _CONSOL_OVERHEAD_CONCRETE.get(effort, 0.35)
            efficiency = _CONSOL_EFFICIENCY.get(effort, 0.50)
            impl_cost = _CONSOL_IMPL_CONCRETE.get(effort, 8_000)

        # Shared-target dedup: if two silos migrate to the same tool,
        # the 2nd one doesn't pay full setup again (60% discount).
        target_key = m.to_tool.lower().strip()[:40]
        if target_key in target_seen:
            impl_cost = round(impl_cost * 0.40)
        else:
            target_seen[target_key] = True

        saved_hours = round(overhead * efficiency, 2)
        annual_hours = round(saved_hours * _WORKING_WEEKS_PER_YEAR, 1)
        annual_cost = round(annual_hours * hourly_wage, 0)

        payback = None
        if impl_cost > 0 and annual_cost > 0:
            payback = round(impl_cost / (annual_cost / 12), 1)

        lines.append(ConsolidationROILine(
            migration_priority=m.priority,
            action=m.action[:120],
            from_tool=m.from_tool,
            to_tool=m.to_tool[:80],
            current_overhead_hours_per_week=overhead,
            hours_saved_per_week=saved_hours,
            annual_hours_saved=annual_hours,
            annual_cost_saved=annual_cost,
            implementation_cost=impl_cost,
            payback_months=payback,
            effort=effort,
        ))

    return lines


# ═══════════════════════════════════════════════════════════════════════════
# Summary + narrative
# ═══════════════════════════════════════════════════════════════════════════

def _compute_summary(
    auto_lines: list[AutomationROILine],
    consol_lines: list[ConsolidationROILine],
) -> ROISummary:
    """Aggregate all line items into a single summary."""
    total_current = (
        sum(l.current_hours_per_week for l in auto_lines)
        + sum(l.current_overhead_hours_per_week for l in consol_lines)
    )
    total_saved_week = (
        sum(l.hours_saved_per_week for l in auto_lines)
        + sum(l.hours_saved_per_week for l in consol_lines)
    )
    total_annual_hours = (
        sum(l.annual_hours_saved for l in auto_lines)
        + sum(l.annual_hours_saved for l in consol_lines)
    )
    total_annual_cost = (
        sum(l.annual_cost_saved for l in auto_lines)
        + sum(l.annual_cost_saved for l in consol_lines)
    )
    total_impl = (
        sum(l.implementation_cost for l in auto_lines)
        + sum(l.implementation_cost for l in consol_lines)
    )

    net_y1 = total_annual_cost - total_impl
    net_3y = (total_annual_cost * 3) - total_impl

    overall_payback = None
    if total_impl > 0 and total_annual_cost > 0:
        overall_payback = round(total_impl / (total_annual_cost / 12), 1)

    roi_pct = 0.0
    if total_impl > 0:
        roi_pct = round((total_annual_cost / total_impl) * 100, 1)

    return ROISummary(
        total_current_hours_per_week=round(total_current, 1),
        total_hours_saved_per_week=round(total_saved_week, 1),
        total_annual_hours_saved=round(total_annual_hours, 1),
        total_annual_cost_saved=round(total_annual_cost, 0),
        total_implementation_cost=round(total_impl, 0),
        net_first_year_benefit=round(net_y1, 0),
        three_year_net_benefit=round(net_3y, 0),
        overall_payback_months=overall_payback,
        roi_percentage=roi_pct,
    )


def _build_executive_summary(
    summary: ROISummary,
    auto_line_count: int,
    consol_line_count: int,
    num_employees: int,
) -> str:
    """2-3 sentence executive summary."""
    parts: list[str] = []

    total_items = auto_line_count + consol_line_count
    if total_items == 0:
        return (
            "No automation or consolidation opportunities were identified. "
            "The current workflow is either already optimised or primarily "
            "involves physical work that cannot be automated."
        )

    parts.append(
        f"Across **{auto_line_count} automation** and **{consol_line_count} "
        f"consolidation** opportunities, an estimated **{summary.total_hours_saved_per_week:.1f} "
        f"hours/week** ({summary.total_annual_hours_saved:,.0f} hours/year) "
        f"can be recovered"
    )

    parts.append(
        f"This translates to projected annual savings of "
        f"**₹{summary.total_annual_cost_saved:,.0f}** against a one-time "
        f"investment of **₹{summary.total_implementation_cost:,.0f}**"
    )

    if summary.overall_payback_months is not None:
        if summary.overall_payback_months <= 6:
            parts.append(
                f"Payback period is **{summary.overall_payback_months:.1f} months** "
                f"— a strong, fast return"
            )
        elif summary.overall_payback_months <= 12:
            parts.append(
                f"Payback period is **{summary.overall_payback_months:.1f} months** "
                f"— a solid return within the first year"
            )
        elif summary.overall_payback_months <= 24:
            parts.append(
                f"Payback period is **{summary.overall_payback_months:.1f} months** "
                f"— reasonable medium-term return"
            )
        else:
            parts.append(
                f"Payback period is **{summary.overall_payback_months:.1f} months** "
                f"— plan for a longer-term return; prioritise quick wins first"
            )

    return ". ".join(parts) + "."


def _build_top_recommendations(
    auto_lines: list[AutomationROILine],
    consol_lines: list[ConsolidationROILine],
    summary: ROISummary,
) -> list[str]:
    """Build up to 6 prioritised, ROI-focused recommendations."""
    recs: list[str] = []

    # 1. Best automation quick win (low effort, best payback)
    quick_auto = [
        l for l in auto_lines
        if l.effort == "Low" and l.annual_cost_saved > 0 and l.payback_months
    ]
    quick_auto.sort(key=lambda l: l.payback_months or 999)
    if quick_auto:
        top = quick_auto[0]
        recs.append(
            f"🟢 **Quick win — Step {top.step_number}** ({top.automation_type}): "
            f"saves ₹{top.annual_cost_saved:,.0f}/year with low effort "
            f"(payback: {top.payback_months or 0:.0f} months). "
            f"Start here for fastest return."
        )

    # 2. Highest annual savings (automation)
    if auto_lines:
        best = auto_lines[0]  # already sorted by annual_cost_saved desc
        if not quick_auto or best.step_number != quick_auto[0].step_number:
            recs.append(
                f"💰 **Highest savings — Step {best.step_number}** "
                f"({best.automation_type}): saves "
                f"₹{best.annual_cost_saved:,.0f}/year. "
                f"{best.description[:60]}."
            )

    # 3. Best concrete consolidation win (exclude vague)
    concrete_consol = [l for l in consol_lines if not _is_vague_recommendation(l.to_tool)]
    concrete_sorted = sorted(concrete_consol, key=lambda l: l.annual_cost_saved, reverse=True)
    if concrete_sorted:
        top_c = concrete_sorted[0]
        recs.append(
            f"📦 **Top consolidation — replace {top_c.from_tool}**: "
            f"saves ₹{top_c.annual_cost_saved:,.0f}/year. "
            f"Migrate to {top_c.to_tool[:50]}."
        )
    elif consol_lines:
        top_c = sorted(consol_lines, key=lambda l: l.annual_cost_saved, reverse=True)[0]
        recs.append(
            f"📦 **Top consolidation — {top_c.from_tool}**: "
            f"eliminating overhead saves ₹{top_c.annual_cost_saved:,.0f}/year."
        )

    # 4. If payback is short, highlight it
    if summary.overall_payback_months is not None and summary.overall_payback_months <= 8:
        recs.append(
            f"⚡ **Fast payback** — the full investment of "
            f"₹{summary.total_implementation_cost:,.0f} pays for itself in "
            f"~{summary.overall_payback_months:.0f} months."
        )

    # 5. 3-year benefit
    if summary.three_year_net_benefit > 0:
        recs.append(
            f"📈 **3-year net benefit: ₹{summary.three_year_net_benefit:,.0f}** "
            f"({summary.roi_percentage:.0f}% annual ROI on the one-time investment)."
        )

    # 6. Warning if negative first year
    if summary.net_first_year_benefit < 0:
        recs.append(
            f"⚠️ **First-year net is negative** (₹{summary.net_first_year_benefit:,.0f}). "
            f"The investment pays off in year 2+. Consider phasing: implement "
            f"quick wins first to fund larger changes."
        )

    return recs[:6]


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def compute_roi_report(
    session_id: str,
    entry: SessionEntry,
) -> ROIReport:
    """Compute ROI projections from Module 4 (automation) results.

    Requires automation_report to be populated in the session.

    Raises:
        ValueError: if Module 4 has not been run.
    """
    auto_report: AutomationReport | None = entry.automation_report

    if auto_report is None:
        raise ValueError(
            "Cannot compute ROI without Module 4 (Automation) results. "
            "Run Module 4 (Role Auditor) first."
        )

    num_employees = entry.company_metadata.get("num_employees", 5)
    hourly_wage = _DEFAULT_HOURLY_WAGE_INR

    # Build assumptions list
    assumptions = _build_assumptions(num_employees, hourly_wage)

    # --- Automation ROI lines ---
    auto_lines: list[AutomationROILine] = []
    auto_lines = _compute_automation_lines(
        auto_report.roles, hourly_wage,
    )

    # --- Consolidation lines are no longer computed (Module 5 rearchitected) ---
    consol_lines: list[ConsolidationROILine] = []

    # --- Summary ---
    summary = _compute_summary(auto_lines, consol_lines)

    # --- Narrative ---
    exec_summary = _build_executive_summary(
        summary, len(auto_lines), len(consol_lines), num_employees,
    )
    top_recs = _build_top_recommendations(auto_lines, consol_lines, summary)

    return ROIReport(
        session_id=session_id,
        assumptions=assumptions,
        automation_lines=auto_lines,
        consolidation_lines=consol_lines,
        summary=summary,
        executive_summary=exec_summary,
        top_recommendations=top_recs,
    )
