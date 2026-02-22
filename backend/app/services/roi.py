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
from collections import Counter
from typing import Any

from app.core.session_store import SessionEntry
from app.schemas.automation import AutomationCandidate, AutomationReport
from app.schemas.consolidation import ConsolidationReport, MigrationStep
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
# Automation ROI lines — with diminishing returns + keyword scaling
# ═══════════════════════════════════════════════════════════════════════════

def _keyword_time_scale(description: str) -> float:
    """Return a multiplier (0.5–1.3) based on step description complexity."""
    if _HEAVY_KEYWORDS.search(description):
        return 1.3
    if _LIGHT_KEYWORDS.search(description):
        return 0.5
    return 0.8  # default: slightly below base (conservative)


def _get_diminishing_multiplier(index: int) -> float:
    """Return the diminishing-returns multiplier for the Nth step (0-based)."""
    if index < len(_DIMINISHING_MULTIPLIERS):
        return _DIMINISHING_MULTIPLIERS[index]
    return _DIMINISHING_FLOOR


def _get_api_cost(description: str) -> float:
    """Return implementation cost for API Integration based on complexity."""
    if _COMPLEX_API_KEYWORDS.search(description):
        return _COMPLEX_API_COST
    return _IMPLEMENTATION_COST_BY_TYPE["API Integration"]


def _compute_automation_lines(
    candidates: list[AutomationCandidate],
    hourly_wage: float,
) -> list[AutomationROILine]:
    """Generate ROI line items with diminishing returns per automation type.

    Steps are grouped by automation type.  Within each group, the first step
    gets the full base hours; subsequent steps get less because activities
    overlap (same person, related work).
    """
    # Separate candidates by whether they're actionable
    actionable = [c for c in candidates if c.is_candidate]
    if not actionable:
        return []

    # Count how many of each type we've seen (for diminishing returns)
    type_counter: Counter = Counter()

    # Sort by priority then step number for deterministic ordering
    priority_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    actionable_sorted = sorted(
        actionable,
        key=lambda c: (priority_order.get(c.priority, 4), c.step_number),
    )

    lines: list[AutomationROILine] = []

    for c in actionable_sorted:
        atype = c.automation_type
        base_hours = _BASE_HOURS_BY_TYPE.get(atype, 0.5)

        # Apply keyword-based scaling
        keyword_scale = _keyword_time_scale(c.description)

        # Apply diminishing returns
        nth = type_counter[atype]
        type_counter[atype] += 1
        dim_mult = _get_diminishing_multiplier(nth)

        current_hours = round(base_hours * keyword_scale * dim_mult, 2)
        # Floor: at least 0.10 hrs/wk (6 min) for any real candidate
        current_hours = max(0.10, current_hours)

        efficiency = _EFFICIENCY_BY_TYPE.get(atype, 0.5)
        saved_hours = round(current_hours * efficiency, 2)
        annual_hours = round(saved_hours * _WORKING_WEEKS_PER_YEAR, 1)
        annual_cost = round(annual_hours * hourly_wage, 0)

        # Implementation cost (API integration is context-aware)
        if atype == "API Integration":
            impl_cost = _get_api_cost(c.description)
        else:
            impl_cost = _IMPLEMENTATION_COST_BY_TYPE.get(atype, 10_000)

        # If this is the Nth tool of the same type, only the 1st needs
        # full setup; subsequent ones share the platform (50% discount).
        if nth >= 1:
            impl_cost = round(impl_cost * 0.50)

        payback = None
        if impl_cost > 0 and annual_cost > 0:
            payback = round(impl_cost / (annual_cost / 12), 1)

        lines.append(AutomationROILine(
            step_number=c.step_number,
            description=c.description[:100],
            automation_type=atype,
            current_hours_per_week=current_hours,
            hours_saved_per_week=saved_hours,
            annual_hours_saved=annual_hours,
            annual_cost_saved=annual_cost,
            implementation_cost=impl_cost,
            payback_months=payback,
            effort=c.estimated_effort,
            priority=c.priority,
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
    """Compute ROI projections from Module 4 (automation) and Module 5
    (consolidation) results.

    Requires at least one of automation_report or consolidation_report
    to be populated in the session.

    Raises:
        ValueError: if neither Module 4 nor Module 5 has been run.
    """
    auto_report: AutomationReport | None = entry.automation_report
    consol_report: ConsolidationReport | None = entry.consolidation_report

    if auto_report is None and consol_report is None:
        raise ValueError(
            "Cannot compute ROI without Module 4 (Automation) or Module 5 "
            "(Consolidation) results. Run at least one of those first."
        )

    num_employees = entry.company_metadata.get("num_employees", 5)
    hourly_wage = _DEFAULT_HOURLY_WAGE_INR

    # Build assumptions list
    assumptions = _build_assumptions(num_employees, hourly_wage)

    # --- Automation ROI lines ---
    auto_lines: list[AutomationROILine] = []
    if auto_report is not None:
        auto_lines = _compute_automation_lines(
            auto_report.candidates, hourly_wage,
        )

    # --- Consolidation ROI lines ---
    consol_lines: list[ConsolidationROILine] = []
    if consol_report is not None:
        consol_lines = _compute_consolidation_lines(
            consol_report.migration_steps, hourly_wage,
        )

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
