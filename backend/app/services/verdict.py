"""Module 7 — Strategic Verdict Generator (rule-based, no LLM).

Aggregates outputs from all preceding modules into a single executive
diagnostic report.  No new analysis is performed — this is purely a
synthesis/consolidation layer.

Module mapping
--------------
  Module 2  — Data Quality & AI Readiness  → quality_report
  Module 3  — Industry Benchmarking        → benchmark_report
  Module 4  — Automation Opportunity       → automation_report
  Module 5  — Data Consolidation           → consolidation_report
  Module 6  — ROI Estimator                → roi_report

At least **one** module must have been run for the verdict to be generated.
The more modules that have been run, the richer and more accurate the
verdict becomes.
"""

from __future__ import annotations

from typing import Any

from app.core.session_store import SessionEntry
from app.schemas.verdict import (
    ActionItem,
    ModuleScorecard,
    OverallVerdict,
    RiskItem,
    StrategicVerdict,
)


# ═══════════════════════════════════════════════════════════════════════════
# Verdict thresholds
# ═══════════════════════════════════════════════════════════════════════════

def _verdict_label(score: float) -> OverallVerdict:
    if score >= 0.75:
        return "AI-Ready"
    if score >= 0.55:
        return "Partially Ready"
    if score >= 0.35:
        return "Significant Gaps"
    return "Not Ready"


def _status_label(score: float | None) -> str:
    """Map a 0-1 score to a status word."""
    if score is None:
        return "Not Run"
    if score >= 0.75:
        return "Strong"
    if score >= 0.55:
        return "Adequate"
    if score >= 0.35:
        return "Weak"
    return "Critical"


# ═══════════════════════════════════════════════════════════════════════════
# Module-level scorecard builders
# ═══════════════════════════════════════════════════════════════════════════

def _scorecard_quality(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 2 — Data Quality."""
    rpt = entry.quality_report
    if rpt is None:
        return ModuleScorecard(
            module="Data Quality & AI Readiness",
            module_number="2",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 2 to assess data quality and AI readiness."],
        )

    score = rpt.ai_readiness_score
    details = [
        f"AI Readiness: {score*100:.0f}% ({rpt.readiness_level})",
        f"Data: {rpt.row_count} rows × {rpt.column_count} columns",
        f"Completeness: {rpt.completeness_score*100:.0f}% · Deduplication: {rpt.deduplication_score*100:.0f}%",
        f"Process digitisation: {rpt.process_digitisation_score*100:.0f}% · Tool maturity: {rpt.tool_maturity_score*100:.0f}%",
    ]
    return ModuleScorecard(
        module="Data Quality & AI Readiness",
        module_number="2",
        ran=True,
        headline=f"{score*100:.0f}% AI-ready — {rpt.readiness_level}",
        score=score,
        status=_status_label(score),
        details=details,
    )


def _scorecard_benchmark(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 3 — Benchmarking."""
    rpt = entry.benchmark_report
    if rpt is None:
        return ModuleScorecard(
            module="Industry Benchmarking",
            module_number="3",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 3 to compare pricing and features against market."],
        )

    comp_score = (rpt.competitiveness_score or 50) / 100.0
    details = [
        f"Position: {rpt.price_position} (percentile: {rpt.price_percentile:.0f}%)",
        f"Price gap: {rpt.price_gap_pct:+.1f}% vs market average",
        f"Feature match: {rpt.feature_match_score:.0f}%",
    ]
    if rpt.competitiveness_score is not None:
        details.append(f"Competitiveness score: {rpt.competitiveness_score}/100 (Gemini)")

    return ModuleScorecard(
        module="Industry Benchmarking",
        module_number="3",
        ran=True,
        headline=f"{rpt.price_position} — feature match {rpt.feature_match_score:.0f}%",
        score=comp_score,
        status=_status_label(comp_score),
        details=details,
    )


def _scorecard_automation(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 4 — Automation."""
    rpt = entry.automation_report
    if rpt is None:
        return ModuleScorecard(
            module="Automation Opportunities",
            module_number="4",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 4 to identify automation opportunities."],
        )

    cov = rpt.summary.automation_coverage
    details = [
        f"{rpt.summary.automatable_steps}/{rpt.summary.total_steps} steps automatable ({cov*100:.0f}% coverage)",
        f"Already automated: {rpt.summary.already_automated}",
        f"Average confidence: {rpt.summary.avg_confidence*100:.0f}%",
    ]
    # Priority breakdown
    by_p = rpt.summary.by_priority
    if by_p:
        prio_str = ", ".join(f"{k}: {v}" for k, v in by_p.items() if v > 0)
        details.append(f"By priority: {prio_str}")

    return ModuleScorecard(
        module="Automation Opportunities",
        module_number="4",
        ran=True,
        headline=f"{rpt.summary.automatable_steps} steps automatable ({cov*100:.0f}% coverage)",
        score=cov,
        status=_status_label(cov),
        details=details,
    )


def _scorecard_consolidation(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 5 — Consolidation."""
    rpt = entry.consolidation_report
    if rpt is None:
        return ModuleScorecard(
            module="Data Consolidation",
            module_number="5",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 5 to analyse data fragmentation."],
        )

    score = rpt.consolidation_score
    details = [
        f"Consolidation: {score*100:.0f}%",
        f"Total silos: {rpt.total_silos} (informal: {rpt.informal_silos})",
        f"Manual data flows: {rpt.manual_flows}",
        f"Redundancies detected: {len(rpt.redundancies)}",
    ]
    return ModuleScorecard(
        module="Data Consolidation",
        module_number="5",
        ran=True,
        headline=f"{score*100:.0f}% consolidated — {rpt.total_silos} silos detected",
        score=score,
        status=_status_label(score),
        details=details,
    )


def _scorecard_roi(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 6 — ROI."""
    rpt = entry.roi_report
    if rpt is None:
        return ModuleScorecard(
            module="ROI Estimator",
            module_number="6",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 6 to estimate savings and payback."],
        )

    s = rpt.summary
    # Normalise ROI percentage to a 0-1 score (200%+ = 1.0, 0% = 0.0)
    roi_norm = min(1.0, max(0.0, s.roi_percentage / 200.0))
    payback_str = f"{s.overall_payback_months:.1f} months" if s.overall_payback_months else "N/A"

    details = [
        f"Annual savings: ₹{s.total_annual_cost_saved:,.0f}",
        f"Implementation cost: ₹{s.total_implementation_cost:,.0f}",
        f"Payback: {payback_str} · ROI: {s.roi_percentage:.0f}%",
        f"3-year net benefit: ₹{s.three_year_net_benefit:,.0f}",
    ]
    return ModuleScorecard(
        module="ROI Estimator",
        module_number="6",
        ran=True,
        headline=f"₹{s.total_annual_cost_saved:,.0f}/year savings — {payback_str} payback",
        score=roi_norm,
        status=_status_label(roi_norm),
        details=details,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Risk identification
# ═══════════════════════════════════════════════════════════════════════════

def _identify_risks(entry: SessionEntry) -> list[RiskItem]:
    """Scan all module outputs for risks and gaps."""
    risks: list[RiskItem] = []

    # --- From quality report ---
    qr = entry.quality_report
    if qr is not None:
        if qr.completeness_score < 0.80:
            risks.append(RiskItem(
                severity="High" if qr.completeness_score < 0.60 else "Medium",
                area="Data Quality",
                description=f"Data completeness is only {qr.completeness_score*100:.0f}% — missing values will degrade AI model accuracy",
                mitigation="Implement data validation rules and mandatory fields in data entry forms",
            ))
        if qr.deduplication_score < 0.90:
            risks.append(RiskItem(
                severity="Medium",
                area="Data Quality",
                description=f"Duplicate rows detected ({qr.duplicate_rows} duplicates) — inflates metrics and wastes storage",
                mitigation="Add unique constraints and run periodic deduplication scripts",
            ))
        if qr.process_digitisation_score < 0.30:
            risks.append(RiskItem(
                severity="Critical",
                area="Process Digitisation",
                description=f"Only {qr.process_digitisation_score*100:.0f}% of processes are digitised — AI adoption will be blocked without digital data capture",
                mitigation="Prioritise converting top 3 manual processes to digital forms or apps",
            ))
        if qr.tool_maturity_score < 0.40:
            risks.append(RiskItem(
                severity="High",
                area="Tool Maturity",
                description=f"Tool maturity is only {qr.tool_maturity_score*100:.0f}% — reliance on informal tools (paper/WhatsApp) blocks integration",
                mitigation="Adopt at least one cloud-based productivity tool (Google Sheets, POS, or CRM)",
            ))
        if qr.data_coverage_score < 0.50:
            risks.append(RiskItem(
                severity="Medium",
                area="Data Coverage",
                description=f"Data coverage is only {qr.data_coverage_score*100:.0f}% — key business data types (invoices, payroll, inventory) are not digitised",
                mitigation="Start collecting and digitising at least invoices and payroll data",
            ))

    # --- From consolidation report ---
    cr = entry.consolidation_report
    if cr is not None:
        if cr.informal_silos >= 3:
            risks.append(RiskItem(
                severity="Critical",
                area="Data Consolidation",
                description=f"{cr.informal_silos} informal data silos (paper, verbal, messaging) — permanent data loss risk",
                mitigation="Digitise the 3 most critical informal silos within 30 days",
            ))
        elif cr.informal_silos > 0:
            risks.append(RiskItem(
                severity="High",
                area="Data Consolidation",
                description=f"{cr.informal_silos} informal data silo(s) — data trapped in non-searchable, non-backed-up media",
                mitigation="Replace informal tools with digital alternatives (forms, spreadsheets, apps)",
            ))
        if cr.manual_flows >= 3:
            risks.append(RiskItem(
                severity="High",
                area="Data Flows",
                description=f"{cr.manual_flows} manual data transfers between tools — each is an error and delay risk",
                mitigation="Automate the highest-risk transfers first using API integrations or shared databases",
            ))

    # --- From ROI report ---
    rr = entry.roi_report
    if rr is not None:
        if rr.summary.net_first_year_benefit < 0:
            risks.append(RiskItem(
                severity="Medium",
                area="ROI",
                description=f"First-year ROI is negative (₹{rr.summary.net_first_year_benefit:,.0f}) — investment won't pay off immediately",
                mitigation="Phase implementation: start with quick wins that pay back in <6 months",
            ))

    # --- From automation report ---
    ar = entry.automation_report
    if ar is not None:
        if ar.summary.automation_coverage < 0.30:
            risks.append(RiskItem(
                severity="Medium",
                area="Automation",
                description=f"Only {ar.summary.automation_coverage*100:.0f}% of steps are automatable — most processes may require physical or human judgment",
                mitigation="Focus on the few automatable steps for maximum impact with minimal disruption",
            ))

    # Sort by severity
    severity_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    risks.sort(key=lambda r: severity_order.get(r.severity, 4))
    return risks


# ═══════════════════════════════════════════════════════════════════════════
# Strengths & weaknesses
# ═══════════════════════════════════════════════════════════════════════════

def _identify_strengths(entry: SessionEntry) -> list[str]:
    """Find things the business is doing well."""
    strengths: list[str] = []

    qr = entry.quality_report
    if qr is not None:
        if qr.completeness_score >= 0.90:
            strengths.append(f"Excellent data completeness ({qr.completeness_score*100:.0f}%) — records are well-maintained")
        if qr.deduplication_score >= 0.95:
            strengths.append("Minimal duplicate records — data hygiene is strong")
        if qr.process_digitisation_score >= 0.60:
            strengths.append(f"Good process digitisation ({qr.process_digitisation_score*100:.0f}%) — many workflows already use digital tools")
        if qr.tool_maturity_score >= 0.60:
            strengths.append(f"Reasonable tool maturity ({qr.tool_maturity_score*100:.0f}%) — using some structured software")

    br = entry.benchmark_report
    if br is not None:
        if br.price_position in ("Competitive", "Below Market"):
            strengths.append(f"Pricing is {br.price_position.lower()} — well-positioned against competitors")
        if br.feature_match_score >= 70:
            strengths.append(f"Strong feature alignment ({br.feature_match_score:.0f}%) with market leaders")

    ar = entry.automation_report
    if ar is not None:
        if ar.summary.automation_coverage >= 0.60:
            strengths.append(f"High automation potential ({ar.summary.automation_coverage*100:.0f}%) — many steps can be automated")
        if ar.summary.already_automated > 0:
            strengths.append(f"{ar.summary.already_automated} step(s) already automated — some digital maturity in place")

    rr = entry.roi_report
    if rr is not None:
        if rr.summary.overall_payback_months is not None and rr.summary.overall_payback_months <= 8:
            strengths.append(f"Fast payback ({rr.summary.overall_payback_months:.0f} months) — automation investment recovers quickly")
        if rr.summary.three_year_net_benefit > 0:
            strengths.append(f"Positive 3-year ROI (₹{rr.summary.three_year_net_benefit:,.0f}) — transformation is financially viable")

    return strengths[:6]


def _identify_weaknesses(entry: SessionEntry) -> list[str]:
    """Find the top areas that need improvement."""
    weaknesses: list[str] = []

    qr = entry.quality_report
    if qr is not None:
        if qr.completeness_score < 0.80:
            weaknesses.append(f"Poor data completeness ({qr.completeness_score*100:.0f}%) — too many missing values for reliable AI")
        if qr.process_digitisation_score < 0.30:
            weaknesses.append(f"Low process digitisation ({qr.process_digitisation_score*100:.0f}%) — most workflows are still manual")
        if qr.tool_maturity_score < 0.40:
            weaknesses.append(f"Low tool maturity ({qr.tool_maturity_score*100:.0f}%) — heavy reliance on paper and informal tools")
        if qr.consistency_score < 0.70:
            weaknesses.append(f"Data consistency issues ({qr.consistency_score*100:.0f}%) — inconsistent naming, formats, or mixed types")

    cr = entry.consolidation_report
    if cr is not None:
        if cr.consolidation_score < 0.30:
            weaknesses.append(f"Critically fragmented data ({cr.consolidation_score*100:.0f}%) — data spread across {cr.total_silos} silos")
        elif cr.consolidation_score < 0.50:
            weaknesses.append(f"Significant data fragmentation ({cr.consolidation_score*100:.0f}%) — {cr.total_silos} separate tools/media in use")
        if cr.informal_silos >= 3:
            weaknesses.append(f"{cr.informal_silos} informal data silos — high risk of data loss")

    ar = entry.automation_report
    if ar is not None:
        not_rec = ar.summary.not_recommended
        if not_rec > ar.summary.total_steps * 0.5:
            weaknesses.append(f"Many steps ({not_rec}/{ar.summary.total_steps}) are not automatable — physical/human-dependent processes dominate")

    rr = entry.roi_report
    if rr is not None:
        if rr.summary.net_first_year_benefit < 0:
            weaknesses.append(f"Negative first-year ROI (₹{rr.summary.net_first_year_benefit:,.0f}) — upfront cost exceeds first year savings")

    return weaknesses[:6]


# ═══════════════════════════════════════════════════════════════════════════
# Action plan builder
# ═══════════════════════════════════════════════════════════════════════════

def _build_action_plan(entry: SessionEntry) -> list[ActionItem]:
    """Build a prioritised action roadmap from all module outputs."""
    actions: list[ActionItem] = []
    priority = 0

    # --- Phase 1: Quick wins (Week 1-2) ---

    # From Module 4: Low-effort automation quick wins
    ar = entry.automation_report
    if ar is not None:
        quick = [c for c in ar.candidates if c.is_candidate and c.estimated_effort == "Low"]
        if quick:
            top = quick[0]
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Automate '{top.description[:60]}' ({top.automation_type})",
                source_module="Module 4 — Automation",
                impact=f"Low-effort automation of the easiest step — builds momentum",
                effort="Low",
                timeframe="Week 1–2",
            ))

    # From Module 5: Digitise most critical informal silo
    cr = entry.consolidation_report
    if cr is not None:
        informal_migrations = [
            m for m in cr.migration_steps if m.effort == "Low"
        ]
        if informal_migrations:
            top_m = informal_migrations[0]
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Replace '{top_m.from_tool}' with {top_m.to_tool[:50]}",
                source_module="Module 5 — Consolidation",
                impact=f"Eliminates data loss risk from informal tool '{top_m.from_tool}'",
                effort="Low",
                timeframe="Week 1–2",
            ))

    # --- Phase 2: Foundation building (Month 1) ---

    # From quality: fix data completeness if low
    qr = entry.quality_report
    if qr is not None and qr.completeness_score < 0.85:
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action="Add data validation rules and mandatory fields to all data entry points",
            source_module="Module 2 — Data Quality",
            impact=f"Raise data completeness from {qr.completeness_score*100:.0f}% towards 95%+",
            effort="Medium",
            timeframe="Month 1",
        ))

    # From consolidation: eliminate high-risk data flows
    if cr is not None and cr.manual_flows >= 2:
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action=f"Automate {cr.manual_flows} manual data transfers between tools",
            source_module="Module 5 — Consolidation",
            impact="Eliminate re-entry errors and verbal hand-off risks",
            effort="Medium",
            timeframe="Month 1",
        ))

    # --- Phase 3: Core automation (Month 2–3) ---

    # From Module 4: High-priority automation steps
    if ar is not None:
        high_priority = [
            c for c in ar.candidates
            if c.is_candidate and c.priority in ("Critical", "High") and c.estimated_effort != "Low"
        ]
        for hp in high_priority[:2]:
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Implement {hp.automation_type} for '{hp.description[:50]}'",
                source_module="Module 4 — Automation",
                impact=f"{hp.priority}-priority step — high time/cost savings potential",
                effort=hp.estimated_effort,
                timeframe="Month 2–3",
            ))

    # --- Phase 4: Medium-effort consolidation (Month 2–3) ---
    if cr is not None:
        medium_migrations = [m for m in cr.migration_steps if m.effort == "Medium"]
        if medium_migrations:
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Complete {len(medium_migrations)} medium-effort tool migrations",
                source_module="Module 5 — Consolidation",
                impact="Further reduce tool fragmentation and redundancy",
                effort="Medium",
                timeframe="Month 2–3",
            ))

    # --- Phase 5: Strategic (Quarter 2+) ---

    # Benchmark-driven pricing action
    br = entry.benchmark_report
    if br is not None:
        if br.price_position == "Uncompetitive":
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action="Review pricing strategy — currently above market at the premium tier",
                source_module="Module 3 — Benchmarking",
                impact="Pricing realignment could improve competitiveness and revenue",
                effort="Low",
                timeframe="Quarter 2",
            ))
        elif br.feature_match_score < 50:
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action="Enhance product features to match top competitors",
                source_module="Module 3 — Benchmarking",
                impact=f"Feature match score is only {br.feature_match_score:.0f}% — closing the gap improves market position",
                effort="High",
                timeframe="Quarter 2",
            ))

    # AI/ML opportunity (if readiness is high enough)
    if qr is not None and qr.ai_readiness_score >= 0.60 and ar is not None:
        aiml_steps = [c for c in ar.candidates if c.automation_type == "AI/ML" and c.is_candidate]
        if aiml_steps:
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Pilot AI/ML for '{aiml_steps[0].description[:50]}' — data readiness supports ML",
                source_module="Module 4 — Automation",
                impact="AI/ML can unlock deeper insights once foundation data is solid",
                effort="High",
                timeframe="Quarter 2–3",
            ))

    return actions[:10]


# ═══════════════════════════════════════════════════════════════════════════
# Key metrics summary
# ═══════════════════════════════════════════════════════════════════════════

def _build_key_metrics(entry: SessionEntry) -> dict[str, str]:
    """Build at-a-glance metrics dict."""
    metrics: dict[str, str] = {}

    qr = entry.quality_report
    if qr is not None:
        metrics["AI Readiness"] = f"{qr.ai_readiness_score*100:.0f}% ({qr.readiness_level})"
        metrics["Data Completeness"] = f"{qr.completeness_score*100:.0f}%"
        metrics["Process Digitisation"] = f"{qr.process_digitisation_score*100:.0f}%"

    br = entry.benchmark_report
    if br is not None:
        metrics["Market Position"] = br.price_position
        metrics["Feature Match"] = f"{br.feature_match_score:.0f}%"

    ar = entry.automation_report
    if ar is not None:
        metrics["Automation Coverage"] = f"{ar.summary.automation_coverage*100:.0f}%"
        metrics["Automatable Steps"] = f"{ar.summary.automatable_steps}/{ar.summary.total_steps}"

    cr = entry.consolidation_report
    if cr is not None:
        metrics["Consolidation Score"] = f"{cr.consolidation_score*100:.0f}%"
        metrics["Data Silos"] = f"{cr.total_silos} ({cr.informal_silos} informal)"

    rr = entry.roi_report
    if rr is not None:
        metrics["Annual Savings"] = f"₹{rr.summary.total_annual_cost_saved:,.0f}"
        metrics["Implementation Cost"] = f"₹{rr.summary.total_implementation_cost:,.0f}"
        payback = f"{rr.summary.overall_payback_months:.0f} mo" if rr.summary.overall_payback_months else "N/A"
        metrics["Payback Period"] = payback
        metrics["3-Year Net Benefit"] = f"₹{rr.summary.three_year_net_benefit:,.0f}"

    return metrics


# ═══════════════════════════════════════════════════════════════════════════
# Overall readiness score
# ═══════════════════════════════════════════════════════════════════════════

# Weights for each module in the overall composite.
# Sum = 1.0 when all modules have run; re-normalised if some are missing.
_MODULE_WEIGHTS = {
    "quality":       0.30,   # Data quality is foundational
    "automation":    0.25,   # Automation coverage is core to AI readiness
    "consolidation": 0.20,  # Data fragmentation blocks integration
    "roi":           0.15,   # Financial viability of transformation
    "benchmark":     0.10,   # Market positioning is context, not readiness
}


def _compute_overall_score(entry: SessionEntry) -> float:
    """Weighted average of available module scores."""
    scores: dict[str, float] = {}

    qr = entry.quality_report
    if qr is not None:
        scores["quality"] = qr.ai_readiness_score

    ar = entry.automation_report
    if ar is not None:
        scores["automation"] = ar.summary.automation_coverage

    cr = entry.consolidation_report
    if cr is not None:
        scores["consolidation"] = cr.consolidation_score

    rr = entry.roi_report
    if rr is not None:
        # Normalise ROI percentage: 200%+ → 1.0
        scores["roi"] = min(1.0, max(0.0, rr.summary.roi_percentage / 200.0))

    br = entry.benchmark_report
    if br is not None:
        scores["benchmark"] = (br.competitiveness_score or 50) / 100.0

    if not scores:
        return 0.0

    # Re-normalise weights to sum to 1.0 for available modules
    total_weight = sum(_MODULE_WEIGHTS[k] for k in scores)
    if total_weight == 0:
        return 0.0

    weighted_sum = sum(
        scores[k] * (_MODULE_WEIGHTS[k] / total_weight) for k in scores
    )
    return round(max(0.0, min(1.0, weighted_sum)), 2)


# ═══════════════════════════════════════════════════════════════════════════
# Executive report narrative
# ═══════════════════════════════════════════════════════════════════════════

def _build_executive_report(
    entry: SessionEntry,
    overall_score: float,
    verdict: OverallVerdict,
    scorecard: list[ModuleScorecard],
    strengths: list[str],
    weaknesses: list[str],
    risks: list[RiskItem],
    actions: list[ActionItem],
    key_metrics: dict[str, str],
) -> str:
    """Generate the full Markdown executive report."""
    industry = entry.company_metadata.get("industry", "Unknown")
    num_emp = entry.company_metadata.get("num_employees", "Unknown")
    tools = entry.company_metadata.get("tools_used", [])

    parts: list[str] = []

    # --- Header ---
    parts.append(f"# FoundationIQ — Strategic Diagnostic Report\n")
    parts.append(f"**Industry:** {industry} · **Employees:** {num_emp} · **Tools:** {', '.join(tools) if tools else 'N/A'}\n")

    # --- Overall verdict ---
    score_bar = "█" * int(overall_score * 20) + "░" * (20 - int(overall_score * 20))
    parts.append(f"## Overall AI Readiness: {overall_score*100:.0f}% — {verdict}\n")
    parts.append(f"`{score_bar}` {overall_score*100:.0f}/100\n")

    # --- Verdict summary ---
    verdict_text = _build_verdict_summary(entry, overall_score, verdict)
    parts.append(f"{verdict_text}\n")

    # --- Module scorecard ---
    parts.append("## Module Scorecard\n")
    parts.append("| Module | Status | Score | Headline |")
    parts.append("|---|---|---|---|")
    status_icons = {"Strong": "🟢", "Adequate": "🟡", "Weak": "🟠", "Critical": "🔴", "Not Run": "⚪"}
    for sc in scorecard:
        icon = status_icons.get(sc.status, "⚪")
        score_str = f"{sc.score*100:.0f}%" if sc.score is not None else "—"
        parts.append(f"| {sc.module_number}. {sc.module} | {icon} {sc.status} | {score_str} | {sc.headline} |")
    parts.append("")

    # --- Key metrics ---
    if key_metrics:
        parts.append("## Key Metrics at a Glance\n")
        parts.append("| Metric | Value |")
        parts.append("|---|---|")
        for k, v in key_metrics.items():
            parts.append(f"| {k} | **{v}** |")
        parts.append("")

    # --- Strengths ---
    if strengths:
        parts.append("## ✅ Strengths\n")
        for s in strengths:
            parts.append(f"- {s}")
        parts.append("")

    # --- Weaknesses ---
    if weaknesses:
        parts.append("## ⚠️ Areas for Improvement\n")
        for w in weaknesses:
            parts.append(f"- {w}")
        parts.append("")

    # --- Risk register ---
    if risks:
        severity_icons = {"Critical": "🔴", "High": "🟠", "Medium": "🟡", "Low": "🟢"}
        parts.append("## 🛡️ Risk Register\n")
        parts.append("| Severity | Area | Risk | Mitigation |")
        parts.append("|---|---|---|---|")
        for r in risks:
            icon = severity_icons.get(r.severity, "⚪")
            parts.append(f"| {icon} {r.severity} | {r.area} | {r.description[:80]} | {r.mitigation[:80]} |")
        parts.append("")

    # --- Action plan ---
    if actions:
        parts.append("## 🗺️ Implementation Roadmap\n")
        parts.append("| # | Action | Module | Effort | Timeframe |")
        parts.append("|---|---|---|---|---|")
        for a in actions:
            parts.append(
                f"| {a.priority} | {a.action[:70]} | {a.source_module} | {a.effort} | {a.timeframe} |"
            )
        parts.append("")

    # --- Closing ---
    parts.append("---\n")
    parts.append("*Report generated by FoundationIQ — AI Readiness & Automation Diagnostic Platform.*")
    parts.append(f"*Modules analysed: {sum(1 for sc in scorecard if sc.ran)}/5*\n")

    return "\n".join(parts)


def _build_verdict_summary(
    entry: SessionEntry,
    overall_score: float,
    verdict: OverallVerdict,
) -> str:
    """2-3 sentence plain-English verdict."""
    industry = entry.company_metadata.get("industry", "this business")
    num_emp = entry.company_metadata.get("num_employees", "a small team")

    modules_run = sum(1 for r in [
        entry.quality_report, entry.benchmark_report, entry.automation_report,
        entry.consolidation_report, entry.roi_report,
    ] if r is not None)

    # Count available details
    parts: list[str] = []

    if verdict == "AI-Ready":
        parts.append(
            f"This {industry} business with {num_emp} employees shows **strong AI readiness** "
            f"at {overall_score*100:.0f}%. The foundation for AI-driven transformation is in place."
        )
    elif verdict == "Partially Ready":
        parts.append(
            f"This {industry} business with {num_emp} employees is **partially ready** for AI adoption "
            f"at {overall_score*100:.0f}%. Key building blocks exist, but targeted improvements "
            f"are needed before AI tools can deliver their full value."
        )
    elif verdict == "Significant Gaps":
        parts.append(
            f"This {industry} business with {num_emp} employees has **significant gaps** "
            f"({overall_score*100:.0f}%) that must be addressed before AI adoption. "
            f"The priority is building a digital foundation — digitising manual processes, "
            f"consolidating data silos, and improving data quality."
        )
    else:
        parts.append(
            f"This {industry} business with {num_emp} employees is **not yet ready** for AI "
            f"({overall_score*100:.0f}%). Operations are primarily manual and data infrastructure "
            f"is fragmented. A phased digital transformation is needed before AI tools will add value."
        )

    # Add ROI context if available
    rr = entry.roi_report
    if rr is not None:
        s = rr.summary
        if s.three_year_net_benefit > 0:
            parts.append(
                f"The recommended improvements project **₹{s.total_annual_cost_saved:,.0f}/year** "
                f"in savings with a **{s.overall_payback_months:.0f}-month** payback "
                f"and **₹{s.three_year_net_benefit:,.0f}** 3-year net benefit."
            )
        else:
            parts.append(
                f"Projected annual savings are ₹{s.total_annual_cost_saved:,.0f}, but "
                f"the upfront investment of ₹{s.total_implementation_cost:,.0f} means the 3-year "
                f"net benefit is marginal. Phased implementation is recommended."
            )

    parts.append(f"Based on analysis of {modules_run} diagnostic module(s).")
    return " ".join(parts)


# ═══════════════════════════════════════════════════════════════════════════
# Main entry point
# ═══════════════════════════════════════════════════════════════════════════

def compute_strategic_verdict(
    session_id: str,
    entry: SessionEntry,
) -> StrategicVerdict:
    """Generate the final strategic verdict aggregating all module outputs.

    Requires at least one of the analysis modules (2–6) to have been run.

    Raises:
        ValueError: if no modules have been run yet.
    """
    has_any = any([
        entry.quality_report,
        entry.benchmark_report,
        entry.automation_report,
        entry.consolidation_report,
        entry.roi_report,
    ])
    if not has_any:
        raise ValueError(
            "Cannot generate strategic verdict without at least one analysis "
            "module (2–6). Run at least Module 2 (Data Quality) first."
        )

    # --- Build scorecard ---
    scorecard = [
        _scorecard_quality(entry),
        _scorecard_benchmark(entry),
        _scorecard_automation(entry),
        _scorecard_consolidation(entry),
        _scorecard_roi(entry),
    ]

    # --- Overall score & verdict ---
    overall_score = _compute_overall_score(entry)
    verdict = _verdict_label(overall_score)

    # --- Analysis ---
    strengths = _identify_strengths(entry)
    weaknesses = _identify_weaknesses(entry)
    risks = _identify_risks(entry)
    actions = _build_action_plan(entry)
    key_metrics = _build_key_metrics(entry)

    # --- Verdict summary ---
    verdict_summary = _build_verdict_summary(entry, overall_score, verdict)

    # --- Executive report ---
    executive_report = _build_executive_report(
        entry, overall_score, verdict, scorecard,
        strengths, weaknesses, risks, actions, key_metrics,
    )

    return StrategicVerdict(
        session_id=session_id,
        overall_readiness_score=overall_score,
        verdict=verdict,
        verdict_summary=verdict_summary,
        scorecard=scorecard,
        strengths=strengths,
        weaknesses=weaknesses,
        risks=risks,
        action_plan=actions,
        key_metrics=key_metrics,
        executive_report=executive_report,
    )
