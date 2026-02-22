"""Module 7 — Strategic Verdict Generator (rule-based, no LLM).

Aggregates outputs from all preceding modules into a single executive
diagnostic report.  No new analysis is performed — this is purely a
synthesis/consolidation layer.

Module mapping
--------------
  Module 2  — Data Quality & AI Readiness  → quality_report
  Module 3  — Bottleneck & Speed Analyzer  → benchmark_report
  Module 4  — Role & Automation Auditor    → automation_report
  Module 5  — Financial Impact Simulator   → financial_report
  Module 6  — Growth & Retention           → retention_report

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
    """Build scorecard for Module 3 — Bottleneck & Speed."""
    rpt = entry.benchmark_report
    if rpt is None:
        return ModuleScorecard(
            module="Bottleneck & Speed",
            module_number="3",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 3 to analyse pipeline TAT and bottlenecks."],
        )

    score = round(1.0 - (rpt.bottleneck_pct / 100.0), 4)
    details = [
        f"Closed inquiries: {rpt.closed_inquiries}/{rpt.total_inquiries}",
        f"Avg TAT: {rpt.avg_tat_hours:.1f}h (median: {rpt.median_tat_hours:.1f}h)",
        f"Bottlenecks (>48h): {rpt.bottleneck_count} ({rpt.bottleneck_pct:.0f}%)",
        f"TAT improvement with automation: {rpt.avg_tat_improvement_pct:.0f}%  (Metric 11)",
        f"Total hours saved: {rpt.total_hours_saved:.0f}h  (Metric 4)",
    ]
    return ModuleScorecard(
        module="Bottleneck & Speed",
        module_number="3",
        ran=True,
        headline=f"{rpt.bottleneck_count} bottleneck(s) · avg TAT {rpt.avg_tat_hours:.1f}h",
        score=score,
        status=_status_label(score),
        details=details,
    )


def _scorecard_automation(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 4 — Role & Automation Auditor."""
    rpt = entry.automation_report
    if rpt is None:
        return ModuleScorecard(
            module="Role & Automation Auditor",
            module_number="4",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 4 to audit role automation potential and RPE lift."],
        )

    cov = rpt.automation_coverage
    rpe = rpt.rpe_metrics
    vuln_breakdown = (
        f"High: {rpt.high_vulnerability_count} · "
        f"Medium: {rpt.medium_vulnerability_count} · "
        f"Low: {rpt.low_vulnerability_count}"
    )
    details = [
        f"{rpt.total_employees} employees audited · avg automation potential: {rpt.avg_automation_pct:.0f}% (Metric 3)",
        f"Vulnerability: {vuln_breakdown}",
        f"Top role: {rpt.top_automatable_role} ({rpt.top_automatable_pct:.0f}% automatable)",
        f"Hours freed/week: {rpt.total_hours_saved_per_week:.0f}h across team",
        f"RPE lift: ₹{rpe.current_rpe_monthly:,.0f} → ₹{rpe.projected_rpe_monthly:,.0f}/mo (+{rpe.rpe_lift_pct:.0f}%) (Metric 8)",
    ]
    return ModuleScorecard(
        module="Role & Automation Auditor",
        module_number="4",
        ran=True,
        headline=f"{rpt.high_vulnerability_count} high-risk roles · RPE lift {rpe.rpe_lift_pct:.0f}%",
        score=cov,
        status=_status_label(cov),
        details=details,
    )


def _scorecard_financial(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 5 — Financial Impact & ROI Simulator."""
    rpt = entry.financial_report
    if rpt is None:
        return ModuleScorecard(
            module="Financial Impact Simulator",
            module_number="5",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 5 to compute CFO-level savings, margin lift, and opportunity cost."],
        )

    # Score = net margin lift normalised (15pp lift = perfect score)
    score = min(1.0, max(0.0, rpt.gross_margin_lift_pct / 15.0))
    details = [
        f"Net monthly savings (Metric 5): ₹{rpt.net_monthly_savings_inr:,.0f}/month",
        f"Operating margin: {rpt.current_operating_margin_pct:.1f}% → {rpt.projected_operating_margin_pct:.1f}% (+{rpt.gross_margin_lift_pct:.1f}pp, Metric 12)",
        f"Opportunity cost of delay (Metric 7): ₹{rpt.opportunity_cost_per_month_inr:,.0f}/month",
        f"AI tools required: {rpt.new_ai_tools_monthly_cost_inr:,.0f} INR/month new tooling",
    ]
    return ModuleScorecard(
        module="Financial Impact Simulator",
        module_number="5",
        ran=True,
        headline=(
            f"₹{rpt.net_monthly_savings_inr/100_000:.1f}L/mo savings · "
            f"+{rpt.gross_margin_lift_pct:.1f}pp margin lift"
        ),
        score=score,
        status=_status_label(score),
        details=details,
    )


def _scorecard_retention(entry: SessionEntry) -> ModuleScorecard:
    """Build scorecard for Module 6 — Growth & Retention Benchmarking."""
    rpt = entry.retention_report
    if rpt is None:
        return ModuleScorecard(
            module="Growth & Retention Benchmarking",
            module_number="6",
            ran=False,
            headline="Not run",
            score=None,
            status="Not Run",
            details=["Run Module 6 to benchmark churn and project NRR."],
        )

    # Score: NRR vs benchmark (at benchmark = 0.8, +10pp above = 1.0, -20pp below = 0)
    nrr_ratio = rpt.projected_nrr_pct / max(1.0, rpt.nrr_benchmark_pct)
    score = min(1.0, max(0.0, nrr_ratio * 0.9))
    details = [
        f"Win rate: {rpt.win_rate_pct:.1f}% · Repeat rate: {rpt.repeat_rate_pct:.1f}%",
        f"Churn (Metric 9): {rpt.current_churn_pct:.1f}% → {rpt.projected_churn_pct:.1f}% (−{rpt.churn_reduction_pct:.1f}pp)",
        f"NRR (Metric 10): {rpt.current_nrr_pct:.0f}% → {rpt.projected_nrr_pct:.0f}% (benchmark: {rpt.nrr_benchmark_pct:.0f}%)",
    ]
    return ModuleScorecard(
        module="Growth & Retention Benchmarking",
        module_number="6",
        ran=True,
        headline=(
            f"Churn {rpt.current_churn_pct:.1f}% → {rpt.projected_churn_pct:.1f}% · "
            f"NRR {rpt.projected_nrr_pct:.0f}%"
        ),
        score=round(score, 2),
        status=_status_label(score),
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

    # --- From financial report ---
    fr = entry.financial_report
    if fr is not None:
        if fr.opportunity_cost_per_month_inr > fr.current_mrr * 0.05:
            risks.append(RiskItem(
                severity="Critical",
                area="Financial Impact",
                description=(
                    f"₹{fr.opportunity_cost_per_month_inr:,.0f}/month being lost to operational "
                    "inefficiency — every month of inaction directly erodes profit (Metric 7)"
                ),
                mitigation="Prioritise AI tooling rollout within 30 days to recover opportunity cost.",
            ))
        if fr.net_monthly_savings_inr <= 0:
            risks.append(RiskItem(
                severity="High",
                area="Financial Impact",
                description="AI tool costs currently exceed projected savings — tool selection needs review",
                mitigation="Re-evaluate AI tool choices; prioritise tools already in stack (zero marginal cost).",
            ))

    # --- From automation report ---
    ar = entry.automation_report
    if ar is not None:
        if ar.high_vulnerability_count > 0:
            risks.append(RiskItem(
                severity="Medium",
                area="Role Automation",
                description=(
                    f"{ar.high_vulnerability_count} role(s) are >60% automatable — "
                    "these staff are at risk of task displacement without proactive upskilling"
                ),
                mitigation="Invest in upskilling programmes for high-vulnerability roles before automating their tasks",
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
        if br.bottleneck_count == 0:
            strengths.append("Zero TAT bottlenecks — all inquiries close within 48 hours")
        if br.avg_tat_hours < 24:
            strengths.append(f"Excellent pipeline speed — average TAT {br.avg_tat_hours:.1f}h (well under 24h)")

    ar = entry.automation_report
    if ar is not None:
        if ar.rpe_metrics.rpe_lift_pct >= 40:
            strengths.append(
                f"Strong RPE growth potential — revenue per employee projected to grow "
                f"+{ar.rpe_metrics.rpe_lift_pct:.0f}% without adding headcount (Metric 8)"
            )
        if ar.low_vulnerability_count >= ar.total_employees * 0.5:
            strengths.append(
                f"{ar.low_vulnerability_count} role(s) are low-vulnerability — "
                "the leadership and technical team are well-positioned for the AI era"
            )

    fr = entry.financial_report
    if fr is not None and fr.gross_margin_lift_pct >= 10.0:
        strengths.append(
            f"Strong operating margin improvement — +{fr.gross_margin_lift_pct:.1f}pp projected "
            "with full AI adoption (Metric 12)"
        )

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

    cr = entry.financial_report
    if cr is not None:
        if cr.gross_margin_lift_pct < 5.0:
            weaknesses.append(
                f"Operating margin gain <5pp ({cr.gross_margin_lift_pct:.1f}pp) — "
                "may not justify AI implementation cost without phased rollout"
            )

    ar = entry.automation_report
    if ar is not None:
        if ar.high_vulnerability_count > ar.total_employees * 0.5:
            weaknesses.append(
                f"Over half the team ({ar.high_vulnerability_count}/{ar.total_employees} roles) "
                "have >60% automation potential — significant upskilling investment needed before automation"
            )

    return weaknesses[:6]


# ═══════════════════════════════════════════════════════════════════════════
# Action plan builder
# ═══════════════════════════════════════════════════════════════════════════

def _build_action_plan(entry: SessionEntry) -> list[ActionItem]:
    """Build a prioritised action roadmap from all module outputs."""
    actions: list[ActionItem] = []
    priority = 0

    # --- Phase 1: Quick wins (Week 1-2) ---

    # From Module 4: Target highest-vulnerability role first
    ar = entry.automation_report
    if ar is not None:
        high_vuln = [r for r in ar.roles if r.vulnerability_level == "High"]
        if high_vuln:
            top_r = max(high_vuln, key=lambda r: r.automation_pct)
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=(
                    f"Automate top tasks for '{top_r.job_title}': "
                    f"{', '.join(top_r.automatable_tasks[:2])}"
                ),
                source_module="Module 4 — Role Auditor",
                impact=(
                    f"Frees ~{top_r.hours_saved_per_week:.0f}h/week from {top_r.job_title} · "
                    f"{top_r.automation_pct:.0f}% of this role is automatable"
                ),
                effort="Low",
                timeframe="Week 1–2",
            ))

    # From Module 5: Implement top AI tool recommendation
    fr = entry.financial_report
    if fr is not None and fr.ai_tool_recommendations:
        top_tool = fr.ai_tool_recommendations[0]
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action=f"Deploy {top_tool.tool_name} for {top_tool.for_role_category}",
            source_module="Module 5 — Financial Impact",
            impact=(
                f"Frees ₹{fr.net_monthly_savings_inr:,.0f}/month net savings (Metric 5). "
                f"Cost: ₹{top_tool.monthly_cost_inr:,.0f}/mo"
                if not top_tool.already_in_stack
                else f"Zero new cost — already in stack. Optimise setup to capture savings."
            ),
            effort="Low" if top_tool.already_in_stack else "Medium",
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

    # From financial report: eliminate opportunity cost
    if fr is not None and fr.opportunity_cost_per_month_inr > fr.current_mrr * 0.03:
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action=(
                f"Roll out remaining AI tools to capture ₹{fr.net_monthly_savings_inr:,.0f}/mo savings"
            ),
            source_module="Module 5 — Financial Impact",
            impact=(
                f"Recover ₹{fr.opportunity_cost_per_month_inr:,.0f}/month in opportunity cost (Metric 7) "
                f"and boost margin by +{fr.gross_margin_lift_pct:.1f}pp (Metric 12)"
            ),
            effort="Medium",
            timeframe="Month 1",
        ))

    # --- Phase 3: Upskill + automate remaining roles (Month 2–3) ---

    if ar is not None and ar.high_vulnerability_count > 0:
        medium_vuln = [r for r in ar.roles if r.vulnerability_level == "Medium"]
        if medium_vuln:
            skills = list({r.upskilling_rec.split(",")[0].strip() for r in medium_vuln})[:2]
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=f"Upskill {len(medium_vuln)} medium-vulnerability role(s) in: {', '.join(skills)}",
                source_module="Module 4 — Role Auditor",
                impact="Prepare staff to supervise and extend automation instead of being replaced by it",
                effort="Medium",
                timeframe="Month 2–3",
            ))

    # --- Phase 4: Strategic AI scaling (Month 2–3) ---
    if fr is not None and fr.months_to_break_even is not None:
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action=f"Track AI savings dashboard — target break-even in {fr.months_to_break_even:.0f} months",
            source_module="Module 5 — Financial Impact",
            impact=(
                f"Annual savings of ₹{fr.net_annual_savings_inr:,.0f} recur from year 2 onwards"
            ),
            effort="Low",
            timeframe="Month 2–3",
        ))

    # --- Phase 5: Strategic (Quarter 2+) ---

    # Bottleneck-driven automation action (Module 3)
    br = entry.benchmark_report
    if br is not None:
        if br.bottleneck_count > 0:
            priority += 1
            actions.append(ActionItem(
                priority=priority,
                action=(
                    f"Automate payment follow-up for {br.bottleneck_count} bottleneck inquiry(ies) "
                    f"(avg TAT {br.avg_tat_hours:.1f}h → target 2h)"
                ),
                source_module="Module 3 — Bottleneck & Speed",
                impact=(
                    f"Eliminate {br.bottleneck_pct:.0f}% of inquiries stuck >48h · "
                    f"save ~{br.total_hours_saved:.0f}h/cycle (Metric 4)"
                ),
                effort="Medium",
                timeframe="Quarter 2",
            ))

    # RPE leverage (if high growth projected)
    if ar is not None and ar.rpe_metrics.rpe_lift_pct >= 30 and ar.rpe_metrics.current_mrr > 0:
        priority += 1
        actions.append(ActionItem(
            priority=priority,
            action=(
                f"Scale revenue to ₹{ar.rpe_metrics.projected_mrr:,.0f}/mo "
                f"without new hires — automation enables RPE growth from "
                f"₹{ar.rpe_metrics.current_rpe_monthly:,.0f} to "
                f"₹{ar.rpe_metrics.projected_rpe_monthly:,.0f}/employee/mo"
            ),
            source_module="Module 4 — Role Auditor",
            impact=f"RPE lift of +{ar.rpe_metrics.rpe_lift_pct:.0f}% (Metric 8) — same team, higher revenue",
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
        metrics["Avg TAT"] = f"{br.avg_tat_hours:.1f}h"
        metrics["Bottlenecks (>48h)"] = f"{br.bottleneck_count} ({br.bottleneck_pct:.0f}%)"
        metrics["Hours Saved"] = f"{br.total_hours_saved:.0f}h  (Metric 4)"

    ar = entry.automation_report
    if ar is not None:
        metrics["Avg Role Automation"] = f"{ar.avg_automation_pct:.0f}% (Metric 3)"
        metrics["High-Vuln Roles"] = f"{ar.high_vulnerability_count}/{ar.total_employees}"
        metrics["RPE Lift"] = f"+{ar.rpe_metrics.rpe_lift_pct:.0f}% (Metric 8)"

    fr = entry.financial_report
    if fr is not None:
        metrics["Net Monthly Savings"] = f"₹{fr.net_monthly_savings_inr:,.0f}/mo (Metric 5)"
        metrics["Operating Margin Lift"] = f"+{fr.gross_margin_lift_pct:.1f}pp (Metric 12)"
        metrics["Opportunity Cost/Month"] = f"₹{fr.opportunity_cost_per_month_inr:,.0f} (Metric 7)"

    return metrics


# ═══════════════════════════════════════════════════════════════════════════
# Overall readiness score
# ═══════════════════════════════════════════════════════════════════════════

# Weights for each module in the overall composite.
# Sum = 1.0 when all modules have run; re-normalised if some are missing.
_MODULE_WEIGHTS = {
    "quality":    0.30,   # Data quality is foundational
    "automation": 0.25,   # Automation coverage is core to AI readiness
    "financial":  0.20,   # Financial impact drives investment decisions
    "retention":  0.20,   # Churn & NRR benchmarking
    "benchmark":  0.05,   # Market positioning is context, not readiness
}


def _compute_overall_score(entry: SessionEntry) -> float:
    """Weighted average of available module scores."""
    scores: dict[str, float] = {}

    qr = entry.quality_report
    if qr is not None:
        scores["quality"] = qr.ai_readiness_score

    ar = entry.automation_report
    if ar is not None:
        scores["automation"] = ar.automation_coverage

    fr = entry.financial_report
    if fr is not None:
        # Normalise margin lift: 15pp+ → 1.0
        scores["financial"] = min(1.0, max(0.0, fr.gross_margin_lift_pct / 15.0))

    ret = entry.retention_report
    if ret is not None:
        nrr_ratio = ret.projected_nrr_pct / max(1.0, ret.nrr_benchmark_pct)
        scores["retention"] = min(1.0, max(0.0, nrr_ratio * 0.9))

    br = entry.benchmark_report
    if br is not None:
        scores["benchmark"] = round(1.0 - br.bottleneck_pct / 100.0, 4)

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
        entry.financial_report, entry.retention_report,
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
        entry.financial_report,
        entry.retention_report,
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
        _scorecard_financial(entry),
        _scorecard_retention(entry),
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
