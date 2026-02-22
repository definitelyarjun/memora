"""Module 4 — Automation Opportunity Detector (rule-based, no LLM).

Takes structured workflow steps + AI readiness score from session and
produces an AutomationReport classifying each step.

Classification pipeline (per step)
-----------------------------------
1. Already Automated?   → skip (flag as already_automated)
2. Keyword matching     → detect task nature (data entry, communication,
                          calculation, decision, inspection, delivery)
3. Automation type      → map task nature to best-fit automation type
4. Confidence scoring   → combine keyword signal + step context + readiness
5. Effort estimation    → based on automation type + tool landscape
6. Priority             → impact (manual + repetitive) × feasibility (readiness + effort)

Keyword dictionaries
---------------------
Each dictionary maps a task nature to sets of keywords that appear in
step descriptions.  The classifier checks description + actor + tool_used.

This is intentionally deterministic and transparent — no black-box LLM.
Every classification can be explained by pointing to which keywords matched.
"""

from __future__ import annotations

import re
from collections import Counter

from app.core.session_store import SessionEntry
from app.schemas.automation import (
    AutomationCandidate,
    AutomationReport,
    AutomationSummary,
    AutomationType,
    ConfidenceLevel,
)
from app.schemas.quality import QualityReport


# ---------------------------------------------------------------------------
# Keyword dictionaries for task-nature detection
# ---------------------------------------------------------------------------
# Each key is a "task nature"; value is a set of keyword phrases.
# Matching is case-insensitive on the step description.

_TASK_KEYWORDS: dict[str, set[str]] = {
    "data_entry": {
        "enter", "input", "record", "log", "register", "write",
        "fill", "type", "copy", "transfer", "transcribe", "update",
        "key in", "data entry", "ledger",
        # NOTE: diary/notebook removed — paper artifacts map to document_handling
    },
    "calculation": {
        "calculate", "compute", "sum", "total", "tally", "count",
        "add up", "subtract", "multiply", "estimate", "forecast",
        "reconcile", "balance", "audit",
        "calculator", "spreadsheet", "formula", "tally sheet",
    },
    "communication": {
        "call", "phone", "whatsapp", "sms", "email", "send",
        "notify", "inform", "message", "remind", "alert",
        "verbal", "tell", "announce",
    },
    "document_handling": {
        "print", "file", "store", "archive", "scan", "photocopy",
        "stamp", "sign", "paper", "invoice", "receipt",
        "hand", "deliver document", "sticky note", "note",
        # Paper-based artefacts — replacing these with a digital form
        "diary", "notebook", "logbook", "log book", "handwritten",
        "folder", "binder", "hard copy",
        # NOTE: "bill" removed — belongs only in payment to avoid misclassification
    },
    "decision": {
        "check", "verify", "approve", "reject", "decide", "review",
        "inspect", "threshold", "if", "whether", "confirm",
        "validate", "compare", "assess",
    },
    "physical": {
        "deliver", "pick up", "carry", "move", "transport",
        "prepare", "cook", "serve", "clean", "assemble",
        "package", "load", "unload", "stock", "shelve",
    },
    "scheduling": {
        "schedule", "book", "reserve", "assign", "allocate",
        "roster", "shift", "appointment", "calendar",
        "reservation", "slot",
        # Diary/notebook in scheduling context = replace with booking system
        "diary", "booking",
    },
    "reporting": {
        "report", "dashboard", "summary", "generate report",
        "daily report", "weekly report", "monthly report",
        "export", "chart", "graph", "analyse", "analyze",
    },
    "payment": {
        "pay", "payment", "collect", "charge", "bill",
        "invoice", "transaction", "pos", "cash", "card",
        "upi", "process payment",
    },
}

# ---------------------------------------------------------------------------
# Task nature → automation type mapping
# ---------------------------------------------------------------------------

_NATURE_TO_AUTOMATION: dict[str, AutomationType] = {
    "data_entry":        "RPA",
    "calculation":       "RPA",
    "communication":     "API Integration",
    "document_handling": "Digital Form",
    "decision":          "Decision Engine",
    "physical":          "Not Recommended",     # inherently physical
    "scheduling":        "API Integration",
    "reporting":         "RPA",
    "payment":           "API Integration",
}

# If AI readiness is high enough, some types upgrade
_AI_UPGRADE_ELIGIBLE: set[str] = {
    "decision", "reporting", "data_entry",
}

# ---------------------------------------------------------------------------
# Effort estimation by automation type
# ---------------------------------------------------------------------------

_TYPE_EFFORT: dict[AutomationType, str] = {
    "RPA":              "Medium",
    "Digital Form":     "Low",
    "API Integration":  "Medium",
    "AI/ML":            "High",
    "Decision Engine":  "Medium",
    "Not Recommended":  "High",   # because it implies process redesign
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _detect_task_natures(text: str) -> list[tuple[str, int, list[str]]]:
    """Return a list of (task_nature, match_count, matched_keywords) sorted by match strength.

    Scans the combined text (description + actor + tool) against all
    keyword dictionaries.  Returns natures with ≥1 match, strongest first.
    The matched_keywords list is used downstream to produce specific reasoning.
    """
    text_lower = text.lower()
    hits: list[tuple[str, int, list[str]]] = []
    for nature, keywords in _TASK_KEYWORDS.items():
        matched = [kw for kw in keywords if kw in text_lower]
        if matched:
            hits.append((nature, len(matched), matched))
    hits.sort(key=lambda x: x[1], reverse=True)
    return hits


def _confidence_for_step(
    natures: list[tuple[str, int, list[str]]],
    step_type: str,
    ai_readiness: float,
    automation_type: AutomationType,
) -> float:
    """Compute a 0–1 confidence score for the automation recommendation.

    Factors:
      1. Keyword signal strength (max matches / total keywords checked)
      2. Step type clarity (Manual → +0.15, Decision → +0.10, Unknown → −0.10)
      3. AI readiness influence (if AI/ML type, readiness heavily impacts confidence)
      4. Automation type reliability (RPA/Digital Form → more reliable than AI/ML)
    """
    # Base: keyword match strength (0.0 – 0.6 range)
    if natures:
        best_matches = natures[0][1]
        base = min(0.60, best_matches * 0.15)
    else:
        base = 0.10  # no keyword matches — very low confidence

    # Step type adjustment
    type_bonus = {
        "Manual": 0.20,    # Manual steps are prime candidates
        "Decision": 0.15,  # Decision steps are codifiable
        "Unknown": 0.05,   # Unclear → penalise
        "Automated": 0.0,  # Already automated — no bonus
    }
    base += type_bonus.get(step_type, 0.0)

    # Automation type reliability
    type_reliability = {
        "RPA": 0.15,
        "Digital Form": 0.20,
        "API Integration": 0.10,
        "Decision Engine": 0.10,
        "AI/ML": -0.05,     # uncertain, depends on data
        "Not Recommended": -0.10,
    }
    base += type_reliability.get(automation_type, 0.0)

    # AI readiness influence (for AI/ML, high readiness → big boost)
    if automation_type == "AI/ML":
        base += ai_readiness * 0.25  # up to +0.25 for perfect readiness
    elif automation_type != "Not Recommended":
        base += ai_readiness * 0.10  # general readiness adds small confidence

    return round(max(0.05, min(1.0, base)), 2)


def _confidence_level(conf: float) -> ConfidenceLevel:
    if conf >= 0.80:
        return "High"
    if conf >= 0.50:
        return "Medium"
    return "Low"


def _compute_priority(
    is_candidate: bool,
    step_type: str,
    confidence: float,
    effort: str,
    automation_type: AutomationType,
) -> str:
    """Assign implementation priority: Critical / High / Medium / Low / Skip.

    Priority = impact × feasibility
      Impact:      Manual + high confidence = high impact
      Feasibility: Low effort + high confidence = high feasibility
    """
    if not is_candidate:
        return "Skip"

    impact = 0.0
    if step_type == "Manual":
        impact += 0.50
    elif step_type == "Decision":
        impact += 0.30
    else:
        impact += 0.15

    impact += confidence * 0.50  # high confidence → high impact

    effort_factor = {"Low": 1.0, "Medium": 0.70, "High": 0.40}
    feasibility = effort_factor.get(effort, 0.50)

    score = impact * feasibility

    # Deliberate high bar for Critical — reserves it for genuinely urgent,
    # high-confidence, low-friction automations so the label stays meaningful.
    if score >= 0.85:
        return "Critical"
    if score >= 0.62:
        return "High"
    if score >= 0.40:
        return "Medium"
    return "Low"


# Human-friendly descriptions of what each automation type replaces
_AUTOMATION_RATIONALE: dict[str, str] = {
    "RPA":              "repetitive data work can be handled by a software bot",
    "Digital Form":     "paper/manual recording can be replaced with a digital form or app",
    "API Integration":  "manual handoffs between people/tools can be bridged by an API",
    "Decision Engine":  "the rule-based logic can be codified into an automated decision engine",
    "AI/ML":            "historical data patterns can train a model to handle this automatically",
}


def _build_reasoning(
    natures: list[tuple[str, int, list[str]]],
    step_type: str,
    automation_type: AutomationType,
    ai_readiness: float,
    is_candidate: bool,
) -> str:
    """Generate a specific, keyword-grounded explanation for the classification."""
    if not is_candidate and step_type == "Automated":
        return "Already automated — no action needed."

    if not is_candidate:
        if automation_type == "Not Recommended":
            return (
                "Involves physical/hands-on work that cannot be meaningfully "
                "automated with current technology."
            )
        return "Insufficient signal to recommend automation for this step."

    parts: list[str] = []

    # Lead with specific evidence — which keywords triggered the classification
    if natures:
        top_nature, _, matched_kws = natures[0]
        nature_label = top_nature.replace("_", " ")
        # Show up to 3 matched keywords as evidence
        examples = ", ".join(f'"{kw}"' for kw in sorted(matched_kws)[:3])
        parts.append(f"Flagged as a {nature_label} task (keywords: {examples})")

    # Why this automation type was chosen
    rationale = _AUTOMATION_RATIONALE.get(automation_type, "")
    if rationale:
        parts.append(rationale)

    # Step context
    if step_type == "Manual":
        parts.append("currently performed manually with no software support")
    elif step_type == "Decision":
        parts.append("decision criteria can be expressed as explicit rules")

    # AI/ML specific note
    if automation_type == "AI/ML":
        if ai_readiness >= 0.60:
            parts.append(
                f"AI readiness score ({ai_readiness*100:.0f}%) is sufficient "
                "to support ML model deployment"
            )
        else:
            parts.append(
                f"Note: AI readiness ({ai_readiness*100:.0f}%) is below 60% — "
                "build data foundations before pursuing AI/ML here"
            )

    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Classify a single step
# ---------------------------------------------------------------------------

def _classify_step(
    step,  # WorkflowStep
    ai_readiness: float,
) -> AutomationCandidate:
    """Run the full classification pipeline on one workflow step."""

    # 1. Already automated → not a candidate
    if step.step_type == "Automated":
        return AutomationCandidate(
            step_number=step.step_number,
            description=step.description,
            actor=step.actor,
            current_step_type=step.step_type,
            tool_used=step.tool_used,
            is_candidate=False,
            automation_type="Not Recommended",
            confidence=1.0,
            confidence_level="High",
            reasoning="This step is already automated — no action needed.",
            estimated_effort="Low",
            priority="Skip",
        )

    # 2. Build combined text for keyword matching
    combined = f"{step.description} {step.actor}"
    if step.tool_used:
        combined += f" {step.tool_used}"

    # 3. Detect task natures
    natures = _detect_task_natures(combined)

    # 4. Determine automation type
    if natures:
        primary_nature = natures[0][0]
        automation_type: AutomationType = _NATURE_TO_AUTOMATION.get(
            primary_nature, "Digital Form"
        )

        # AI/ML upgrade: if readiness is high and the task nature is eligible
        if (
            ai_readiness >= 0.60
            and primary_nature in _AI_UPGRADE_ELIGIBLE
            and natures[0][1] >= 2  # strong keyword signal
        ):
            automation_type = "AI/ML"

        # If readiness is too low for AI/ML, downgrade
        if automation_type == "AI/ML" and ai_readiness < 0.60:
            automation_type = _NATURE_TO_AUTOMATION.get(primary_nature, "RPA")
    else:
        # No keyword matches — default to Digital Form for manual, skip otherwise
        if step.step_type == "Manual":
            automation_type = "Digital Form"
            primary_nature = "unknown"
        else:
            automation_type = "Not Recommended"
            primary_nature = "unknown"

    # 5. Physical tasks are generally not automatable
    is_physical_only = (
        natures
        and natures[0][0] == "physical"
        and (len(natures) == 1 or natures[1][1] < natures[0][1])
    )
    if is_physical_only:
        automation_type = "Not Recommended"

    # 6. Determine candidacy
    is_candidate = (
        automation_type != "Not Recommended"
        and step.step_type != "Automated"
    )

    # 7. Confidence
    confidence = _confidence_for_step(
        natures, step.step_type, ai_readiness, automation_type
    )

    # 8. Effort
    effort = _TYPE_EFFORT.get(automation_type, "Medium")

    # 9. Priority
    priority = _compute_priority(
        is_candidate, step.step_type, confidence, effort, automation_type
    )

    # 10. Reasoning
    reasoning = _build_reasoning(
        natures, step.step_type, automation_type, ai_readiness, is_candidate
    )

    return AutomationCandidate(
        step_number=step.step_number,
        description=step.description,
        actor=step.actor,
        current_step_type=step.step_type,
        tool_used=step.tool_used,
        is_candidate=is_candidate,
        automation_type=automation_type,
        confidence=confidence,
        confidence_level=_confidence_level(confidence),
        reasoning=reasoning,
        estimated_effort=effort,
        priority=priority,
    )


# ---------------------------------------------------------------------------
# Recommendations
# ---------------------------------------------------------------------------

def _generate_recommendations(
    candidates: list[AutomationCandidate],
    ai_readiness: float,
) -> tuple[list[str], list[str]]:
    """Generate top recommendations and quick wins from the classified steps.

    Returns (top_recommendations, quick_wins).
    """
    recs: list[str] = []
    quick_wins: list[str] = []

    # Sort candidates by priority
    priority_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3, "Skip": 4}
    ranked = sorted(
        [c for c in candidates if c.is_candidate],
        key=lambda c: (priority_order.get(c.priority, 4), -c.confidence),
    )

    # Quick wins: Low effort + High/Medium confidence candidates
    for c in ranked:
        if c.estimated_effort == "Low" and c.confidence >= 0.50:
            quick_wins.append(
                f"Step {c.step_number} ({c.description[:60]}): "
                f"{c.automation_type} — {c.confidence_level} confidence, "
                f"low implementation effort."
            )
    quick_wins = quick_wins[:3]

    # Data readiness warning (insert first — must not be truncated)
    if ai_readiness < 0.60:
        recs.append(
            f"⚠️ AI readiness is {ai_readiness*100:.0f}% (below 60%). "
            "AI/ML-based automation is not recommended until data quality and "
            "operational maturity improve. Focus on RPA and Digital Form "
            "automations first."
        )

    # Top recommendations: highest-priority candidates
    for c in ranked[:5]:
        recs.append(
            f"**Step {c.step_number}** — {c.description[:60]}: "
            f"Automate via **{c.automation_type}** "
            f"(confidence: {c.confidence*100:.0f}%, effort: {c.estimated_effort}). "
            f"{c.reasoning}"
        )

    # Overall assessment
    candidate_count = sum(1 for c in candidates if c.is_candidate)
    total_manual = sum(1 for c in candidates if c.current_step_type == "Manual")
    if total_manual > 0 and candidate_count == 0:
        recs.append(
            "No automation candidates were identified despite manual steps existing. "
            "This typically indicates the workflow involves primarily physical labour "
            "that requires process redesign before automation becomes feasible."
        )

    return recs[:6], quick_wins


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def compute_automation_report(
    session_id: str,
    entry: SessionEntry,
) -> AutomationReport:
    """Classify every workflow step and produce an AutomationReport.

    Args:
        session_id: The session identifier.
        entry: The SessionEntry with workflow_analysis and quality_report populated.

    Returns:
        AutomationReport with per-step classifications and aggregate stats.

    Raises:
        ValueError: if workflow_analysis or quality_report is missing.
    """
    # --- Validate prerequisites ---
    workflow = entry.workflow_analysis
    if workflow is None or not getattr(workflow, "steps", None):
        raise ValueError(
            "No workflow analysis found. Run Module 1a first with a workflow "
            "description to generate workflow steps."
        )

    quality_report: QualityReport | None = entry.quality_report
    if quality_report is None:
        raise ValueError(
            "No quality report found. Run Module 2 (POST /analyze/quality) "
            "before running the Automation Detector."
        )

    ai_readiness = quality_report.ai_readiness_score
    readiness_level = quality_report.readiness_level

    # --- Classify each step ---
    candidates = [_classify_step(step, ai_readiness) for step in workflow.steps]

    # --- Aggregate stats ---
    automatable = [c for c in candidates if c.is_candidate]
    already_automated = [c for c in candidates if c.current_step_type == "Automated"]
    not_recommended = [
        c for c in candidates
        if not c.is_candidate and c.current_step_type != "Automated"
    ]

    # By type (only candidates)
    by_type = dict(Counter(c.automation_type for c in automatable))
    by_priority = dict(Counter(c.priority for c in automatable))

    total = len(candidates)
    coverage = (
        (len(automatable) + len(already_automated)) / total
        if total > 0
        else 0.0
    )
    avg_conf = (
        sum(c.confidence for c in automatable) / len(automatable)
        if automatable
        else 0.0
    )

    summary = AutomationSummary(
        total_steps=total,
        automatable_steps=len(automatable),
        already_automated=len(already_automated),
        not_recommended=len(not_recommended),
        automation_coverage=round(coverage, 4),
        avg_confidence=round(avg_conf, 4),
        by_type=by_type,
        by_priority=by_priority,
    )

    # --- Recommendations ---
    top_recs, quick_wins = _generate_recommendations(candidates, ai_readiness)

    return AutomationReport(
        session_id=session_id,
        ai_readiness_score=ai_readiness,
        readiness_level=readiness_level,
        candidates=candidates,
        summary=summary,
        top_recommendations=top_recs,
        quick_wins=quick_wins,
    )
