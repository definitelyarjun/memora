"""LLM service — LangChain + Google Gemini.

FoundationIQ 3.0 (Startup Edition)

Functions:
  - analyse_startup_profile: Takes onboarding answers → MRR trend,
    growth gap analysis, tech stack maturity, executive summary
  - analyse_workflow: Workflow text → structured steps + Mermaid diagram
    (used by Module 3 — Workflow Bottleneck & Speed)
  - analyse_benchmark: Market benchmarking analysis

All prompts are tuned for early-stage tech startups (SaaS, EdTech,
FinTech, E-commerce) operating at the ₹50K–₹50L MRR range.
"""

from __future__ import annotations

import json
import re

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate

from app.core.config import settings
from app.schemas.ingestion import (
    StartupProfileAnalysis,
    WorkflowDiagram,
    WorkflowStep,
)
# ---------------------------------------------------------------------------
# Startup Profile Analysis prompt (NEW — Module 1)
# ---------------------------------------------------------------------------

_STARTUP_PROFILE_SYSTEM_PROMPT = """
You are a startup diagnostics analyst specialising in early-stage Indian tech startups (SaaS, EdTech, FinTech, E-commerce).

You will receive a startup's onboarding profile including:
- Company name and vertical (sub-type)
- Last 3 months of MRR (Monthly Recurring Revenue in ₹)
- Target monthly growth goal (%)
- Patience horizon (months willing to wait for results)
- Current tech stack
- Team size

Your job is to analyse this profile and return a JSON object with:

1. **mrr_trend**: "Growing" if MRR increased both months, "Declining" if it decreased both months, "Flat" if change is < 2% or mixed.
2. **mrr_mom_growth_pct**: Average month-on-month MRR growth percentage across the 2 transitions (round to 1 decimal).
3. **growth_gap**: A concise sentence comparing actual MRR growth to the target growth goal. Be specific with numbers.
4. **tech_stack_maturity**: "Early" if < 3 tools or mostly manual/spreadsheet tools, "Developing" if 3-6 tools with some automation, "Mature" if 7+ tools or has dedicated CRM/billing/analytics platforms.
5. **key_observations**: 3-5 bullet-point observations about the startup's current position, MRR trajectory, team efficiency, and readiness for AI/automation.
6. **recommended_focus_areas**: Top 2-3 areas the subsequent diagnostic modules should focus on (e.g., "churn reduction", "sales cycle speed", "cost optimisation", "role automation").
7. **executive_summary**: A single paragraph summarising the startup's profile, strengths, and potential areas for improvement through AI/automation.

Return a single valid JSON object. Do NOT include any text outside the JSON.

{{
  "mrr_trend": "Growing" | "Flat" | "Declining",
  "mrr_mom_growth_pct": <float>,
  "growth_gap": "<string>",
  "tech_stack_maturity": "Early" | "Developing" | "Mature",
  "key_observations": ["<string>", ...],
  "recommended_focus_areas": ["<string>", ...],
  "executive_summary": "<string>"
}}
""".strip()

_STARTUP_PROFILE_HUMAN_PROMPT = """
Startup Profile:
  Company: {company_name}
  Vertical: {sub_type}
  MRR (last 3 months): ₹{mrr_1:,.0f} → ₹{mrr_2:,.0f} → ₹{mrr_3:,.0f}
  Target monthly growth: {growth_goal_pct}%
  Patience horizon: {patience_months} months
  Team size: {num_employees} employees
  Tech stack: {tech_stack}
""".strip()

_STARTUP_PROFILE_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _STARTUP_PROFILE_SYSTEM_PROMPT),
    ("human", _STARTUP_PROFILE_HUMAN_PROMPT),
])


# ---------------------------------------------------------------------------
# Workflow Analysis prompt (updated for tech startups — used by Module 3)
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """
You are a startup operations analyst specialising in workflow optimisation for early-stage tech companies (SaaS, EdTech, FinTech, E-commerce).

You will receive a description of a startup's operational workflow — typically around sales inquiries, customer onboarding, support, or internal processes.

Your job is to:
1. Extract every distinct step in the workflow, in order.
2. For each step identify: who performs it (actor/role), whether it is Manual, Automated, a Decision point, or Unknown, and any tool mentioned.
3. Produce a Mermaid flowchart diagram that visually represents the workflow with actors shown in parentheses.
4. Write a concise one-paragraph executive summary identifying bottlenecks, manual handoffs, and automation opportunities relevant to a startup trying to scale efficiently.

CRITICAL MERMAID FORMATTING RULES — YOU MUST FOLLOW THESE EXACTLY:
- Every Mermaid statement MUST be on its own line separated by \\n
- Use \\n between EVERY node, arrow, and subgraph statement
- NEVER put multiple statements on a single line
- subgraph and end keywords must each be on their own line
- Correct example:
  "flowchart TD\\n    A[Start] --> B[Step 1]\\n    B --> C{{Decision?}}\\n    C -- Yes --> D[Do it]\\n    C -- No --> E[Skip]\\n    D --> F[End]\\n    E --> F"

Return your answer as a single valid JSON object with this exact shape:
{{
  "steps": [
    {{
      "step_number": 1,
      "description": "...",
      "actor": "...",
      "step_type": "Manual" | "Automated" | "Decision" | "Unknown",
      "tool_used": "..." | null
    }}
  ],
  "mermaid_diagram": "flowchart TD\\n    A[First step] --> B[Second step]\\n    B --> C[Third step]",
  "summary": "..."
}}

Do not include any text outside the JSON object.
""".strip()

_HUMAN_PROMPT = "Workflow description:\n\n{workflow_text}"

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", _SYSTEM_PROMPT),
    ("human", _HUMAN_PROMPT),
])


# ---------------------------------------------------------------------------
# LLM client (lazy-initialised so missing key doesn't crash on import)
# ---------------------------------------------------------------------------

_llm: ChatGoogleGenerativeAI | None = None


def _get_llm() -> ChatGoogleGenerativeAI:
    global _llm
    if _llm is None:
        if not settings.gemini_api_key:
            raise RuntimeError(
                "GEMINI_API_KEY is not set. Add it to backend/.env and restart."
            )
        _llm = ChatGoogleGenerativeAI(
            model=settings.gemini_model,
            google_api_key=settings.gemini_api_key,
            temperature=0.2,
        )
    return _llm


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def analyse_startup_profile(profile: dict) -> StartupProfileAnalysis:
    """Send startup onboarding profile to Gemini and return structured analysis.

    Args:
        profile: Dict with keys matching StartupProfile fields.

    Returns:
        StartupProfileAnalysis with MRR trend, growth gap, tech maturity,
        observations, focus areas, and executive summary.

    Raises:
        RuntimeError: if GEMINI_API_KEY is not set.
        ValueError: if the LLM returns unparseable JSON.
    """
    llm = _get_llm()
    chain = _STARTUP_PROFILE_PROMPT | llm

    mrr = profile.get("mrr_last_3_months", [0, 0, 0])
    tech_stack = profile.get("current_tech_stack", [])

    response = chain.invoke({
        "company_name": profile.get("company_name", "Unknown"),
        "sub_type": profile.get("sub_type", "SaaS"),
        "mrr_1": mrr[0] if len(mrr) > 0 else 0,
        "mrr_2": mrr[1] if len(mrr) > 1 else 0,
        "mrr_3": mrr[2] if len(mrr) > 2 else 0,
        "growth_goal_pct": profile.get("monthly_growth_goal_pct", 10),
        "patience_months": profile.get("patience_months", 6),
        "num_employees": profile.get("num_employees", 1),
        "tech_stack": ", ".join(tech_stack) if tech_stack else "None specified",
    })
    raw: str = response.content  # type: ignore[union-attr]

    # Strip markdown code fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LLM returned invalid JSON for profile analysis: {exc}\n\nRaw response:\n{raw}"
        )

    return StartupProfileAnalysis(**data)


def analyse_workflow(workflow_text: str) -> WorkflowDiagram:
    """Send workflow text to Gemini and return a structured WorkflowDiagram.

    Raises RuntimeError if the API key is missing.
    Raises ValueError if the LLM response cannot be parsed.
    """
    llm = _get_llm()
    chain = _PROMPT | llm

    response = chain.invoke({"workflow_text": workflow_text})
    raw: str = response.content  # type: ignore[union-attr]

    # Strip markdown code fences if the model wraps its JSON
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"LLM returned invalid JSON: {exc}\n\nRaw response:\n{raw}")

    steps = [WorkflowStep(**s) for s in data.get("steps", [])]

    return WorkflowDiagram(
        steps=steps,
        mermaid_diagram=data.get("mermaid_diagram", ""),
        summary=data.get("summary", ""),
    )


# ---------------------------------------------------------------------------
# Market benchmarking (text-based structured analysis)
# ---------------------------------------------------------------------------

_BENCHMARK_SYSTEM_PROMPT = """
You are a senior growth strategy consultant specialising in early-stage tech startups in India — SaaS, EdTech, FinTech, and E-commerce companies.
You will receive structured market data about a startup's product and its competitive landscape.

Context: The companies you analyse are typically at ₹50K–₹50L MRR, 5-50 employees, and looking to scale with AI/automation.

Return a single JSON object with exactly these fields:
{
  "competitiveness_score": <integer 0-100>,
  "strategic_recommendation": "<2-3 sentence actionable pricing and positioning advice tailored to a scaling startup>",
  "suggested_price": <number or null>,
  "key_insights": ["<insight 1>", "<insight 2>", "<insight 3>"],
  "confidence": "<High|Medium|Low>"
}

Scoring guide:
  90-100  → Clear market leader, strong positioning
  70-89   → Competitive with minor gaps
  50-69   → Below average, needs improvement
  30-49   → Significant disadvantage
  0-29    → Uncompetitive, major repositioning required

Confidence:
  High   → 10+ comparable competitors in dataset
  Medium → 5-9 comparable competitors
  Low    → fewer than 5 comparable competitors

Do NOT include any text outside the JSON object.
""".strip()


def analyse_benchmark(
    product_name: str,
    user_price: float,
    currency: str,
    features: list[str],
    category: str,
    market_stats: dict,
    feature_match_score: float,
    top_competitors: list[dict],
    company_metadata: dict | None,
) -> dict:
    """Send competitive market data to Gemini and return a strategy analysis.

    Args:
        product_name: User's product/service name.
        user_price: Current selling price.
        currency: Currency code.
        features: User's product features.
        category: Market category being analyzed.
        market_stats: Dict with avg, min, max, median, std, sample_size.
        feature_match_score: Keyword overlap score 0-100.
        top_competitors: List of dicts with competitor details.
        company_metadata: Company context from session (industry, employees, tools).

    Returns:
        Dict with competitiveness_score, strategic_recommendation, suggested_price,
        key_insights, confidence.

    Raises:
        RuntimeError: if GEMINI_API_KEY is not set.
        ValueError: if the LLM returns unparseable JSON.
    """
    llm = _get_llm()

    company_context = ""
    if company_metadata:
        industry = company_metadata.get("industry", "Unknown")
        employees = company_metadata.get("num_employees", "Unknown")
        tools = ", ".join(company_metadata.get("tools_used", [])) or "Not specified"
        company_context = (
            f"\nCompany Context:\n"
            f"  Industry: {industry}\n"
            f"  Employees: {employees}\n"
            f"  Tools currently used: {tools}\n"
        )

    competitors_text = "\n".join(
        f"  - {c['competitor_name']} | {c['product_name']} | "
        f"{currency} {c['price']} | Rating: {c.get('rating', 'N/A')} | "
        f"Features: {c['features']}"
        for c in top_competitors[:5]
    )

    human_text = f"""
Category: {category}
{company_context}
My Product: {product_name}
My Price: {currency} {user_price}
My Features: {", ".join(features)}

Market Data ({market_stats['sample_size']} comparable competitors):
  Average price:  {currency} {market_stats['avg_price']:.0f}
  Lowest price:   {currency} {market_stats['min_price']:.0f}
  Highest price:  {currency} {market_stats['max_price']:.0f}
  Median price:   {currency} {market_stats['median_price']:.0f}
  Std deviation:  {currency} {market_stats['price_std']:.0f}
  Feature match:  {feature_match_score:.0f}% keyword overlap with competitors

Top Comparable Competitors:
{competitors_text}

Based on this data, provide your strategic pricing analysis.
""".strip()

    prompt = ChatPromptTemplate.from_messages([
        ("system", _BENCHMARK_SYSTEM_PROMPT),
        ("human", "{input}"),
    ])
    chain = prompt | llm

    response = chain.invoke({"input": human_text})
    raw: str = response.content  # type: ignore[union-attr]

    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Gemini returned invalid JSON for benchmark: {exc}\n\nRaw:\n{raw}")

    return data


# ---------------------------------------------------------------------------
# Module 6 — Growth & Retention Benchmarking (LLM-powered)
# ---------------------------------------------------------------------------

_RETENTION_SYSTEM_PROMPT = """
You are a growth strategy analyst specialising in 2026 retention benchmarks for Indian tech startups.
You understand the SaaS, EdTech, FinTech, and E-commerce verticals deeply.

You will receive:
- Startup sub-type (sector)
- Current estimated monthly churn rate
- Current estimated NRR
- Win rate and repeat customer rate from sales data

Your job is to return a JSON object with EXACTLY these fields:

{{
  "industry_avg_churn_pct": <float, 2026 industry average monthly churn % for this sector>,
  "top_tier_churn_pct": <float, top-decile best-in-class monthly churn % for this sector>,
  "nrr_benchmark_pct": <float, 2026 industry median annual NRR % for this sector>,
  "projected_churn_pct": <float, realistic projected monthly churn after AI personalisation + automated follow-ups>,
  "projected_nrr_pct": <float, realistic projected NRR % post-automation (should approach or exceed benchmark)>,
  "growth_levers": ["<lever 1>", "<lever 2>", "<lever 3>"],
  "sector_risks": ["<risk 1>", "<risk 2>"],
  "competitor_benchmarks": [
    {{"company": "<name>", "sector": "<sector>", "churn_pct": <float>, "nrr_pct": <float or null>}},
    ...  // 3-5 entries
  ],
  "executive_summary": "<2-3 sentence paragraph comparing startup to 2026 benchmarks and projecting post-AI improvement>"
}}

2026 benchmark reference (use as baseline, adjust for company size/stage):
  SaaS (SMB):      avg churn 3.5%, top-tier 1.5%, NRR benchmark 108%
  SaaS (Mid-mkt):  avg churn 2.0%, top-tier 0.8%, NRR benchmark 115%
  EdTech:          avg churn 6.5%, top-tier 3.0%, NRR benchmark 98%
  FinTech:         avg churn 4.0%, top-tier 1.8%, NRR benchmark 105%
  E-commerce:      avg churn 8.0%, top-tier 4.0%, NRR benchmark 95%

Rules for projections:
  - projected_churn_pct should be BETWEEN current and top_tier (AI improves but rarely reaches top-tier in year 1)
  - projected_nrr_pct should improve by 5-15pp above current_nrr
  - Do NOT exceed top-tier benchmarks for projections

Do NOT include any text outside the JSON object.
""".strip()

_RETENTION_HUMAN_PROMPT = """
Startup Sub-Type: {sub_type}
Current Monthly Churn (estimated): {current_churn_pct:.1f}%
Current NRR (estimated): {current_nrr_pct:.0f}%
Win Rate: {win_rate_pct:.1f}%
Repeat Customer Rate (of Closed Won): {repeat_rate_pct:.1f}%
Total Inquiries Analysed: {total_inquiries}
""".strip()


def analyse_retention_benchmarks(
    sub_type: str,
    current_churn_pct: float,
    current_nrr_pct: float,
    win_rate_pct: float,
    repeat_rate_pct: float,
    total_inquiries: int,
) -> dict:
    """Send startup retention stats to Gemini and return 2026 benchmark analysis.

    Args:
        sub_type:          Startup vertical (SaaS, EdTech, FinTech, E-commerce).
        current_churn_pct: Estimated current monthly churn percentage.
        current_nrr_pct:   Estimated current annual NRR percentage.
        win_rate_pct:      Closed Won / total inquiries × 100.
        repeat_rate_pct:   Repeat customers / Closed Won × 100.
        total_inquiries:   Total rows in sales_inquiries.csv.

    Returns:
        Dict with industry_avg_churn_pct, top_tier_churn_pct, nrr_benchmark_pct,
        projected_churn_pct, projected_nrr_pct, growth_levers, sector_risks,
        competitor_benchmarks, executive_summary.

    Raises:
        RuntimeError: if GEMINI_API_KEY is not set.
        ValueError:   if the LLM returns unparseable JSON.
    """
    llm = _get_llm()
    prompt = ChatPromptTemplate.from_messages([
        ("system", _RETENTION_SYSTEM_PROMPT),
        ("human", "{input}"),
    ])
    chain = prompt | llm

    human_text = _RETENTION_HUMAN_PROMPT.format(
        sub_type=sub_type,
        current_churn_pct=current_churn_pct,
        current_nrr_pct=current_nrr_pct,
        win_rate_pct=win_rate_pct,
        repeat_rate_pct=repeat_rate_pct,
        total_inquiries=total_inquiries,
    )

    response = chain.invoke({"input": human_text})
    raw: str = response.content  # type: ignore[union-attr]

    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LLM returned invalid JSON for retention benchmarks: {exc}\n\nRaw:\n{raw}"
        )

    return data
