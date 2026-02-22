"""LLM service — LangChain + Google Gemini.

Takes a free-text workflow description and returns:
  - A list of structured WorkflowStep objects
  - A Mermaid flowchart diagram
  - A one-paragraph executive summary

Also handles multimodal invoice PDF extraction via Gemini Vision.

Uses langchain-google-genai with structured output via JSON mode.
"""

from __future__ import annotations

import base64
import json
import re

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.messages import HumanMessage

from app.core.config import settings
from app.schemas.ingestion import WorkflowDiagram, WorkflowStep
from app.schemas.document_ingestion import InvoiceData, LineItem


# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """
You are a business process analyst. You will receive a free-text description of a company's operational workflow.

Your job is to:
1. Extract every distinct step in the workflow, in order.
2. For each step identify: who performs it (actor/role), whether it is Manual, Automated, a Decision point, or Unknown, and any tool mentioned.
3. Produce a Mermaid flowchart diagram that visually represents the workflow with actors shown in parentheses.
4. Write a concise one-paragraph executive summary of what the workflow does and where the main inefficiencies appear to be.

CRITICAL MERMAID FORMATTING RULES — YOU MUST FOLLOW THESE EXACTLY:
- Every Mermaid statement MUST be on its own line separated by \n
- Use \n between EVERY node, arrow, and subgraph statement
- NEVER put multiple statements on a single line
- subgraph and end keywords must each be on their own line
- Correct example:
  "flowchart TD\n    A[Start] --> B[Step 1]\n    B --> C{{Decision?}}\n    C -- Yes --> D[Do it]\n    C -- No --> E[Skip]\n    D --> F[End]\n    E --> F"

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
  "mermaid_diagram": "flowchart TD\n    A[First step] --> B[Second step]\n    B --> C[Third step]",
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
# Public function
# ---------------------------------------------------------------------------

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
# Invoice PDF extraction (multimodal)
# ---------------------------------------------------------------------------

_INVOICE_EXTRACTION_PROMPT = """
You are a financial document extraction specialist. I am sending you one or more pages of a business invoice or ledger PDF rendered as images.

Extract the following fields and return them as a single JSON object. Use null for any field you cannot find.

Required fields:
{
  "invoice_number":      string or null,
  "invoice_date":        string or null  (preserve original format, e.g. "15/01/2025"),
  "seller_name":         string or null,
  "seller_gstin":        string or null,
  "buyer_name":          string or null,
  "buyer_gstin":         string or null,
  "line_items": [
    {
      "description": string,
      "quantity":    number or null,
      "unit":        string or null,
      "rate":        number or null,
      "amount":      number or null
    }
  ],
  "subtotal":            number or null,
  "tax_amount":          number or null,
  "total_amount":        number or null,
  "currency":            string  (default "INR"),
  "raw_extraction_notes": string or null  (note any uncertainties)
}

Do NOT include any text outside the JSON object.
""".strip()


def analyse_invoice_pdf(page_images: list[bytes]) -> InvoiceData:
    """Send PDF pages (as PNG bytes) to Gemini Vision and extract invoice data.

    Args:
        page_images: List of PNG image bytes — one per page, from pymupdf.

    Returns:
        InvoiceData with all fields populated where found.

    Raises:
        RuntimeError: if GEMINI_API_KEY is not set.
        ValueError: if the LLM returns unparseable JSON.
    """
    llm = _get_llm()

    # Build multimodal message: text prompt + one image block per page
    content: list[dict] = [{"type": "text", "text": _INVOICE_EXTRACTION_PROMPT}]
    for img_bytes in page_images:
        b64 = base64.b64encode(img_bytes).decode("utf-8")
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        })

    response = llm.invoke([HumanMessage(content=content)])
    raw: str = response.content  # type: ignore[union-attr]

    # Strip markdown fences
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Gemini returned invalid JSON: {exc}\n\nRaw response:\n{raw}")

    line_items = [LineItem(**item) for item in data.get("line_items", [])]

    return InvoiceData(
        invoice_number=data.get("invoice_number"),
        invoice_date=data.get("invoice_date"),
        seller_name=data.get("seller_name"),
        seller_gstin=data.get("seller_gstin"),
        buyer_name=data.get("buyer_name"),
        buyer_gstin=data.get("buyer_gstin"),
        line_items=line_items,
        subtotal=data.get("subtotal"),
        tax_amount=data.get("tax_amount"),
        total_amount=data.get("total_amount"),
        currency=data.get("currency", "INR"),
        raw_extraction_notes=data.get("raw_extraction_notes"),
    )


# ---------------------------------------------------------------------------
# Market benchmarking (text-based structured analysis)
# ---------------------------------------------------------------------------

_BENCHMARK_SYSTEM_PROMPT = """
You are a senior pricing strategy consultant specialising in SME markets in India.
You will receive structured market data about a business's product and its competitive landscape.

Return a single JSON object with exactly these fields:
{
  "competitiveness_score": <integer 0-100>,
  "strategic_recommendation": "<2-3 sentence actionable pricing and positioning advice>",
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
