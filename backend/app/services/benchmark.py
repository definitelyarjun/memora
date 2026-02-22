"""Module 3 — Industry Benchmarking & Competitiveness Analyzer.

Pipeline
--------
1. Load the bundled static market dataset (CSV shipped with the app)
2. Filter rows by category (case-insensitive keyword match)
3. Compute descriptive stats with Pandas: avg, min, max, median, std
4. Compute the user's price percentile rank
5. Score feature overlap via Jaccard keyword similarity (no extra dependencies)
6. Select top 5 closest competitors by price proximity
7. Pass everything to Gemini for strategic analysis
8. Return BenchmarkReport (stats always present; LLM fields None on failure)

Dataset location: app/data/market_data.csv
Categories: hotel, restaurant, electronics, apparel, saas, consulting
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from app.core.session_store import SessionEntry
from app.schemas.benchmark import (
    BenchmarkReport,
    BenchmarkRequest,
    CompetitorSnapshot,
    MarketPosition,
    MarketStats,
)
from app.services.llm import analyse_benchmark

_DATA_PATH = Path(__file__).parent.parent / "data" / "market_data.csv"

# Minimum rows needed before we attempt LLM analysis (still run stats with fewer)
_MIN_SAMPLE_FOR_LLM = 3


# ---------------------------------------------------------------------------
# Dataset loader (cached at module level — loaded once on first call)
# ---------------------------------------------------------------------------

_market_df: pd.DataFrame | None = None


def _load_market_data() -> pd.DataFrame:
    global _market_df
    if _market_df is None:
        _market_df = pd.read_csv(_DATA_PATH)
        _market_df["price"] = pd.to_numeric(_market_df["price"], errors="coerce")
        _market_df["rating"] = pd.to_numeric(_market_df["rating"], errors="coerce")
        _market_df = _market_df.dropna(subset=["price"])
    return _market_df


# ---------------------------------------------------------------------------
# Category matching
# ---------------------------------------------------------------------------

_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "hotel":       ["hotel", "accommodation", "resort", "hostel", "stay", "room"],
    "restaurant":  ["restaurant", "food", "cafe", "dining", "meal", "eatery", "canteen"],
    "electronics": ["electronics", "gadget", "earphone", "headphone", "device", "tech", "audio"],
    "apparel":     ["apparel", "clothing", "fashion", "garment", "wear", "shirt", "dress", "kurta"],
    "saas":        ["saas", "software", "crm", "erp", "hrms", "accounting", "app", "platform", "tool"],
    "consulting":  ["consulting", "consultancy", "advisory", "agency", "freelance", "services"],
}


def _resolve_category(user_input: str) -> str | None:
    """Map a free-text category input to a dataset category key."""
    lower = user_input.lower()
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return cat
    return None


# ---------------------------------------------------------------------------
# Feature similarity — Jaccard keyword overlap
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> set[str]:
    """Lowercase, split on spaces and common punctuation, return word set."""
    import re
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _jaccard(set_a: set[str], set_b: set[str]) -> float:
    if not set_a and not set_b:
        return 1.0
    union = set_a | set_b
    if not union:
        return 0.0
    return len(set_a & set_b) / len(union)


def _feature_match_score(user_features: list[str], competitors_df: pd.DataFrame) -> float:
    """Average Jaccard similarity between user features and each competitor's features."""
    if competitors_df.empty:
        return 0.0

    user_tokens = _tokenize(" ".join(user_features))
    scores = []
    for _, row in competitors_df.iterrows():
        comp_tokens = _tokenize(str(row.get("features", "")))
        scores.append(_jaccard(user_tokens, comp_tokens))

    return round(sum(scores) / len(scores) * 100, 1) if scores else 0.0


# ---------------------------------------------------------------------------
# Price metrics
# ---------------------------------------------------------------------------

def _price_percentile(user_price: float, prices: pd.Series) -> float:
    """Fraction of competitors cheaper than user (0–100)."""
    if prices.empty:
        return 50.0
    below = (prices < user_price).sum()
    return round(below / len(prices) * 100, 1)


def _market_position(percentile: float) -> MarketPosition:
    if percentile < 25:
        return "Below Market"
    if percentile <= 75:
        return "Competitive"
    if percentile <= 90:
        return "Premium"
    return "Uncompetitive"


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_benchmark(
    request: BenchmarkRequest,
    entry: SessionEntry,
    session_id: str,
) -> BenchmarkReport:
    """Execute the full benchmarking pipeline and return a BenchmarkReport.

    Stats layer always runs.
    LLM strategy layer runs if GEMINI_API_KEY is set and sample ≥ MIN_SAMPLE.
    LLM failures are non-fatal — report is returned with LLM fields as None.
    """
    warnings: list[str] = []

    # --- Resolve category -------------------------------------------------
    resolved_category = _resolve_category(request.category)
    if resolved_category is None:
        resolved_category = request.category.lower()
        warnings.append(
            f"Category '{request.category}' not recognised. "
            f"Supported: {', '.join(_CATEGORY_KEYWORDS)}. "
            "Analysis may be less accurate."
        )

    # --- Filter market data -----------------------------------------------
    df = _load_market_data()
    filtered = df[df["category"] == resolved_category].copy()

    if filtered.empty:
        # Fall back to full dataset with a warning
        filtered = df.copy()
        warnings.append(
            f"No data found for category '{resolved_category}'. "
            "Using full market dataset as fallback — results are less representative."
        )

    prices = filtered["price"]

    # --- Stats layer -------------------------------------------------------
    market_stats = MarketStats(
        sample_size=len(filtered),
        avg_price=round(prices.mean(), 2),
        min_price=round(prices.min(), 2),
        max_price=round(prices.max(), 2),
        median_price=round(prices.median(), 2),
        price_std=round(prices.std(), 2),
    )

    percentile = _price_percentile(request.price, prices)
    position = _market_position(percentile)
    gap_pct = round((request.price - market_stats.avg_price) / market_stats.avg_price * 100, 1)

    # --- Feature similarity -----------------------------------------------
    feat_score = _feature_match_score(request.features, filtered)

    # --- Top 5 competitors closest to user's price -----------------------
    filtered["price_diff"] = (filtered["price"] - request.price).abs()
    top5 = filtered.nsmallest(5, "price_diff")
    top_competitors = [
        CompetitorSnapshot(
            competitor_name=str(row["competitor_name"]),
            product_name=str(row["product_name"]),
            price=float(row["price"]),
            features=str(row["features"]),
            rating=float(row["rating"]) if not math.isnan(row["rating"]) else None,
        )
        for _, row in top5.iterrows()
    ]

    # --- LLM strategy layer -----------------------------------------------
    competitiveness_score = None
    strategic_recommendation = None
    suggested_price = None
    key_insights: list[str] = []
    llm_confidence = None

    if len(filtered) < _MIN_SAMPLE_FOR_LLM:
        warnings.append(
            f"Only {len(filtered)} comparable competitor(s) found. "
            "LLM analysis skipped — too few data points for reliable strategy."
        )
    else:
        try:
            llm_result = analyse_benchmark(
                product_name=request.product_name,
                user_price=request.price,
                currency=request.currency,
                features=request.features,
                category=resolved_category,
                market_stats=market_stats.model_dump(),
                feature_match_score=feat_score,
                top_competitors=[c.model_dump() for c in top_competitors],
                company_metadata=entry.company_metadata if entry else None,
            )
            competitiveness_score = int(llm_result.get("competitiveness_score", 50))
            strategic_recommendation = llm_result.get("strategic_recommendation")
            suggested_price = llm_result.get("suggested_price")
            key_insights = llm_result.get("key_insights", [])
            llm_confidence = llm_result.get("confidence")
        except RuntimeError as exc:
            warnings.append(f"LLM analysis skipped — {exc}")
        except Exception as exc:
            warnings.append(f"LLM analysis failed: {exc}")

    return BenchmarkReport(
        session_id=session_id,
        product_name=request.product_name,
        user_price=request.price,
        currency=request.currency,
        category=resolved_category,
        market_stats=market_stats,
        price_percentile=percentile,
        price_position=position,
        price_gap_pct=gap_pct,
        feature_match_score=feat_score,
        top_competitors=top_competitors,
        competitiveness_score=competitiveness_score,
        strategic_recommendation=strategic_recommendation,
        suggested_price=suggested_price,
        key_insights=key_insights,
        llm_confidence=llm_confidence,
        warnings=warnings,
    )
