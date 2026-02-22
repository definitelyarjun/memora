"""Pydantic models for Module 3 — Industry Benchmarking & Competitiveness Analyzer."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


MarketPosition = Literal["Below Market", "Competitive", "Premium", "Uncompetitive"]
LLMConfidence = Literal["High", "Medium", "Low"]


class BenchmarkRequest(BaseModel):
    """Input supplied by the client for a benchmarking run."""

    session_id: str = Field(..., description="session_id from Module 1 /ingest/tabular")
    product_name: str = Field(..., description="Name of your product or service")
    price: float = Field(..., gt=0, description="Your current selling price")
    currency: str = Field("INR", description="Currency code, e.g. INR, USD")
    features: list[str] = Field(
        ...,
        min_length=1,
        max_length=10,
        description="3–5 key features or selling points",
    )
    category: str = Field(
        ...,
        description=(
            "Product/service category. Supported: "
            "hotel, restaurant, electronics, apparel, saas, consulting"
        ),
    )
    region: str | None = Field(None, description="Target market region, e.g. 'Mumbai', 'India'")


class CompetitorSnapshot(BaseModel):
    """A single competitor entry from the market dataset."""

    competitor_name: str
    product_name: str
    price: float
    features: str
    rating: float | None = None


class MarketStats(BaseModel):
    """Descriptive statistics computed from the filtered market dataset."""

    sample_size: int = Field(..., description="Number of comparable competitors found")
    avg_price: float
    min_price: float
    max_price: float
    median_price: float
    price_std: float


class BenchmarkReport(BaseModel):
    """Full competitiveness report — stats layer + LLM strategy layer."""

    session_id: str
    product_name: str
    user_price: float
    currency: str
    category: str

    # --- Stats layer (always present) ------------------------------------
    market_stats: MarketStats
    price_percentile: float = Field(
        ...,
        description="How many competitors you are cheaper than (0–100)",
        ge=0.0,
        le=100.0,
    )
    price_position: MarketPosition = Field(
        ...,
        description=(
            "Below Market <25th pct · Competitive 25–75th · "
            "Premium 75–90th · Uncompetitive >90th"
        ),
    )
    price_gap_pct: float = Field(
        ...,
        description="% difference vs market average (negative = below average)",
    )
    feature_match_score: float = Field(
        ...,
        description="Keyword overlap with top competitors' features (0–100)",
        ge=0.0,
        le=100.0,
    )
    top_competitors: list[CompetitorSnapshot]

    # --- LLM strategy layer (None if Gemini call failed) -----------------
    competitiveness_score: int | None = Field(
        None,
        description="0–100 overall competitiveness score from Gemini",
        ge=0,
        le=100,
    )
    strategic_recommendation: str | None = Field(
        None,
        description="Gemini's pricing and positioning strategy advice",
    )
    suggested_price: float | None = Field(
        None,
        description="Gemini's recommended optimal price point",
    )
    key_insights: list[str] = Field(
        default_factory=list,
        description="3–5 key takeaways from Gemini's analysis",
    )
    llm_confidence: LLMConfidence | None = None

    warnings: list[str] = Field(default_factory=list)
