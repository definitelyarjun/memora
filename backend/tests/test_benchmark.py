"""Tests for Module 3 — POST /api/v1/analyze/benchmark."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.core.session_store import session_store

client = TestClient(app)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session() -> str:
    return session_store.create(
        raw_dataframe=None,
        workflow_text="test workflow",
        company_metadata={
            "industry": "Hospitality",
            "num_employees": 12,
            "tools_used": ["WhatsApp", "Excel"],
        },
        data_issues=[],
        workflow_analysis=None,
    )


def _post_benchmark(payload: dict):
    return client.post("/api/v1/analyze/benchmark", json=payload)


_MOCK_LLM_RESULT = {
    "competitiveness_score": 72,
    "strategic_recommendation": (
        "Your price is 8% below the market average for your category. "
        "Consider raising to ₹3200 while emphasising your included breakfast "
        "and parking to justify the premium."
    ),
    "suggested_price": 3200.0,
    "key_insights": [
        "You are priced below 55% of direct competitors.",
        "Your feature set matches 68% of competitor offerings.",
        "Adding a gym or pool mention could justify a 15% price increase.",
    ],
    "confidence": "High",
}


# ---------------------------------------------------------------------------
# Test: happy path — hotel category, LLM mocked
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_hotel_happy_path(mock_llm):
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "Sunrise Boutique Hotel",
        "price": 2800,
        "currency": "INR",
        "features": ["AC", "WiFi", "breakfast", "parking", "rooftop"],
        "category": "hotel",
    })

    assert resp.status_code == 200
    body = resp.json()

    assert body["session_id"] == sid
    assert body["product_name"] == "Sunrise Boutique Hotel"
    assert body["user_price"] == 2800
    assert body["category"] == "hotel"

    # Stats layer
    stats = body["market_stats"]
    assert stats["sample_size"] >= 10       # we have 12 hotel rows
    assert stats["avg_price"] > 0
    assert stats["min_price"] <= 2800 or stats["max_price"] >= 2800
    assert 0.0 <= body["price_percentile"] <= 100.0
    assert body["price_position"] in ("Below Market", "Competitive", "Premium", "Uncompetitive")
    assert 0.0 <= body["feature_match_score"] <= 100.0
    assert len(body["top_competitors"]) <= 5

    # LLM layer
    assert body["competitiveness_score"] == 72
    assert "below" in body["strategic_recommendation"].lower()
    assert body["suggested_price"] == 3200.0
    assert len(body["key_insights"]) == 3
    assert body["llm_confidence"] == "High"

    mock_llm.assert_called_once()


# ---------------------------------------------------------------------------
# Test: stats are always present even when LLM fails
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", side_effect=RuntimeError("No API key"))
def test_benchmark_llm_failure_stats_still_returned(mock_llm):
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "Budget Hotel",
        "price": 1500,
        "currency": "INR",
        "features": ["WiFi", "AC"],
        "category": "hotel",
    })

    assert resp.status_code == 200
    body = resp.json()

    # Stats still present
    assert body["market_stats"]["sample_size"] > 0
    assert body["price_position"] is not None
    assert body["price_percentile"] is not None

    # LLM fields are null
    assert body["competitiveness_score"] is None
    assert body["strategic_recommendation"] is None
    assert body["suggested_price"] is None
    assert body["llm_confidence"] is None

    # Warning appended
    assert any("skipped" in w.lower() or "failed" in w.lower() for w in body["warnings"])


# ---------------------------------------------------------------------------
# Test: session not found → 404
# ---------------------------------------------------------------------------

def test_benchmark_session_not_found():
    resp = _post_benchmark({
        "session_id": "nonexistent_abc123",
        "product_name": "Test Product",
        "price": 500,
        "currency": "INR",
        "features": ["feature A"],
        "category": "electronics",
    })
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Test: unknown category → warning added, still returns 200
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_unknown_category_with_warning(mock_llm):
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "Mystery Service",
        "price": 999,
        "currency": "INR",
        "features": ["feature A", "feature B"],
        "category": "underwater_basketweaving",
    })

    assert resp.status_code == 200
    body = resp.json()
    assert any("not recognised" in w.lower() or "fallback" in w.lower() for w in body["warnings"])


# ---------------------------------------------------------------------------
# Test: different category — restaurant
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_restaurant_category(mock_llm):
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "The Spice Garden",
        "price": 800,
        "currency": "INR",
        "features": ["North Indian", "AC", "alcohol", "live music"],
        "category": "restaurant",
    })

    assert resp.status_code == 200
    body = resp.json()
    assert body["category"] == "restaurant"
    assert body["market_stats"]["sample_size"] >= 10


# ---------------------------------------------------------------------------
# Test: result is stored back in session (Option B)
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_stored_in_session(mock_llm):
    sid = _make_session()
    _post_benchmark({
        "session_id": sid,
        "product_name": "My Hotel",
        "price": 3000,
        "currency": "INR",
        "features": ["AC", "WiFi"],
        "category": "hotel",
    })

    entry = session_store.get(sid)
    assert entry is not None
    assert entry.benchmark_report is not None
    assert entry.benchmark_report.session_id == sid  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Test: price percentile correctness
# ---------------------------------------------------------------------------

@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_price_percentile_cheapest(mock_llm):
    """Cheapest price in category → percentile near 0."""
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "Budget Option",
        "price": 1,   # cheapest possible
        "currency": "INR",
        "features": ["basic"],
        "category": "hotel",
    })

    assert resp.status_code == 200
    assert resp.json()["price_percentile"] == 0.0
    assert resp.json()["price_position"] == "Below Market"


@patch("app.services.benchmark.analyse_benchmark", return_value=_MOCK_LLM_RESULT)
def test_benchmark_price_percentile_most_expensive(mock_llm):
    """Most expensive price → percentile near 100, position Uncompetitive."""
    sid = _make_session()
    resp = _post_benchmark({
        "session_id": sid,
        "product_name": "Ultra Premium",
        "price": 999999,
        "currency": "INR",
        "features": ["luxury"],
        "category": "hotel",
    })

    assert resp.status_code == 200
    body = resp.json()
    assert body["price_percentile"] == 100.0
    assert body["price_position"] == "Uncompetitive"
