"""Phase 2 acceptance tests for app/price_sources.py (TWSE-only branch).

This phase only wires the TW route. TPEX comes in Phase 5 and yfinance in
Phase 6, so calls for non-TWD currencies should raise NotImplementedError
to fail loudly until those phases land.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.price_sources import get_prices, months_in_range


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --- date math ----------------------------------------------------------


def test_months_in_range_single_month() -> None:
    assert months_in_range("2026-04-01", "2026-04-30") == [(2026, 4)]


def test_months_in_range_spans_year_boundary() -> None:
    assert months_in_range("2025-11-15", "2026-02-10") == [
        (2025, 11),
        (2025, 12),
        (2026, 1),
        (2026, 2),
    ]


def test_months_in_range_clips_to_first_of_month_logic() -> None:
    """Even if the start is mid-month, we still emit that month."""
    assert months_in_range("2026-04-26", "2026-04-26") == [(2026, 4)]


# --- TWSE dispatch ------------------------------------------------------


def test_get_prices_tw_routes_to_twse(monkeypatch) -> None:
    """For TWD currency, get_prices walks calendar months and calls fetch_month."""
    calls: list[tuple] = []

    def fake_fetch_month(stockNo, year, month):
        calls.append((stockNo, year, month))
        # mock 2 rows per month
        return [
            {"date": f"{year:04d}-{month:02d}-01", "close": 100.0, "volume": 1},
            {"date": f"{year:04d}-{month:02d}-15", "close": 105.0, "volume": 2},
        ]

    monkeypatch.setattr("app.price_sources.twse_fetch_month", fake_fetch_month)

    rows = get_prices("2330", "TWD", "2026-03-15", "2026-04-20")
    assert calls == [("2330", 2026, 3), ("2330", 2026, 4)]
    # 2026-03-01 is dropped (before window start); 2026-03-15, -04-01, -04-15 kept.
    assert len(rows) == 3
    assert [r["date"] for r in rows] == ["2026-03-15", "2026-04-01", "2026-04-15"]
    # All rows tagged with the correct symbol/source
    assert all(r["symbol"] == "2330" for r in rows)
    assert all(r["currency"] == "TWD" for r in rows)
    assert all(r["source"] == "twse" for r in rows)


def test_get_prices_filters_to_window(monkeypatch) -> None:
    """Rows outside [start, end] are dropped even if TWSE returned them."""
    def fake_fetch_month(stockNo, year, month):
        return [
            {"date": "2026-04-01", "close": 100.0, "volume": 1},
            {"date": "2026-04-15", "close": 105.0, "volume": 2},
            {"date": "2026-04-30", "close": 110.0, "volume": 3},
        ]

    monkeypatch.setattr("app.price_sources.twse_fetch_month", fake_fetch_month)

    rows = get_prices("2330", "TWD", "2026-04-10", "2026-04-20")
    assert [r["date"] for r in rows] == ["2026-04-15"]


def test_get_prices_dedupes_when_overlapping_months(monkeypatch) -> None:
    """Asking the same month twice (shouldn't happen, but) returns one row."""
    def fake_fetch_month(stockNo, year, month):
        return [{"date": "2026-04-01", "close": 100.0, "volume": 1}]

    monkeypatch.setattr("app.price_sources.twse_fetch_month", fake_fetch_month)
    rows = get_prices("2330", "TWD", "2026-04-01", "2026-04-01")
    assert len(rows) == 1


def test_get_prices_returns_empty_when_twse_returns_nothing(monkeypatch) -> None:
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    assert get_prices("9999", "TWD", "2026-04-01", "2026-04-30") == []


def test_get_prices_usd_routed_to_yfinance(monkeypatch) -> None:
    """Phase 6 wires the USD branch; this test pins the historical
    NotImplementedError contract is gone and USD now flows through."""
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_prices",
        lambda s, st, ed: [{"date": "2026-04-01", "close": 150.0, "volume": 1}],
    )
    rows = get_prices("SNDK", "USD", "2026-04-01", "2026-04-30")
    assert rows and rows[0]["currency"] == "USD"
