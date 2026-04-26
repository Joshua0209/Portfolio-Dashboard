"""Phase 6 tests for price_sources foreign + FX branches."""
from __future__ import annotations

import pytest

from app.price_sources import get_fx_rates, get_prices


def test_get_prices_usd_routes_to_yfinance(monkeypatch) -> None:
    calls: list[tuple] = []

    def fake_yf_prices(symbol, start, end):
        calls.append((symbol, start, end))
        return [
            {"date": "2026-04-01", "close": 150.0, "volume": 1_000_000},
            {"date": "2026-04-02", "close": 152.5, "volume": 1_200_000},
        ]

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake_yf_prices)
    rows = get_prices("SNDK", "USD", "2026-04-01", "2026-04-30")
    assert calls == [("SNDK", "2026-04-01", "2026-04-30")]
    assert all(r["currency"] == "USD" for r in rows)
    assert all(r["source"] == "yfinance" for r in rows)
    assert [r["date"] for r in rows] == ["2026-04-01", "2026-04-02"]


def test_get_prices_filters_window_for_foreign(monkeypatch) -> None:
    """yfinance can return rows outside [start, end] in edge cases — filter
    same as TW path."""
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_prices",
        lambda s, st, ed: [
            {"date": "2026-03-31", "close": 1.0, "volume": 1},
            {"date": "2026-04-01", "close": 2.0, "volume": 1},
            {"date": "2026-04-30", "close": 3.0, "volume": 1},
            {"date": "2026-05-01", "close": 4.0, "volume": 1},
        ],
    )
    rows = get_prices("SNDK", "USD", "2026-04-01", "2026-04-30")
    assert [r["date"] for r in rows] == ["2026-04-01", "2026-04-30"]


def test_get_prices_hkd_jpy_also_route_to_yfinance(monkeypatch) -> None:
    """Phase 6 wires USD as the primary case; HKD/JPY follow the same path
    (the parser caveat in CLAUDE.md notes only USD is parsed today)."""
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_prices",
        lambda s, st, ed: [{"date": "2026-04-01", "close": 100.0, "volume": 1}],
    )
    rows = get_prices("0700.HK", "HKD", "2026-04-01", "2026-04-30")
    assert rows[0]["currency"] == "HKD"


# --- FX rates ------------------------------------------------------------


def test_get_fx_rates_returns_tagged_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_fx",
        lambda ccy, st, ed: [
            {"date": "2026-04-01", "rate": 32.50},
            {"date": "2026-04-02", "rate": 32.55},
        ],
    )
    rows = get_fx_rates("USD", "2026-04-01", "2026-04-30")
    assert len(rows) == 2
    assert all(r["ccy"] == "USD" for r in rows)
    assert all(r["source"] == "yfinance" for r in rows)
    assert rows[0]["rate"] == 32.50


def test_get_fx_rates_filters_window(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_fx",
        lambda ccy, st, ed: [
            {"date": "2026-03-31", "rate": 32.40},
            {"date": "2026-04-01", "rate": 32.50},
            {"date": "2026-05-01", "rate": 32.80},
        ],
    )
    rows = get_fx_rates("USD", "2026-04-01", "2026-04-30")
    assert [r["date"] for r in rows] == ["2026-04-01"]


def test_get_fx_rates_dedupes(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_fx",
        lambda ccy, st, ed: [
            {"date": "2026-04-01", "rate": 32.50},
            {"date": "2026-04-01", "rate": 32.50},
        ],
    )
    rows = get_fx_rates("USD", "2026-04-01", "2026-04-30")
    assert len(rows) == 1


# --- Backwards compat for phase 2/5 -------------------------------------


def test_phase2_call_for_unknown_currency_now_works(monkeypatch) -> None:
    """USD used to raise NotImplementedError in phase 5. Phase 6 wires it."""
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_prices",
        lambda s, st, ed: [{"date": "2026-04-01", "close": 100.0, "volume": 1}],
    )
    # Should NOT raise
    rows = get_prices("SNDK", "USD", "2026-04-01", "2026-04-30")
    assert len(rows) == 1
