"""Acceptance tests for app/price_sources.py.

After the TWSE/TPEX-removal refactor, all price fetching goes through
yfinance — TW symbols are queried with `.TW` (listed) or `.TWO` (OTC)
suffixes; foreign symbols use bare tickers. The `symbol_market` cache
still distinguishes 'twse' / 'tpex' / 'unknown' as market verdicts.
"""
from __future__ import annotations

from app.price_sources import get_prices, months_in_range


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


# --- TW dispatch (yfinance with .TW / .TWO suffix) ----------------------


def test_get_prices_tw_routes_to_yfinance_dot_tw(monkeypatch) -> None:
    """For TWD currency with no cache hit, the router probes yfinance with
    the `.TW` suffix first and tags rows with the bare symbol."""
    calls: list[tuple] = []

    def fake_yf(symbol, start, end):
        calls.append((symbol, start, end))
        return [
            {"date": "2026-04-01", "close": 1855.0, "volume": 46_000_000},
            {"date": "2026-04-02", "close": 1860.0, "volume": 45_000_000},
        ]

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake_yf)

    rows = get_prices("2330", "TWD", "2026-04-01", "2026-04-02")
    # Single yfinance call with the .TW suffix — no month batching.
    assert calls == [("2330.TW", "2026-04-01", "2026-04-02")]
    assert len(rows) == 2
    # Tagged rows carry the bare symbol (the `prices` table key) and
    # source='yfinance' regardless of which suffix succeeded.
    assert all(r["symbol"] == "2330" for r in rows)
    assert all(r["currency"] == "TWD" for r in rows)
    assert all(r["source"] == "yfinance" for r in rows)


def test_get_prices_tw_filters_to_window(monkeypatch) -> None:
    """Rows outside [start, end] are dropped even if yfinance returned them."""
    def fake_yf(symbol, start, end):
        return [
            {"date": "2026-04-01", "close": 100.0, "volume": 1},
            {"date": "2026-04-15", "close": 105.0, "volume": 2},
            {"date": "2026-04-30", "close": 110.0, "volume": 3},
        ]

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake_yf)

    rows = get_prices("2330", "TWD", "2026-04-10", "2026-04-20")
    assert [r["date"] for r in rows] == ["2026-04-15"]


def test_get_prices_tw_returns_empty_when_neither_suffix_responds(
    monkeypatch,
) -> None:
    """`.TW` empty AND `.TWO` empty → empty result, no exceptions."""
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", lambda *_: [])
    assert get_prices("9999", "TWD", "2026-04-01", "2026-04-30") == []


def test_get_prices_tw_falls_back_to_dot_two_for_otc(monkeypatch) -> None:
    """If `.TW` returns nothing, the router probes `.TWO` (TPEX/OTC)."""
    queried: list[str] = []

    def fake_yf(symbol, start, end):
        queried.append(symbol)
        if symbol.endswith(".TWO"):
            return [{"date": "2026-04-01", "close": 80.0, "volume": 1}]
        return []  # .TW returns empty

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake_yf)

    rows = get_prices("5483", "TWD", "2026-04-01", "2026-04-30")
    assert queried == ["5483.TW", "5483.TWO"]
    assert len(rows) == 1
    assert rows[0]["source"] == "yfinance"
    assert rows[0]["symbol"] == "5483"


def test_get_prices_usd_routed_to_yfinance(monkeypatch) -> None:
    """Foreign branch passes bare ticker to yfinance — no suffix logic."""
    monkeypatch.setattr(
        "app.price_sources.yfinance_fetch_prices",
        lambda s, st, ed: [{"date": "2026-04-01", "close": 150.0, "volume": 1}],
    )
    rows = get_prices("SNDK", "USD", "2026-04-01", "2026-04-30")
    assert rows and rows[0]["currency"] == "USD"
    assert rows[0]["symbol"] == "SNDK"
    assert rows[0]["source"] == "yfinance"
