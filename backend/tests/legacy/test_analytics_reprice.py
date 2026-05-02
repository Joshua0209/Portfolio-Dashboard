"""Unit tests for analytics.reprice_holdings_with_daily.

Phase 7's repricer is the unconditional override that makes /api/tax and
/ KPI hero reflect today's close instead of last month-end. End-to-end
coverage exists via tests/test_api_daily_resolution.py, but several
branches were untested in isolation:

  - Per-ticker fallback when the daily store has no row for a symbol
    (delisted, thin volume) — must keep month-end values, not crash.
  - Foreign holdings with non-USD ccy — current implementation falls
    back to rate=1.0 (documented limitation in CLAUDE.md). Pin that
    behavior so a refactor can't silently change it.
  - Zero or missing close — must not divide by zero on unrealized_pct.
  - Empty list / missing code — pass-through identity.
"""
from __future__ import annotations

import pytest

from invest.analytics import monthly as analytics


@pytest.fixture()
def tw_holding() -> dict:
    return {
        "code": "2330", "name": "TSMC", "venue": "TW",
        "qty": 1000.0, "avg_cost": 800.0,
        "cost_local": 800_000.0, "cost_twd": 800_000.0,
        "ref_price": 850.0,
        "mkt_value_local": 850_000.0, "mkt_value_twd": 850_000.0,
        "unrealized_pnl_local": 50_000.0, "unrealized_pnl_twd": 50_000.0,
    }


@pytest.fixture()
def usd_holding() -> dict:
    return {
        "code": "AAPL", "name": "Apple", "venue": "US", "ccy": "USD",
        "qty": 10.0, "avg_cost": 150.0,
        "cost_local": 1500.0, "cost_twd": 45_000.0,
        "ref_price": 160.0,
        "mkt_value_local": 1600.0, "mkt_value_twd": 48_000.0,
        "unrealized_pnl_local": 100.0, "unrealized_pnl_twd": 3_000.0,
    }


def test_tw_holding_repriced_against_today_close(tw_holding):
    closes = {"2330": {"date": "2026-04-25", "close": 1100.0, "currency": "TWD"}}
    out = analytics.reprice_holdings_with_daily([tw_holding], closes.get)

    assert len(out) == 1
    h = out[0]
    assert h["ref_price"] == 1100.0
    assert h["mkt_value_local"] == 1_100_000.0
    assert h["mkt_value_twd"] == 1_100_000.0  # TW is local=twd
    assert h["unrealized_pnl_local"] == 300_000.0  # 1.1M - 800k
    assert h["unrealized_pnl_twd"] == 300_000.0
    assert h["unrealized_pct"] == pytest.approx(0.375, rel=1e-3)
    assert h["repriced_at"] == "2026-04-25"


def test_missing_daily_close_keeps_month_end_values(tw_holding):
    """Per-ticker fallback: a holding without a daily price (delisted,
    thin volume) keeps its month-end ref_price/mv/unrealized verbatim."""
    out = analytics.reprice_holdings_with_daily([tw_holding], lambda c: None)

    assert out[0]["ref_price"] == 850.0  # month-end preserved
    assert out[0]["mkt_value_twd"] == 850_000.0
    assert "repriced_at" not in out[0]


def test_usd_holding_uses_current_fx(usd_holding):
    closes = {"AAPL": {"date": "2026-04-25", "close": 200.0, "currency": "USD"}}
    out = analytics.reprice_holdings_with_daily(
        [usd_holding], closes.get, current_fx_usd_twd=31.5
    )

    h = out[0]
    assert h["mkt_value_local"] == 2000.0
    assert h["mkt_value_twd"] == pytest.approx(63_000.0)  # 2000 * 31.5
    assert h["unrealized_pnl_local"] == 500.0  # 2000 - 1500
    assert h["unrealized_pnl_twd"] == pytest.approx(15_750.0)  # 500 * 31.5


def test_non_usd_foreign_falls_back_to_rate_1(usd_holding):
    """Documented CLAUDE.md limitation: only USD positions get FX-converted.
    HKD/JPY positions silently use rate=1.0. Pin the behavior so a future
    refactor can't change it without updating the contract."""
    hkd = {**usd_holding, "code": "0700", "ccy": "HKD"}
    closes = {"0700": {"date": "2026-04-25", "close": 200.0, "currency": "HKD"}}
    out = analytics.reprice_holdings_with_daily(
        [hkd], closes.get, current_fx_usd_twd=31.5
    )

    h = out[0]
    assert h["mkt_value_local"] == 2000.0
    # rate=1.0 fallback because ccy != "USD"
    assert h["mkt_value_twd"] == 2000.0
    assert h["unrealized_pnl_twd"] == 500.0


def test_zero_cost_does_not_divide_by_zero(tw_holding):
    """A holding with cost_local=0 (rare edge: free shares from a stock
    grant) must produce unrealized_pct=0 instead of crashing."""
    h = {**tw_holding, "cost_local": 0.0, "cost_twd": 0.0}
    closes = {"2330": {"date": "2026-04-25", "close": 1100.0, "currency": "TWD"}}
    out = analytics.reprice_holdings_with_daily([h], closes.get)

    assert out[0]["unrealized_pct"] == 0


def test_empty_holdings_passthrough():
    out = analytics.reprice_holdings_with_daily([], lambda c: None)
    assert out == []


def test_holding_without_code_passthrough(tw_holding):
    """Holdings missing a code can't be repriced — pass through unchanged
    rather than crash on the closes lookup."""
    h = {**tw_holding, "code": None}
    out = analytics.reprice_holdings_with_daily([h], lambda c: None)

    assert out[0]["ref_price"] == 850.0  # untouched
    assert "repriced_at" not in out[0]


def test_function_is_side_effect_free(tw_holding):
    """Returns a new list of new dicts — input is not mutated."""
    original = dict(tw_holding)
    closes = {"2330": {"date": "2026-04-25", "close": 1100.0, "currency": "TWD"}}
    out = analytics.reprice_holdings_with_daily([tw_holding], closes.get)

    assert tw_holding == original  # input untouched
    assert out[0] is not tw_holding  # new dict instance
