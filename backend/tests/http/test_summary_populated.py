"""Populated-state tests for /api/summary — Phase 6.5 wiring.

Exercises the non-empty branches of the new summary router using
synthetic portfolio + daily fixtures. Empty-state contract is
covered separately in test_data_routers.py:TestSummary.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from invest.app import create_app

from .conftest import _FakeDaily, _FakePortfolio, install_store_overrides


_PORTFOLIO_RAW = {
    "months": [
        {
            "month": "2024-01",
            "equity_twd": 1_000_000.0,
            "tw_market_value_twd": 600_000.0,
            "foreign_market_value_twd": 300_000.0,
            "bank_twd": 100_000.0,
            "bank_usd_in_twd": 0.0,
            "external_flow_twd": 1_000_000.0,
            "fx_usd_twd": 31.5,
            "tw": {"holdings": [], "trades": [], "rebates": []},
            "foreign": {"holdings": [], "trades": []},
            "bank": {"flows": []},
        },
        {
            "month": "2024-02",
            "equity_twd": 1_050_000.0,
            "tw_market_value_twd": 630_000.0,
            "foreign_market_value_twd": 315_000.0,
            "bank_twd": 105_000.0,
            "bank_usd_in_twd": 0.0,
            "external_flow_twd": 0.0,
            "fx_usd_twd": 31.6,
            "xirr": 0.6,
            "tw": {"holdings": [], "trades": [], "rebates": []},
            "foreign": {"holdings": [], "trades": []},
            "bank": {"flows": []},
        },
    ],
    "summary": {
        "kpis": {
            "as_of": "2024-02-29",
            "real_now_twd": 1_050_000.0,
            "profit_twd": 50_000.0,
            "counterfactual_twd": 1_000_000.0,
        },
        "by_ticker": {},
        "all_trades": [],
        "dividends": [],
        "venue_flows_twd": [],
        "holdings_total_return": [],
        "cumulative_flows": {},
    },
}


@pytest.fixture
def populated_client():
    app = create_app()
    portfolio = _FakePortfolio(raw=_PORTFOLIO_RAW)
    daily = _FakeDaily()
    install_store_overrides(app, portfolio=portfolio, daily=daily)
    return TestClient(app), portfolio, daily


class TestPopulated:
    def test_returns_full_envelope(self, populated_client):
        client, _, _ = populated_client
        r = client.get("/api/summary")
        assert r.status_code == 200
        d = r.json()["data"]
        assert d.get("empty") is not True  # not the empty branch
        assert d["months_covered"] == 2
        assert d["first_month"] == "2024-01"
        assert d["last_month"] == "2024-02"
        assert len(d["equity_curve"]) == 2

    def test_kpis_passed_through_from_summary(self, populated_client):
        client, _, _ = populated_client
        d = client.get("/api/summary").json()["data"]
        assert d["kpis"]["as_of"] == "2024-02-29"

    def test_xirr_from_last_month(self, populated_client):
        client, _, _ = populated_client
        d = client.get("/api/summary").json()["data"]
        # last month has xirr=0.6, first doesn't — last wins.
        assert d["xirr"] == 0.6

    def test_allocation_from_last_month(self, populated_client):
        client, _, _ = populated_client
        d = client.get("/api/summary").json()["data"]
        assert d["allocation"]["tw"] == 630_000.0
        assert d["allocation"]["foreign"] == 315_000.0
        assert d["allocation"]["bank_twd"] == 105_000.0

    def test_equity_curve_has_cum_twr(self, populated_client):
        client, _, _ = populated_client
        d = client.get("/api/summary").json()["data"]
        # First month is forced to 0 by period_returns. Second computes
        # the real return. cum_twr accumulates.
        assert d["equity_curve"][0]["cum_twr"] == 0.0
        assert d["equity_curve"][1]["cum_twr"] != 0.0


class TestDailyResolution:
    def test_daily_with_empty_store_falls_back_to_monthly(self, populated_client):
        client, _, _ = populated_client
        d = client.get("/api/summary?resolution=daily").json()["data"]
        # Daily store has no equity curve → resolution downgrades.
        assert d["resolution"] == "monthly"

    def test_daily_with_points_swaps_equity_curve(self, populated_client):
        client, _, daily = populated_client
        daily.equity_curve = [
            {"date": "2024-01-15", "equity_twd": 1_010_000.0, "n_positions": 5,
             "fx_usd_twd": 31.5, "has_overlay": False, "cash_twd": 0},
            {"date": "2024-02-15", "equity_twd": 1_040_000.0, "n_positions": 5,
             "fx_usd_twd": 31.55, "has_overlay": False, "cash_twd": 0},
            {"date": "2024-02-29", "equity_twd": 1_050_000.0, "n_positions": 5,
             "fx_usd_twd": 31.6, "has_overlay": True, "cash_twd": 0},
        ]
        d = client.get("/api/summary?resolution=daily").json()["data"]
        assert d["resolution"] == "daily"
        assert len(d["equity_curve"]) == 3
        assert d["equity_curve"][0]["date"] == "2024-01-15"
        # cum_twr is monthly-anchored — the last daily date sits at the
        # last month-end (2024-02-29) which equals the monthly cum_twr.
        assert d["equity_curve"][-1]["cum_twr"] != 0


class TestTodayRepricedKPIs:
    def test_warm_path_repriced_kpis_appear(self, populated_client):
        client, _, daily = populated_client
        # Warm path: snapshot present + positions list → today reprice
        # populates real_now_twd, unrealized_pnl_twd.
        daily.snapshot = {"date": "2024-03-01", "fx_usd_twd": 31.7}
        daily.positions = [
            {"symbol": "2330", "qty": 1000, "cost_local": 600_000,
             "mv_local": 700_000, "mv_twd": 700_000,
             "type": "現股", "source": "pdf"},
        ]
        d = client.get("/api/summary").json()["data"]
        kpis = d["kpis"]
        assert "real_now_twd" in kpis
        assert "repriced_holdings_count" in kpis
        assert kpis["repriced_holdings_count"] == 1
