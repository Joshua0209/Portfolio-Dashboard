"""Phase 8 acceptance tests for /api/daily/prices/<symbol>.

The endpoint returns daily close history for one symbol, with optional
trade markers from portfolio.json (so charts can show buy/sell pins).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app import create_app
from app.daily_store import DailyStore


def _portfolio_with_2330() -> dict:
    return {
        "months": [
            {"month": "2025-08", "fx_usd_twd": 30.0,
             "tw": {"month": "2025-08", "holdings": [
                 {"type": "現股", "code": "2330", "qty": 100.0,
                  "avg_cost": 800.0, "cost": 80000.0,
                  "ref_price": 850.0, "mkt_value": 85000.0}],
                    "subtotal": {}, "trades": [], "rebates": []},
             "foreign": {"month": "2025-08", "holdings": [], "trades": [],
                         "dividends": [], "cashflow_by_ccy": {}},
             "bank": {"month": "2025-08", "fx": 30.0, "cash_total_twd": 0,
                      "cash_twd": 0, "cash_foreign_twd": 0},
             "tw_market_value_twd": 85000.0, "foreign_market_value_twd": 0.0,
             "bank_usd_in_twd": 0.0, "bank_twd": 0.0, "equity_twd": 85000.0,
             "external_flow_twd": 0.0, "investment_flows_twd": {},
             "dividend_events": [], "period_return": 0.0, "cum_twr": 1.0,
             "v_start": 85000.0, "xirr": 0.0},
        ],
        "summary": {
            "all_trades": [
                {"month": "2025-01", "date": "2025/01/10", "venue": "TW",
                 "side": "普買", "code": "2330", "name": "台積電", "qty": 100.0,
                 "price": 800.0, "ccy": "TWD", "gross_twd": 80000,
                 "fee_twd": 100, "tax_twd": 0, "net_twd": -80100,
                 "margin_loan_twd": 0, "self_funded_twd": 80100},
                {"month": "2025-09", "date": "2025/09/15", "venue": "TW",
                 "side": "普賣", "code": "2330", "name": "台積電", "qty": 50.0,
                 "price": 900.0, "ccy": "TWD", "gross_twd": 45000,
                 "fee_twd": 50, "tax_twd": 135, "net_twd": 44815,
                 "margin_loan_twd": 0, "self_funded_twd": -44815},
            ],
            "by_ticker": {
                "2330": {
                    "code": "2330", "name": "台積電", "venue": "TW",
                    "trades": [], "lots": [], "realized_twd": 0.0,
                },
            },
            "kpis": {"as_of": "2025-08"},
        },
    }


def _seed_prices(store: DailyStore, symbol: str, rows: list[tuple]) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with store.connect_rw() as conn:
        for d, close in rows:
            conn.execute(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?)",
                (d, symbol, close, "TWD", "yfinance", now),
            )


@pytest.fixture()
def app_with_daily_prices(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "dashboard.db"))
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(
        json.dumps(_portfolio_with_2330()), encoding="utf-8"
    )
    app = create_app(data_path=portfolio_path)
    _seed_prices(app.extensions["daily_store"], "2330", [
        ("2025-08-15", 850.0),
        ("2025-08-18", 855.0),
        ("2025-09-15", 900.0),
        ("2025-09-16", 895.0),
    ])
    return app


def test_daily_prices_returns_seeded_history(app_with_daily_prices) -> None:
    client = app_with_daily_prices.test_client()
    r = client.get("/api/daily/prices/2330")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    pts = body["data"]["points"]
    assert len(pts) == 4
    assert pts[0]["date"] == "2025-08-15"
    assert pts[0]["close"] == 850.0


def test_daily_prices_includes_trade_markers(app_with_daily_prices) -> None:
    """Trade markers must align to portfolio.json trade dates."""
    client = app_with_daily_prices.test_client()
    r = client.get("/api/daily/prices/2330")
    body = r.get_json()
    trades = body["data"]["trades"]
    assert len(trades) == 2
    # Dates normalized to ISO
    dates = sorted(t["date"] for t in trades)
    assert dates == ["2025-01-10", "2025-09-15"]
    # Side surfaced for marker color
    sides = {t["date"]: t["side"] for t in trades}
    assert "買" in sides["2025-01-10"]
    assert "賣" in sides["2025-09-15"]


def test_daily_prices_filters_by_window(app_with_daily_prices) -> None:
    client = app_with_daily_prices.test_client()
    r = client.get("/api/daily/prices/2330?start=2025-09-01&end=2025-09-30")
    pts = r.get_json()["data"]["points"]
    assert all("2025-09" in p["date"] for p in pts)
    assert len(pts) == 2


def test_daily_prices_404_for_unknown_symbol(app_with_daily_prices) -> None:
    client = app_with_daily_prices.test_client()
    r = client.get("/api/daily/prices/9999")
    assert r.status_code == 200  # empty envelope is preferred over 404
    body = r.get_json()
    assert body["data"]["points"] == []
    assert body["data"]["empty"] is True


def test_daily_prices_empty_envelope_on_fresh_db(tmp_path, monkeypatch) -> None:
    """Phase 4 contract: empty DB returns empty envelope, not 500."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "dashboard.db"))
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({"months": [], "summary": {}}))
    app = create_app(data_path=portfolio_path)
    client = app.test_client()
    r = client.get("/api/daily/prices/2330")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"]["points"] == []
    assert body["data"]["empty"] is True


def test_ticker_detail_with_resolution_daily_returns_daily_branch(
    app_with_daily_prices,
) -> None:
    """/api/tickers/<code>?resolution=daily exposes a 'daily_prices' field
    on the response so ticker.js can render the daily line without a
    second roundtrip."""
    client = app_with_daily_prices.test_client()
    r = client.get("/api/tickers/2330?resolution=daily")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert "daily_prices" in body["data"]
    pts = body["data"]["daily_prices"]["points"]
    assert len(pts) == 4
    # Trade markers carry through too
    assert "trades" in body["data"]["daily_prices"]


def test_ticker_detail_without_resolution_param_unchanged(
    app_with_daily_prices,
) -> None:
    """Backwards-compat: no resolution param → no daily_prices key."""
    client = app_with_daily_prices.test_client()
    r = client.get("/api/tickers/2330")
    body = r.get_json()
    assert "daily_prices" not in body["data"]
