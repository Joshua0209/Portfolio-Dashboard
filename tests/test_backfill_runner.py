"""Phase 3 acceptance tests for app/backfill_runner.run_tw_backfill().

The runner pulls TW symbols from portfolio.json, computes per-symbol
windows via compute_fetch_window(), calls price_sources.get_prices, and
UPSERTs into prices + derives basic positions_daily / portfolio_daily.

These tests stub out the network (via app.backfill_runner.get_prices) and
exercise the runner against a tiny synthetic portfolio fixture.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from app.backfill_runner import (
    iter_tw_symbols_with_metadata,
    run_tw_backfill,
)
from app.daily_store import DailyStore


def _portfolio_fixture() -> dict:
    """Minimal portfolio.json: 3 TW symbols hitting different windowing cases.

    - 2330: bought before floor, still held → window starts at floor, ends today
    - 2454: sold before latest month → window ends at sale month_end
    - 9999: exited 2024-12 (before floor) → skipped entirely
    """
    return {
        "months": [
            {
                "month": "2025-08",
                "fx_usd_twd": 30.0,
                "tw": {
                    "month": "2025-08",
                    "holdings": [
                        {"type": "現股", "code": "2330", "qty": 100.0,
                         "avg_cost": 800.0, "cost": 80000.0, "ref_price": 850.0,
                         "mkt_value": 85000.0},
                    ],
                    "subtotal": {"qty": 100, "cost": 80000, "mkt_value": 85000,
                                 "unrealized_pnl": 5000},
                    "trades": [],
                    "rebates": [],
                },
                "foreign": {"month": "2025-08", "holdings": [], "trades": [],
                            "dividends": [], "cashflow_by_ccy": {}},
                "bank": {"month": "2025-08", "fx": 30.0, "cash_total_twd": 0,
                         "cash_twd": 0, "cash_foreign_twd": 0},
                "tw_market_value_twd": 85000.0,
                "foreign_market_value_twd": 0.0,
                "bank_usd_in_twd": 0.0,
                "bank_twd": 0.0,
                "equity_twd": 85000.0,
                "external_flow_twd": 0.0,
                "investment_flows_twd": {},
                "dividend_events": [],
                "period_return": 0.0,
                "cum_twr": 1.0,
                "v_start": 85000.0,
                "xirr": 0.0,
            },
            {
                "month": "2026-03",
                "fx_usd_twd": 32.0,
                "tw": {
                    "month": "2026-03",
                    "holdings": [
                        {"type": "現股", "code": "2330", "qty": 100.0,
                         "avg_cost": 800.0, "cost": 80000.0, "ref_price": 1000.0,
                         "mkt_value": 100000.0},
                    ],
                    "subtotal": {"qty": 100, "cost": 80000, "mkt_value": 100000,
                                 "unrealized_pnl": 20000},
                    "trades": [],
                    "rebates": [],
                },
                "foreign": {"month": "2026-03", "holdings": [], "trades": [],
                            "dividends": [], "cashflow_by_ccy": {}},
                "bank": {"month": "2026-03", "fx": 32.0, "cash_total_twd": 0,
                         "cash_twd": 0, "cash_foreign_twd": 0},
                "tw_market_value_twd": 100000.0,
                "foreign_market_value_twd": 0.0,
                "bank_usd_in_twd": 0.0,
                "bank_twd": 0.0,
                "equity_twd": 100000.0,
                "external_flow_twd": 0.0,
                "investment_flows_twd": {},
                "dividend_events": [],
                "period_return": 0.0,
                "cum_twr": 1.18,
                "v_start": 85000.0,
                "xirr": 0.0,
            },
        ],
        "summary": {
            "all_trades": [
                {"month": "2025-01", "date": "2025/01/10", "venue": "TW",
                 "side": "普買", "code": "2330", "name": "台積電", "qty": 100.0,
                 "price": 800.0, "ccy": "TWD", "gross_twd": 80000,
                 "fee_twd": 100, "tax_twd": 0, "net_twd": -80100,
                 "margin_loan_twd": 0, "self_funded_twd": 80100},
                {"month": "2025-09", "date": "2025/09/05", "venue": "TW",
                 "side": "普買", "code": "2454", "name": "聯發科", "qty": 50.0,
                 "price": 1500.0, "ccy": "TWD", "gross_twd": 75000,
                 "fee_twd": 100, "tax_twd": 0, "net_twd": -75100,
                 "margin_loan_twd": 0, "self_funded_twd": 75100},
                {"month": "2025-12", "date": "2025/12/20", "venue": "TW",
                 "side": "普賣", "code": "2454", "name": "聯發科", "qty": 50.0,
                 "price": 1600.0, "ccy": "TWD", "gross_twd": 80000,
                 "fee_twd": 100, "tax_twd": 240, "net_twd": 79660,
                 "margin_loan_twd": 0, "self_funded_twd": -79660},
                {"month": "2024-06", "date": "2024/06/15", "venue": "TW",
                 "side": "普買", "code": "9999", "name": "舊持股", "qty": 1000.0,
                 "price": 50.0, "ccy": "TWD", "gross_twd": 50000,
                 "fee_twd": 75, "tax_twd": 0, "net_twd": -50075,
                 "margin_loan_twd": 0, "self_funded_twd": 50075},
                {"month": "2024-12", "date": "2024/12/10", "venue": "TW",
                 "side": "普賣", "code": "9999", "name": "舊持股", "qty": 1000.0,
                 "price": 60.0, "ccy": "TWD", "gross_twd": 60000,
                 "fee_twd": 90, "tax_twd": 180, "net_twd": 59730,
                 "margin_loan_twd": 0, "self_funded_twd": -59730},
            ],
            "kpis": {"as_of": "2026-03"},
        },
    }


@pytest.fixture()
def portfolio_path(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps(_portfolio_fixture()), encoding="utf-8")
    return p


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dashboard.db")
    s.init_schema()
    return s


def test_iter_tw_symbols_emits_codes_with_window_metadata(portfolio_path: Path) -> None:
    """The iterator should emit one entry per distinct TW code with its
    trade dates and held months pre-collected."""
    p = json.loads(portfolio_path.read_text())
    rows = list(iter_tw_symbols_with_metadata(p))
    by_code = {r["code"]: r for r in rows}
    # All 3 TW codes appear, foreign codes do not
    assert set(by_code) == {"2330", "2454", "9999"}
    assert sorted(by_code["2330"]["trade_dates"]) == ["2025-01-10"]
    assert "2025-08" in by_code["2330"]["held_months"]
    assert "2026-03" in by_code["2330"]["held_months"]


def test_run_tw_backfill_skips_symbols_outside_floor(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """9999 last activity = 2024-12 → fully precedes 2025-08-01 → no calls,
    no symbol_market row."""
    fetched: list[tuple] = []

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        fetched.append((symbol, currency, start, end))
        # Synthesize one row per requested month so positions_daily has data.
        return [
            {"date": start, "close": 1000.0, "volume": 1,
             "symbol": symbol, "currency": currency, "source": "yfinance"},
        ]

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    summary = run_tw_backfill(store, portfolio_path)

    fetched_codes = {f[0] for f in fetched}
    assert "9999" not in fetched_codes
    assert {"2330", "2454"} <= fetched_codes
    assert summary["skipped"] == ["9999"]


def test_run_tw_backfill_writes_prices(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    rows = [
        {"date": "2025-09-01", "close": 850.0, "volume": 100,
         "symbol": "2330", "currency": "TWD", "source": "yfinance"},
        {"date": "2025-09-02", "close": 855.0, "volume": 120,
         "symbol": "2330", "currency": "TWD", "source": "yfinance"},
    ]

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        return rows if symbol == "2330" else []

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_tw_backfill(store, portfolio_path)

    history = store.get_ticker_history("2330")
    assert len(history) == 2
    assert history[0]["close"] == 850.0


def test_run_tw_backfill_is_idempotent_via_upsert(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    rows1 = [{"date": "2025-09-01", "close": 850.0, "volume": 100,
              "symbol": "2330", "currency": "TWD", "source": "yfinance"}]
    rows2 = [{"date": "2025-09-01", "close": 999.0, "volume": 100,
              "symbol": "2330", "currency": "TWD", "source": "yfinance"}]
    state = {"calls": 0}

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if symbol != "2330":
            return []
        state["calls"] += 1
        return rows1 if state["calls"] == 1 else rows2

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_tw_backfill(store, portfolio_path)
    run_tw_backfill(store, portfolio_path)

    history = store.get_ticker_history("2330")
    assert len(history) == 1
    # Latest write wins (UPSERT)
    assert history[0]["close"] == 999.0


def test_run_tw_backfill_populates_portfolio_daily(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """portfolio_daily should have at least one row per trading day with
    a price, and equity_twd should reflect held qty * close."""
    rows = [
        {"date": "2025-08-15", "close": 850.0, "volume": 100,
         "symbol": "2330", "currency": "TWD", "source": "yfinance"},
        {"date": "2026-03-31", "close": 1000.0, "volume": 100,
         "symbol": "2330", "currency": "TWD", "source": "yfinance"},
    ]

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        return rows if symbol == "2330" else []

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_tw_backfill(store, portfolio_path)

    curve = store.get_equity_curve()
    by_date = {r["date"]: r for r in curve}
    # equity_twd folds positions MV with the synthesized broker-cash schedule
    # (Σ trade.net_twd through d). The fixture's chronological trade ledger
    # accumulates as: 2024-06-15 buy 9999 (-50_075), 2024-12-10 sell 9999
    # (+59_730), 2025-01-10 buy 2330 (-80_100), 2025-09-05 buy 2454 (-75_100),
    # 2025-12-20 sell 2454 (+79_660). Cumulative cash on each priced day:
    # • 2025-08-15 → -50_075 + 59_730 - 80_100 = -70_445
    # • 2026-03-31 → -70_445 - 75_100 + 79_660 = -65_885
    assert "2025-08-15" in by_date
    assert by_date["2025-08-15"]["equity_twd"] == pytest.approx(85_000.0 - 70_445.0)
    # On 2026-03-31: 100 shares of 2330 @ 1000 = 100,000 TWD positions MV
    assert by_date["2026-03-31"]["equity_twd"] == pytest.approx(100_000.0 - 65_885.0)


def test_month_end_iso_handles_month_lengths() -> None:
    from app.backfill_runner import month_end_iso
    assert month_end_iso("2025-02") == "2025-02-28"  # non-leap
    assert month_end_iso("2024-02") == "2024-02-29"  # leap
    assert month_end_iso("2025-04") == "2025-04-30"
    assert month_end_iso("2025-12") == "2025-12-31"
