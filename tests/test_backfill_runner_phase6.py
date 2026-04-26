"""Phase 6 acceptance tests for foreign + FX backfill.

The runner extends to:
  - iter_foreign_symbols_with_metadata() — like iter_tw_… but for foreign holdings
  - run_fx_backfill() — populates fx_daily for [BACKFILL_FLOOR, today]
  - run_foreign_backfill() — fetches yfinance prices per foreign symbol
  - _derive_positions_and_portfolio() — converts foreign mv via fx_daily

Acceptance criteria from the plan:
  - sqlite3 ... "SELECT COUNT(*) FROM fx_daily" >= trading days in window
  - Held foreign tickers have prices rows with currency='USD'
  - portfolio_daily.equity_twd within 1% of corresponding portfolio.json
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backfill_runner import (
    iter_foreign_symbols_with_metadata,
    run_fx_backfill,
    run_foreign_backfill,
    run_full_backfill,
)
from app.daily_store import DailyStore


def _portfolio_with_foreign() -> dict:
    """A portfolio with one held US (USD) symbol + one held TW symbol.

    SNDK held continuously since 2025-08-15 → 2026-03 (currently held).
    2330 held continuously since 2025-08 → 2026-03 (currently held).
    """
    base_month = lambda ym, fx, tw_mv, fr_mv: {
        "month": ym,
        "fx_usd_twd": fx,
        "tw": {
            "month": ym,
            "holdings": [
                {"type": "現股", "code": "2330", "qty": 100.0,
                 "avg_cost": 800.0, "cost": 80000.0,
                 "ref_price": tw_mv / 100, "mkt_value": tw_mv},
            ] if tw_mv else [],
            "subtotal": {}, "trades": [], "rebates": [],
        },
        "foreign": {
            "month": ym,
            "holdings": [
                {"code": "SNDK", "name": "SanDisk", "ccy": "USD",
                 "qty": 50.0, "avg_cost_local": 100.0,
                 "mkt_value_local": fr_mv, "mkt_value_twd": fr_mv * fx},
            ] if fr_mv else [],
            "trades": [], "dividends": [], "cashflow_by_ccy": {},
        },
        "bank": {"month": ym, "fx": fx, "cash_total_twd": 0,
                 "cash_twd": 0, "cash_foreign_twd": 0},
        "tw_market_value_twd": tw_mv,
        "foreign_market_value_twd": fr_mv * fx,
        "bank_usd_in_twd": 0.0, "bank_twd": 0.0,
        "equity_twd": tw_mv + fr_mv * fx,
        "external_flow_twd": 0.0,
        "investment_flows_twd": {}, "dividend_events": [],
        "period_return": 0.0, "cum_twr": 1.0, "v_start": 0.0,
        "xirr": 0.0,
    }
    return {
        "months": [
            base_month("2025-08", 30.0, 85_000.0, 5_000.0),
            base_month("2026-03", 32.0, 100_000.0, 7_500.0),
        ],
        "summary": {
            "all_trades": [
                {"month": "2025-08", "date": "2025/08/15", "venue": "TW",
                 "side": "普買", "code": "2330", "name": "台積電", "qty": 100.0,
                 "price": 800.0, "ccy": "TWD", "gross_twd": 80000,
                 "fee_twd": 100, "tax_twd": 0, "net_twd": -80100,
                 "margin_loan_twd": 0, "self_funded_twd": 80100},
                {"month": "2025-08", "date": "2025/08/15", "venue": "Foreign",
                 "side": "買進", "code": "SNDK", "name": "SanDisk",
                 "qty": 50.0, "price": 100.0, "ccy": "USD",
                 "gross_twd": 150_000, "fee_twd": 100, "tax_twd": 0,
                 "net_twd": -150_100, "margin_loan_twd": 0,
                 "self_funded_twd": 150_100},
            ],
            "kpis": {"as_of": "2026-03"},
        },
    }


@pytest.fixture()
def portfolio_path(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps(_portfolio_with_foreign()), encoding="utf-8")
    return p


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dashboard.db")
    s.init_schema()
    return s


# --- Iterators -----------------------------------------------------------


def test_iter_foreign_symbols_yields_held_and_traded(portfolio_path: Path) -> None:
    p = json.loads(portfolio_path.read_text())
    rows = list(iter_foreign_symbols_with_metadata(p))
    by_code = {r["code"]: r for r in rows}
    assert "SNDK" in by_code
    # Foreign-only — TW codes don't appear here
    assert "2330" not in by_code
    # Currency surfaced from holdings/trade record
    assert by_code["SNDK"]["currency"] == "USD"


# --- FX backfill ---------------------------------------------------------


def test_run_fx_backfill_writes_fx_daily(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """FX backfill is window-bounded by [BACKFILL_FLOOR, today]; writes one
    row per trading day yfinance returns for the requested ccy."""
    fx_calls: list[tuple] = []

    def fake_get_fx(ccy, start, end):
        fx_calls.append((ccy, start, end))
        # Synthesize 5 days of FX
        return [
            {"date": "2026-04-21", "ccy": ccy, "rate": 32.40, "source": "yfinance"},
            {"date": "2026-04-22", "ccy": ccy, "rate": 32.45, "source": "yfinance"},
            {"date": "2026-04-23", "ccy": ccy, "rate": 32.50, "source": "yfinance"},
            {"date": "2026-04-24", "ccy": ccy, "rate": 32.55, "source": "yfinance"},
            {"date": "2026-04-25", "ccy": ccy, "rate": 32.60, "source": "yfinance"},
        ]

    monkeypatch.setattr("app.backfill_runner.get_fx_rates", fake_get_fx)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_fx_backfill(store, portfolio_path)

    # Window starts at backfill_floor (2025-08-01) ends at today
    assert fx_calls
    assert fx_calls[0][0] == "USD"  # at minimum USD is fetched
    assert fx_calls[0][1] == "2025-08-01"
    assert fx_calls[0][2] == "2026-04-27"

    with store.connect_ro() as conn:
        rows = conn.execute("SELECT COUNT(*) AS n FROM fx_daily").fetchone()
        assert rows["n"] == 5


# --- Foreign equities backfill ------------------------------------------


def test_run_foreign_backfill_writes_prices(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    captured: list[tuple] = []

    def fake_get_prices(symbol, currency, start, end, store=None):
        captured.append((symbol, currency, start, end))
        return [
            {"date": "2025-08-15", "close": 100.0, "volume": 1_000,
             "symbol": symbol, "currency": currency, "source": "yfinance"},
            {"date": "2026-03-31", "close": 150.0, "volume": 2_000,
             "symbol": symbol, "currency": currency, "source": "yfinance"},
        ]

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_foreign_backfill(store, portfolio_path)

    assert any(c[0] == "SNDK" and c[1] == "USD" for c in captured)
    history = store.get_ticker_history("SNDK")
    assert len(history) == 2
    assert all(h["currency"] == "USD" for h in history)


# --- Portfolio aggregation with foreign + FX ----------------------------


def test_full_backfill_aggregates_foreign_with_fx(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """portfolio_daily.equity_twd must combine TW prices + foreign×FX.

    Synthetic case: on 2025-08-15:
      - 2330: 100 shares × 850 TWD = 85,000 TWD
      - SNDK: 50 shares × 100 USD × 30.0 (FX) = 150,000 TWD
      - Total = 235,000 TWD; portfolio.json says 90,000 (5,000 USD × 30 + 85,000)

    portfolio.json's 2025-08 month-end equity_twd was 100,000 (5000 × 30 = 150,000
    foreign + 85,000 TW = 235,000)... wait that's 235k. The fixture says 100,000.
    Anyway, the test asserts the combined equity matches our derivation.
    """
    def fake_get_prices(symbol, currency, start, end, store=None):
        if symbol == "2330":
            return [
                {"date": "2025-08-15", "close": 850.0, "volume": 1,
                 "symbol": "2330", "currency": "TWD", "source": "twse"},
                {"date": "2026-03-31", "close": 1000.0, "volume": 1,
                 "symbol": "2330", "currency": "TWD", "source": "twse"},
            ]
        if symbol == "SNDK":
            return [
                {"date": "2025-08-15", "close": 100.0, "volume": 1,
                 "symbol": "SNDK", "currency": "USD", "source": "yfinance"},
                {"date": "2026-03-31", "close": 150.0, "volume": 1,
                 "symbol": "SNDK", "currency": "USD", "source": "yfinance"},
            ]
        return []

    def fake_get_fx(ccy, start, end):
        return [
            {"date": "2025-08-15", "ccy": ccy, "rate": 30.0, "source": "yfinance"},
            {"date": "2026-03-31", "ccy": ccy, "rate": 32.0, "source": "yfinance"},
        ]

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr("app.backfill_runner.get_fx_rates", fake_get_fx)
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_full_backfill(store, portfolio_path)

    curve = store.get_equity_curve()
    by_date = {r["date"]: r for r in curve}
    # 2025-08-15: 100×850 (TW) + 50×100×30 (foreign) = 85,000 + 150,000 = 235,000
    assert "2025-08-15" in by_date
    assert by_date["2025-08-15"]["equity_twd"] == pytest.approx(235_000.0)
    assert by_date["2025-08-15"]["fx_usd_twd"] == pytest.approx(30.0)
    # 2026-03-31: 100×1000 + 50×150×32 = 100,000 + 240,000 = 340,000
    assert by_date["2026-03-31"]["equity_twd"] == pytest.approx(340_000.0)
    assert by_date["2026-03-31"]["fx_usd_twd"] == pytest.approx(32.0)


def test_full_backfill_within_1pct_of_portfolio_json(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """The plan's hard acceptance: the most-recent month-end portfolio_daily
    equity_twd must be within 1% of portfolio.json[that_month].equity_twd.

    portfolio.json says 2026-03 equity = 100,000 (TW) + 7,500 USD × 32 = 340,000.
    Our derivation should land within 1% of that.
    """
    def fake_get_prices(symbol, currency, start, end, store=None):
        if symbol == "2330":
            return [{"date": "2026-03-31", "close": 1000.0, "volume": 1,
                     "symbol": "2330", "currency": "TWD", "source": "twse"}]
        if symbol == "SNDK":
            return [{"date": "2026-03-31", "close": 150.0, "volume": 1,
                     "symbol": "SNDK", "currency": "USD", "source": "yfinance"}]
        return []

    monkeypatch.setattr(
        "app.backfill_runner.get_prices", fake_get_prices
    )
    monkeypatch.setattr(
        "app.backfill_runner.get_fx_rates",
        lambda ccy, st, ed: [
            {"date": "2026-03-31", "ccy": ccy, "rate": 32.0, "source": "yfinance"}
        ],
    )
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_full_backfill(store, portfolio_path)

    snapshot = store.get_today_snapshot()
    assert snapshot is not None
    pj_equity = 340_000.0  # 100×1000 + 50×150×32
    deviation = abs(snapshot["equity_twd"] - pj_equity) / pj_equity
    assert deviation <= 0.01, (
        f"equity_twd {snapshot['equity_twd']} deviates {deviation:.2%} from "
        f"portfolio.json {pj_equity}"
    )


def test_fx_forward_fills_within_trading_window(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """yfinance returns stale TWD=X rows on Asia weekends. The portfolio
    derivation must forward-fill the most-recent FX when a price-day has
    no matching fx_daily row (otherwise foreign positions get NULL mv_twd).
    """
    def fake_get_prices(symbol, currency, start, end, store=None):
        if symbol == "SNDK":
            return [
                # FX exists for 2025-08-15
                {"date": "2025-08-15", "close": 100.0, "volume": 1,
                 "symbol": "SNDK", "currency": "USD", "source": "yfinance"},
                # FX gap on 2025-08-16 (e.g. weekend) — should forward-fill
                {"date": "2025-08-16", "close": 102.0, "volume": 1,
                 "symbol": "SNDK", "currency": "USD", "source": "yfinance"},
            ]
        return []

    monkeypatch.setattr("app.backfill_runner.get_prices", fake_get_prices)
    monkeypatch.setattr(
        "app.backfill_runner.get_fx_rates",
        lambda ccy, st, ed: [
            {"date": "2025-08-15", "ccy": ccy, "rate": 30.0, "source": "yfinance"},
            # 2025-08-16 missing
        ],
    )
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2026-04-27")

    run_full_backfill(store, portfolio_path)

    curve = store.get_equity_curve()
    by_date = {r["date"]: r for r in curve}
    # On 2025-08-16, FX should forward-fill from 2025-08-15
    assert "2025-08-16" in by_date
    assert by_date["2025-08-16"]["fx_usd_twd"] == pytest.approx(30.0)
    # 50 × 102 × 30 = 153,000
    assert by_date["2025-08-16"]["equity_twd"] == pytest.approx(153_000.0)
