"""Phase 7 acceptance tests for scripts/validate_data.py.

The script implements 5 integrity checks per spec §7:
  (a) per-symbol gap detection in prices for every held symbol
  (b) symbol_market resolution coverage (no NULLs / no missing rows)
  (c) fx_daily has no gaps in held-position window
  (d) cross-source agreement spot-check (≤0.5% diff)
  (e) most-recent month-end portfolio_daily equity matches portfolio.json

Each check returns a list of issues. The CLI exits 0 if all are empty,
1 otherwise. The cross-source check (d) is mocked here — the test asserts
the comparison logic, not yfinance behavior.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.daily_store import DailyStore
from scripts import validate_data


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dashboard.db")
    s.init_schema()
    return s


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seed_prices(store: DailyStore, symbol: str, dates: list[str], close: float = 100.0,
                  currency: str = "TWD", source: str = "twse") -> None:
    now = _now()
    with store.connect_rw() as conn:
        for d in dates:
            conn.execute(
                "INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?)",
                (d, symbol, close, currency, source, now),
            )


def _seed_fx(store: DailyStore, dates: list[str], rate: float = 32.0,
             ccy: str = "USD") -> None:
    now = _now()
    with store.connect_rw() as conn:
        for d in dates:
            conn.execute(
                "INSERT INTO fx_daily VALUES (?, ?, ?, ?, ?)",
                (d, ccy, rate, "yfinance", now),
            )


def _seed_symbol_market(store: DailyStore, symbol: str, market: str) -> None:
    now = _now()
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO symbol_market VALUES (?, ?, ?, ?)",
            (symbol, market, now, now),
        )


def _seed_portfolio_daily(store: DailyStore, date: str, equity: float, fx: float = 32.0) -> None:
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO portfolio_daily VALUES (?, ?, NULL, ?, 1, 0)",
            (date, equity, fx),
        )


# --- Check (a): per-symbol price gaps -----------------------------------


def test_check_price_gaps_ok_when_dense(store: DailyStore) -> None:
    """All held symbols have prices for every trading day → no issues."""
    held = {"2330": ["2025-08-15", "2025-08-18", "2025-08-19"]}  # Mon, Mon, Tue
    _seed_prices(store, "2330", held["2330"])
    issues = validate_data.check_price_gaps(store, held)
    assert issues == []


def test_check_price_gaps_flags_missing_dates(store: DailyStore) -> None:
    held = {"2330": ["2025-08-15", "2025-08-18", "2025-08-19"]}
    _seed_prices(store, "2330", ["2025-08-15", "2025-08-19"])  # missing 18th
    issues = validate_data.check_price_gaps(store, held)
    assert len(issues) == 1
    assert issues[0]["symbol"] == "2330"
    assert "2025-08-18" in issues[0]["missing"]


# --- Check (b): symbol_market resolution coverage ----------------------


def test_check_symbol_market_ok_when_all_held_resolved(store: DailyStore) -> None:
    _seed_symbol_market(store, "2330", "twse")
    _seed_symbol_market(store, "5483", "tpex")
    issues = validate_data.check_symbol_market_coverage(store, {"2330", "5483"})
    assert issues == []


def test_check_symbol_market_flags_missing_resolution(store: DailyStore) -> None:
    """Held symbol with no symbol_market row → flagged."""
    _seed_symbol_market(store, "2330", "twse")
    issues = validate_data.check_symbol_market_coverage(store, {"2330", "5483"})
    assert len(issues) == 1
    assert issues[0]["symbol"] == "5483"


def test_check_symbol_market_flags_unknown_market(store: DailyStore) -> None:
    """A held symbol cached as 'unknown' is also a problem — it means
    neither TWSE nor TPEX recognized it, so we have no prices."""
    _seed_symbol_market(store, "5483", "unknown")
    issues = validate_data.check_symbol_market_coverage(store, {"5483"})
    assert len(issues) == 1
    assert issues[0]["market"] == "unknown"


# --- Check (c): fx_daily gaps ------------------------------------------


def test_check_fx_gaps_ok_when_dense(store: DailyStore) -> None:
    _seed_fx(store, ["2025-08-15", "2025-08-18", "2025-08-19"])
    issues = validate_data.check_fx_gaps(
        store, "USD", ["2025-08-15", "2025-08-18", "2025-08-19"]
    )
    assert issues == []


def test_check_fx_gaps_flags_missing_dates(store: DailyStore) -> None:
    _seed_fx(store, ["2025-08-15", "2025-08-19"])  # missing 18th
    issues = validate_data.check_fx_gaps(
        store, "USD", ["2025-08-15", "2025-08-18", "2025-08-19"]
    )
    assert len(issues) == 1
    assert "2025-08-18" in issues[0]["missing"]


# --- Check (d): cross-source agreement ---------------------------------


def test_check_cross_source_agreement_within_tolerance(store: DailyStore) -> None:
    _seed_prices(store, "2330", ["2026-04-01"], close=1850.0)
    # yfinance is mocked to return very close value (0.2% diff)
    issues = validate_data.check_cross_source_agreement(
        store,
        symbols=["2330"],
        sample_date="2026-04-01",
        yfinance_fetch=lambda yf_sym, d: 1853.5,  # 1853.5 / 1850 = 1.0019 → 0.19%
        tolerance_pct=0.5,
    )
    assert issues == []


def test_check_cross_source_agreement_flags_outside_tolerance(store: DailyStore) -> None:
    _seed_prices(store, "2330", ["2026-04-01"], close=1850.0)
    issues = validate_data.check_cross_source_agreement(
        store,
        symbols=["2330"],
        sample_date="2026-04-01",
        yfinance_fetch=lambda yf_sym, d: 1900.0,  # 2.7% diff, > 0.5%
        tolerance_pct=0.5,
    )
    assert len(issues) == 1
    assert issues[0]["symbol"] == "2330"
    assert issues[0]["diff_pct"] > 0.5


def test_check_cross_source_skips_symbols_without_prices(store: DailyStore) -> None:
    """If a symbol has no price for the sample date, skip it (don't error)."""
    issues = validate_data.check_cross_source_agreement(
        store,
        symbols=["2330"],
        sample_date="2026-04-01",
        yfinance_fetch=lambda *_: 1850.0,
        tolerance_pct=0.5,
    )
    assert issues == []


# --- Check (e): month-end equity reconciliation -------------------------


def test_check_month_end_equity_within_tolerance(store: DailyStore) -> None:
    _seed_portfolio_daily(store, "2026-03-31", 100_000.0)
    portfolio = {
        "months": [
            {"month": "2026-03", "equity_twd": 100_500.0},  # 0.5% diff
        ],
    }
    issues = validate_data.check_month_end_equity(store, portfolio, tolerance_pct=1.0)
    assert issues == []


def test_check_month_end_equity_flags_large_drift(store: DailyStore) -> None:
    _seed_portfolio_daily(store, "2026-03-31", 100_000.0)
    portfolio = {
        "months": [
            {"month": "2026-03", "equity_twd": 110_000.0},  # 10% diff
        ],
    }
    issues = validate_data.check_month_end_equity(store, portfolio, tolerance_pct=1.0)
    assert len(issues) == 1
    assert issues[0]["month"] == "2026-03"


def test_check_month_end_equity_handles_missing_portfolio_daily(
    store: DailyStore,
) -> None:
    """If we never derived a portfolio_daily row for the requested
    month-end (e.g. backfill never ran), flag rather than crash."""
    portfolio = {
        "months": [{"month": "2026-03", "equity_twd": 100_000.0}],
    }
    issues = validate_data.check_month_end_equity(store, portfolio, tolerance_pct=1.0)
    assert len(issues) == 1
    assert issues[0].get("reason") == "no_portfolio_daily_row"


# --- CLI exit code -------------------------------------------------------


def test_run_validation_returns_zero_on_clean_db(
    store: DailyStore, tmp_path: Path, monkeypatch
) -> None:
    """An empty-but-valid database (no held symbols) exits 0."""
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({"months": [], "summary": {}}))
    rc = validate_data.run_validation(store, portfolio_path, today="2026-04-27")
    assert rc == 0


def test_run_validation_returns_nonzero_when_check_fails(
    store: DailyStore, tmp_path: Path
) -> None:
    """Seed a portfolio that holds 5483 but no symbol_market row; expect rc=1."""
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({
        "months": [{
            "month": "2025-09",
            "fx_usd_twd": 30.0,
            "tw": {
                "month": "2025-09",
                "holdings": [
                    {"type": "現股", "code": "5483", "qty": 100.0,
                     "avg_cost": 100.0, "cost": 10000.0,
                     "ref_price": 105.0, "mkt_value": 10500.0},
                ],
                "subtotal": {}, "trades": [], "rebates": [],
            },
            "foreign": {"month": "2025-09", "holdings": [], "trades": [],
                        "dividends": [], "cashflow_by_ccy": {}},
            "bank": {"month": "2025-09", "fx": 30.0,
                     "cash_total_twd": 0, "cash_twd": 0, "cash_foreign_twd": 0},
            "equity_twd": 10500.0,
        }],
        "summary": {"all_trades": [], "kpis": {"as_of": "2025-09"}},
    }))
    rc = validate_data.run_validation(store, portfolio_path, today="2025-09-30")
    assert rc == 1
