"""Backfill runner consults symbol_market and persists the correct
verdict per symbol — exercised end-to-end against a portfolio holding
one OTC stock. After the TWSE/TPEX-removal refactor, both verdicts come
from yfinance probes (`.TW` first, then `.TWO`).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from invest.jobs.backfill_runner import run_tw_backfill
from invest.persistence.daily_store import DailyStore


def _portfolio_with_tpex_symbol() -> dict:
    """A portfolio holding one OTC stock (5483) currently."""
    return {
        "months": [
            {
                "month": "2025-09",
                "fx_usd_twd": 30.0,
                "tw": {
                    "month": "2025-09",
                    "holdings": [
                        {"type": "現股", "code": "5483", "qty": 1000.0,
                         "avg_cost": 100.0, "cost": 100000.0,
                         "ref_price": 105.0, "mkt_value": 105000.0},
                    ],
                    "subtotal": {}, "trades": [], "rebates": [],
                },
                "foreign": {"month": "2025-09", "holdings": [], "trades": [],
                            "dividends": [], "cashflow_by_ccy": {}},
                "bank": {"month": "2025-09", "fx": 30.0, "cash_total_twd": 0,
                         "cash_twd": 0, "cash_foreign_twd": 0},
                "tw_market_value_twd": 105000.0, "foreign_market_value_twd": 0.0,
                "bank_usd_in_twd": 0.0, "bank_twd": 0.0,
                "equity_twd": 105000.0, "external_flow_twd": 0.0,
                "investment_flows_twd": {}, "dividend_events": [],
                "period_return": 0.0, "cum_twr": 1.0, "v_start": 105000.0,
                "xirr": 0.0,
            },
        ],
        "summary": {
            "all_trades": [
                {"month": "2025-08", "date": "2025/08/15", "venue": "TW",
                 "side": "普買", "code": "5483", "name": "中美晶", "qty": 1000.0,
                 "price": 100.0, "ccy": "TWD", "gross_twd": 100000,
                 "fee_twd": 100, "tax_twd": 0, "net_twd": -100100,
                 "margin_loan_twd": 0, "self_funded_twd": 100100},
            ],
            "kpis": {"as_of": "2025-09"},
        },
    }


@pytest.fixture()
def portfolio_path(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps(_portfolio_with_tpex_symbol()), encoding="utf-8")
    return p


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dashboard.db")
    s.init_schema()
    return s


def _fake_yf_otc_only(symbol: str, start: str, end: str) -> list[dict]:
    """Fake yfinance: `.TW` returns empty, `.TWO` returns one row.

    Mirrors the production behavior for an OTC symbol (5483 / 中美晶):
    Yahoo has no listing under 5483.TW but does under 5483.TWO.
    """
    if symbol.endswith(".TWO"):
        return [{"date": "2025-09-15", "close": 105.0, "volume": 100}]
    return []


def test_runner_persists_tpex_market_for_otc_symbol(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """When a held symbol is OTC (`.TW` returns empty, `.TWO` returns
    rows), symbol_market must end up with market='tpex', not 'twse'."""
    monkeypatch.setattr("invest.prices.yfinance_client.fetch_prices", _fake_yf_otc_only)
    monkeypatch.setattr("invest.jobs.backfill_runner._today_iso", lambda: "2025-09-30")

    run_tw_backfill(store, portfolio_path)

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("5483",)
        ).fetchone()
        assert row is not None, "symbol_market must have a row for 5483"
        assert row["market"] == "tpex", (
            f"OTC symbol must be cached as tpex, got {row['market']!r}"
        )

        # All price rows are now tagged source='yfinance' regardless of suffix
        prices = conn.execute(
            "SELECT source FROM prices WHERE symbol = ?", ("5483",)
        ).fetchall()
        assert prices, "expected at least one price row for 5483"
        assert all(p["source"] == "yfinance" for p in prices)


def test_runner_does_not_re_probe_cached_symbols(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """Spec acceptance: 're-running backfill_daily.py does not re-probe
    cached symbols'. Once 5483 is cached as 'tpex', the second backfill
    must skip the `.TW` probe entirely.
    """
    queried_first: list[str] = []

    def yf_first(symbol, start, end):
        queried_first.append(symbol)
        return _fake_yf_otc_only(symbol, start, end)

    monkeypatch.setattr("invest.prices.yfinance_client.fetch_prices", yf_first)
    monkeypatch.setattr("invest.jobs.backfill_runner._today_iso", lambda: "2025-09-30")

    run_tw_backfill(store, portfolio_path)
    # First run probed both suffixes (.TW empty → .TWO success)
    assert any(s.endswith(".TW") and not s.endswith(".TWO") for s in queried_first)
    assert any(s.endswith(".TWO") for s in queried_first)

    queried_second: list[str] = []

    def yf_second(symbol, start, end):
        queried_second.append(symbol)
        return _fake_yf_otc_only(symbol, start, end)

    monkeypatch.setattr("invest.prices.yfinance_client.fetch_prices", yf_second)
    run_tw_backfill(store, portfolio_path)
    # Second run must NOT have probed `.TW` — symbol cached as 'tpex'
    listed_probes = [s for s in queried_second if s.endswith(".TW") and not s.endswith(".TWO")]
    assert not listed_probes, (
        f"second backfill should not re-probe .TW for cached TPEX symbols, got {listed_probes}"
    )
