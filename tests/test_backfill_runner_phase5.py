"""Phase 5 integration: backfill_runner must consult symbol_market and
persist the correct verdict per symbol. The runner used to hardcode
market='twse' for any symbol with rows; phase 5 delegates that to the
router so OTC symbols actually land as 'tpex'.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.backfill_runner import run_tw_backfill
from app.daily_store import DailyStore


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


def test_runner_persists_tpex_market_for_otc_symbol(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """When a held symbol is on TPEX (TWSE returns empty), symbol_market
    must end up with market='tpex', not 'twse'.

    This is the regression that the phase-3 runner had: it hardcoded
    'twse' regardless of the actual exchange. Phase 5 delegates to the
    router (price_sources._persist_market) which knows the truth.
    """
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda s, y, m: [
            {"date": f"{y}-{m:02d}-15", "close": 105.0, "volume": 100},
        ],
    )
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2025-09-30")

    run_tw_backfill(store, portfolio_path)

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("5483",)
        ).fetchone()
        assert row is not None, "symbol_market must have a row for 5483"
        assert row["market"] == "tpex", (
            f"OTC symbol must be cached as tpex, got {row['market']!r}"
        )

        # And the price rows must be tagged source='tpex'
        prices = conn.execute(
            "SELECT source FROM prices WHERE symbol = ?", ("5483",)
        ).fetchall()
        assert prices, "expected at least one price row for 5483"
        assert all(p["source"] == "tpex" for p in prices)


def test_runner_does_not_re_probe_cached_symbols(
    portfolio_path: Path, store: DailyStore, monkeypatch
) -> None:
    """Spec acceptance: 're-running backfill_daily.py does not re-probe
    cached symbols' (we count probe-side calls and assert the second run
    skips the TWSE probe for a TPEX-cached symbol).
    """
    twse_calls: list[tuple] = []

    def fake_twse(stockNo, year, month):
        twse_calls.append((stockNo, year, month))
        return []

    monkeypatch.setattr("app.price_sources.twse_fetch_month", fake_twse)
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda s, y, m: [
            {"date": f"{y}-{m:02d}-15", "close": 105.0, "volume": 100},
        ],
    )
    monkeypatch.setattr("app.backfill_runner._today_iso", lambda: "2025-09-30")

    run_tw_backfill(store, portfolio_path)
    twse_first = len(twse_calls)
    assert twse_first > 0  # Probed TWSE on first run

    run_tw_backfill(store, portfolio_path)
    # Second run must NOT have probed TWSE again — symbol cached as tpex
    assert len(twse_calls) == twse_first, (
        "second backfill should not re-probe TWSE for cached TPEX symbols"
    )
