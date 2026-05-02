"""Tests for app/holdings_today.py — the single source of truth for
"what's held today, valued today".

Two layers:
  1. Unit tests pin the warm/cold resolution rules of `current_holdings()`.
  2. The Bug 3 acceptance test proves the cross-endpoint fork is gone:
     when a user sells post-PDF, every endpoint that asks "what's held"
     reflects the sale identically. Pre-refactor, /api/holdings/current
     and /api/holdings/sectors would still show the pre-sale position
     because they branched on ?resolution=daily and the PDF projection
     path didn't know about the overlay.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.daily_store import DailyStore
from app.holdings_today import current_holdings, holdings_for_month


# ─── unit fixtures ────────────────────────────────────────────────────────


class _FakeStore:
    """Minimal DataStore stand-in with just .months."""
    def __init__(self, months):
        self.months = months


def _march_pdf_month():
    return {
        "month": "2026-03",
        "fx_usd_twd": 31.0,
        "tw": {
            "holdings": [
                {
                    "code": "2330", "name": "TSMC",
                    "qty": 1000, "avg_cost": 800.0, "cost": 800_000,
                    "ref_price": 1050.0, "mkt_value": 1_050_000,
                    "unrealized_pnl": 250_000, "type": "現股",
                },
            ],
            "trades": [], "rebates": [],
        },
        "foreign": {"holdings": [], "trades": []},
    }


@pytest.fixture()
def daily_store_empty(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "empty.db")
    s.init_schema()
    return s


@pytest.fixture()
def daily_store_post_pdf_sale(tmp_path: Path) -> DailyStore:
    """positions_daily reflects: user sold all 1000 shares of 2330 in April.

    portfolio_daily snapshot: April 22, after the sale, equity is the
    cash from the sale (we model it as 0 here for simplicity — the test
    only cares that positions_daily has no 2330 row).
    """
    s = DailyStore(tmp_path / "post_sale.db")
    s.init_schema()
    with s.connect_rw() as conn:
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
            "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
            ("2026-04-22", 0.0, 31.5, 0, 1),
        )
        # No INSERT into positions_daily — user sold everything.
    return s


@pytest.fixture()
def daily_store_post_pdf_held(tmp_path: Path) -> DailyStore:
    """positions_daily reflects: user still holds 1000 shares of 2330 on
    April 22 — but at a higher close price (1100 vs PDF's 1050)."""
    s = DailyStore(tmp_path / "post_held.db")
    s.init_schema()
    with s.connect_rw() as conn:
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
            "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
            ("2026-04-22", 1_100_000.0, 31.5, 1, 0),
        )
        conn.execute(
            "INSERT INTO positions_daily(date, symbol, qty, cost_local, "
            "mv_local, mv_twd, type, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("2026-04-22", "2330", 1000, 800_000, 1_100_000, 1_100_000, "現股", "pdf"),
        )
    return s


# ─── unit: warm path ─────────────────────────────────────────────────────


def test_current_holdings_warm_uses_positions_daily(daily_store_post_pdf_held):
    """Warm path: positions_daily has rows for as_of → return them with
    PDF metadata enrichment, NOT the PDF month-end values."""
    s = _FakeStore([_march_pdf_month()])
    rows = current_holdings(s, daily_store_post_pdf_held)

    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "2330"
    # mv comes from positions_daily (1_100_000), NOT PDF (1_050_000).
    assert row["mkt_value_twd"] == pytest.approx(1_100_000)
    # Metadata enrichment from PDF.
    assert row["name"] == "TSMC"
    assert row["avg_cost"] == 800.0
    # repriced_at marker proves we took the warm branch.
    assert row["repriced_at"] == "2026-04-22"


def test_current_holdings_warm_post_pdf_sale_returns_empty(
    daily_store_post_pdf_sale,
):
    """The exact Bug 3 scenario: PDF says 1000 shares, positions_daily
    says 0 (user sold). current_holdings() must reflect the sale, not
    project PDF qty forward."""
    s = _FakeStore([_march_pdf_month()])
    rows = current_holdings(s, daily_store_post_pdf_sale)
    assert rows == []


# ─── unit: cold path ─────────────────────────────────────────────────────


def test_current_holdings_cold_falls_back_to_pdf_month_end(daily_store_empty):
    """Empty daily store → return PDF month-end rows (no repriced_at marker)."""
    s = _FakeStore([_march_pdf_month()])
    rows = current_holdings(s, daily_store_empty)

    assert len(rows) == 1
    row = rows[0]
    assert row["code"] == "2330"
    # Values come straight from the PDF month-end (no repricing).
    assert row["mkt_value_twd"] == pytest.approx(1_050_000)
    assert row["unrealized_pnl_twd"] == pytest.approx(250_000)
    # No repriced_at marker — proves we took the cold branch.
    assert "repriced_at" not in row or row.get("repriced_at") is None


def test_current_holdings_no_months_returns_empty(daily_store_empty):
    s = _FakeStore([])
    assert current_holdings(s, daily_store_empty) == []


# ─── unit: holdings_for_month (historical view) ──────────────────────────


def test_holdings_for_month_normalizes_pdf_shape():
    rows = holdings_for_month(_march_pdf_month())
    assert len(rows) == 1
    row = rows[0]
    assert row["venue"] == "TW"
    assert row["code"] == "2330"
    assert row["qty"] == 1000
    assert row["mkt_value_twd"] == 1_050_000


# ─── integration: Bug 3 cross-endpoint acceptance ────────────────────────


@pytest.fixture()
def portfolio_held_in_march(tmp_path: Path) -> Path:
    """Portfolio.json with 1000 shares of 2330 held at end of March."""
    p = tmp_path / "portfolio.json"
    payload = {
        "months": [{
            "month": "2026-03",
            "equity_twd": 1_050_000,
            "external_flow_twd": 0,
            "fx_usd_twd": 31.0,
            "tw_market_value_twd": 1_050_000,
            "foreign_market_value_twd": 0,
            "bank_twd": 0,
            "bank_usd_in_twd": 0,
            "tw": {
                "holdings": [{
                    "code": "2330", "name": "TSMC",
                    "qty": 1000, "avg_cost": 800.0, "cost": 800_000,
                    "ref_price": 1050.0, "mkt_value": 1_050_000,
                    "unrealized_pnl": 250_000, "type": "現股",
                }],
                "trades": [], "rebates": [],
            },
            "foreign": {"holdings": [], "trades": []},
            "bank": {
                "tx_twd": [
                    {"date": "2026-03-15", "summary": "薪資", "amount": 100_000,
                     "balance": 100_000, "signed_amount": 100_000, "category": "salary"},
                    {"date": "2026-03-20", "summary": "股票款", "amount": 800_000,
                     "balance": 0, "signed_amount": -800_000, "category": "stock_settle_tw"},
                ],
                "tx_foreign": [],
                "fx": {"USD": 31.0},
            },
            "period_return": 0.0,
            "cum_twr": 0.0,
        }],
        "summary": {
            "kpis": {
                "real_now_twd": 1_050_000,
                "counterfactual_twd": 800_000,
                "profit_twd": 250_000,
            },
            "all_trades": [],
            "by_ticker": {
                "2330": {"code": "2330", "name": "TSMC", "trades": [],
                         "fees_twd": 0, "tax_twd": 0},
            },
            "venue_flows_twd": [],
            "cumulative_flows": {},
            "dividends": [],
        },
    }
    p.write_text(json.dumps(payload))
    return p


@pytest.fixture()
def app_post_pdf_sale(tmp_path, monkeypatch, portfolio_held_in_march):
    """The Bug 3 production state: PDF says 1000 shares, positions_daily
    on April 22 says 0 (user sold all). Pre-refactor: /api/holdings/current
    showed 1000 shares (PDF projected). /api/summary showed equity_twd 0
    (overlay-aware). They disagreed by 1_100_000+ TWD."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "post_sale.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app

    app = create_app(portfolio_held_in_march)
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        # April 22: position fully sold. portfolio_daily logs the
        # post-sale equity (0); positions_daily is empty for that date.
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
            "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
            ("2026-04-22", 0.0, 31.5, 0, 1),
        )
        # Symbol_market still resolved (would have been used during
        # fetch); positions_daily is empty by intent.
        conn.execute(
            "INSERT INTO symbol_market(symbol, market, resolved_at, "
            "last_verified_at) VALUES (?, ?, ?, ?)",
            ("2330", "TW", "2026-04-22T16:00:00Z", "2026-04-22T16:00:00Z"),
        )
    return app


def _json(client, url):
    r = client.get(url)
    assert r.status_code == 200, f"{url} → {r.status_code}: {r.data}"
    return r.get_json()["data"]


def test_bug3_post_pdf_sale_endpoints_all_reflect_sale(app_post_pdf_sale):
    """The Bug 3 acceptance test.

    Scenario: user held 1000 shares of 2330 at end of March (PDF-of-
    record). User sold all 1000 in April. positions_daily has no 2330
    row on April 22. The pre-refactor system disagreed:
      - /api/holdings/current showed 1000 shares (PDF projected forward)
      - /api/summary KPI showed today_mv=0 (overlay-aware total)
      - /api/holdings/sectors still allocated 100% to TSMC (PDF projected)
      - /api/tax showed 2330 with non-zero unrealized (PDF projected)

    Post-refactor (single source via current_holdings), all four
    endpoints reflect the sale identically.
    """
    client = app_post_pdf_sale.test_client()
    holdings = _json(client, "/api/holdings/current")
    summary = _json(client, "/api/summary")
    sectors = _json(client, "/api/holdings/sectors")
    tax = _json(client, "/api/tax")

    # /holdings: 2330 must NOT appear (or appear with qty=0).
    holding_codes = {h["code"] for h in holdings["holdings"]}
    assert "2330" not in holding_codes
    assert holdings["total_mv_twd"] == pytest.approx(0)

    # /summary KPI: real_now_twd reflects the post-sale total (no holdings).
    assert summary["kpis"]["real_now_twd"] == pytest.approx(0)
    assert summary["kpis"]["repriced_holdings_count"] == 0

    # /sectors: empty allocation (no holdings).
    assert sectors == []

    # /tax: 2330 may still appear with realized P&L lineage from
    # by_ticker, but its current_qty must be 0 and unrealized must be 0.
    by_code = {r["code"]: r for r in tax["by_ticker"]}
    if "2330" in by_code:
        assert by_code["2330"]["current_qty"] == 0
        assert by_code["2330"]["unrealized_pnl_twd"] == pytest.approx(0)
    assert tax["totals"]["unrealized_pnl_twd"] == pytest.approx(0)


def test_bug3_post_pdf_held_endpoints_all_use_overlay_price(
    portfolio_held_in_march, tmp_path, monkeypatch,
):
    """Inverse scenario: same PDF (1000 shares of 2330), but positions_daily
    on April 22 says still 1000 shares at TODAY's close 1100 (vs PDF's 1050).
    All endpoints must show the warm-path total (1_100_000), not PDF (1_050_000)."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "post_held.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app

    app = create_app(portfolio_held_in_march)
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
            "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
            ("2026-04-22", 1_100_000.0, 31.5, 1, 0),
        )
        conn.execute(
            "INSERT INTO positions_daily(date, symbol, qty, cost_local, "
            "mv_local, mv_twd, type, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("2026-04-22", "2330", 1000, 800_000, 1_100_000, 1_100_000, "現股", "pdf"),
        )
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, "
            "fetched_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("2026-04-22", "2330", 1100.0, "TWD", "yfinance", "2026-04-22T16:00:00Z"),
        )
        conn.execute(
            "INSERT INTO symbol_market(symbol, market, resolved_at, "
            "last_verified_at) VALUES (?, ?, ?, ?)",
            ("2330", "TW", "2026-04-22T16:00:00Z", "2026-04-22T16:00:00Z"),
        )

    client = app.test_client()
    holdings = _json(client, "/api/holdings/current")
    summary = _json(client, "/api/summary")
    tax = _json(client, "/api/tax")

    # All three agree on today's mv (1_100_000) and unrealized (300_000).
    assert holdings["total_mv_twd"] == pytest.approx(1_100_000)
    assert holdings["total_upnl_twd"] == pytest.approx(300_000)
    assert summary["kpis"]["real_now_twd"] == pytest.approx(1_100_000)
    assert summary["kpis"]["unrealized_pnl_twd"] == pytest.approx(300_000)
    assert tax["totals"]["unrealized_pnl_twd"] == pytest.approx(300_000)
