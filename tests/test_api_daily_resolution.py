"""Tests for ?resolution=daily branches across non-/today blueprints.

Per blueprint we verify three properties:
  1. Default (no resolution param) returns the existing monthly shape.
  2. ?resolution=daily with empty daily store falls back gracefully.
  3. ?resolution=daily with seeded daily store returns daily-shape data.

Also: /api/tax and /api/summary kpis must use today's prices unconditionally
(no flag) when the daily store has rows for the held tickers.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.daily_store import DailyStore


# ─────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def portfolio_with_holdings(tmp_path: Path) -> Path:
    """Portfolio with two months and one TW holding so unrealized math is
    exercisable. The holding's month-end ref_price differs from the daily
    store's latest close so we can detect the override."""
    p = tmp_path / "portfolio.json"
    payload = {
        "months": [
            {
                "month": "2026-03",
                "equity_twd": 1_000_000,
                "external_flow_twd": 0,
                "fx_usd_twd": 31.0,
                "tw_market_value_twd": 1_000_000,
                "foreign_market_value_twd": 0,
                "bank_twd": 0,
                "bank_usd_in_twd": 0,
                "tw": {
                    "holdings": [
                        {
                            "code": "2330",
                            "name": "TSMC",
                            "qty": 1000,
                            "avg_cost": 800.0,
                            "cost": 800_000,
                            "ref_price": 1000.0,
                            "mkt_value": 1_000_000,
                            "unrealized_pnl": 200_000,
                            "type": "現股",
                        }
                    ],
                    "trades": [],
                    "rebates": [],
                },
                "foreign": {"holdings": [], "trades": []},
                "bank": {
                    "tx_twd": [
                        {
                            "date": "2026-03-15",
                            "summary": "薪資入帳",
                            "amount": 100_000,
                            "balance": 100_000,
                            "signed_amount": 100_000,
                            "category": "salary",
                        },
                        {
                            "date": "2026-03-20",
                            "summary": "股票款",
                            "amount": 800_000,
                            "balance": 0,
                            "signed_amount": -800_000,
                            "category": "stock_settle_tw",
                        },
                    ],
                    "tx_foreign": [],
                    "fx": {"USD": 31.0},
                },
                "period_return": 0.0,
                "cum_twr": 0.0,
            },
            {
                "month": "2026-04",
                "equity_twd": 1_050_000,
                "external_flow_twd": 0,
                "fx_usd_twd": 31.5,
                "tw_market_value_twd": 1_050_000,
                "foreign_market_value_twd": 0,
                "bank_twd": 0,
                "bank_usd_in_twd": 0,
                "tw": {
                    "holdings": [
                        {
                            "code": "2330",
                            "name": "TSMC",
                            "qty": 1000,
                            "avg_cost": 800.0,
                            "cost": 800_000,
                            "ref_price": 1050.0,
                            "mkt_value": 1_050_000,
                            "unrealized_pnl": 250_000,
                            "type": "現股",
                        }
                    ],
                    "trades": [],
                    "rebates": [],
                },
                "foreign": {"holdings": [], "trades": []},
                "bank": {
                    "tx_twd": [
                        {
                            "date": "2026-04-15",
                            "summary": "薪資入帳",
                            "amount": 100_000,
                            "balance": 100_000,
                            "signed_amount": 100_000,
                            "category": "salary",
                        }
                    ],
                    "tx_foreign": [],
                    "fx": {"USD": 31.5},
                },
                "period_return": 0.05,
                "cum_twr": 0.05,
            },
        ],
        "summary": {
            "kpis": {
                "real_now_twd": 1_050_000,
                "counterfactual_twd": 800_000,
                "profit_twd": 250_000,
            },
            "all_trades": [],
            "by_ticker": {
                "2330": {
                    "code": "2330",
                    "name": "TSMC",
                    "trades": [],
                    "fees_twd": 0,
                    "tax_twd": 0,
                }
            },
            "venue_flows_twd": [],
            "cumulative_flows": {},
            "dividends": [],
        },
    }
    p.write_text(json.dumps(payload))
    return p


@pytest.fixture()
def app_empty_daily(tmp_path, monkeypatch, portfolio_with_holdings):
    """App with portfolio but EMPTY daily store — exercises fallbacks."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "empty.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    return create_app(portfolio_with_holdings)


@pytest.fixture()
def app_seeded_daily(tmp_path, monkeypatch, portfolio_with_holdings):
    """App with portfolio + seeded daily store. TSMC's daily close is
    1100 (vs month-end 1050) — repricing must flow through to /tax & /."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "seeded.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    app = create_app(portfolio_with_holdings)
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.executemany(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
            "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
            [
                ("2026-04-22", 1_040_000.0, 31.5, 1, 0),
                ("2026-04-23", 1_080_000.0, 31.5, 1, 0),
                ("2026-04-24", 1_100_000.0, 31.5, 1, 0),
            ],
        )
        conn.executemany(
            "INSERT INTO positions_daily(date, symbol, qty, cost_local, "
            "mv_local, mv_twd, type, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                ("2026-04-22", "2330", 1000, 800_000, 1_040_000, 1_040_000, "現股", "pdf"),
                ("2026-04-23", "2330", 1000, 800_000, 1_080_000, 1_080_000, "現股", "pdf"),
                ("2026-04-24", "2330", 1000, 800_000, 1_100_000, 1_100_000, "現股", "pdf"),
            ],
        )
        conn.executemany(
            "INSERT INTO prices(date, symbol, close, currency, source, fetched_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                ("2026-04-22", "2330", 1040.0, "TWD", "yfinance", "2026-04-22T16:00:00Z"),
                ("2026-04-23", "2330", 1080.0, "TWD", "yfinance", "2026-04-23T16:00:00Z"),
                ("2026-04-24", "2330", 1100.0, "TWD", "yfinance", "2026-04-24T16:00:00Z"),
            ],
        )
        conn.executemany(
            "INSERT INTO fx_daily(date, ccy, rate_to_twd, source, fetched_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [
                ("2026-04-22", "USD", 31.50, "yfinance", "2026-04-22T16:00:00Z"),
                ("2026-04-23", "USD", 31.55, "yfinance", "2026-04-23T16:00:00Z"),
                ("2026-04-24", "USD", 31.60, "yfinance", "2026-04-24T16:00:00Z"),
            ],
        )
        conn.executemany(
            "INSERT INTO symbol_market(symbol, market, resolved_at, last_verified_at) "
            "VALUES (?, ?, ?, ?)",
            [("2330", "TW", "2026-04-22T16:00:00Z", "2026-04-22T16:00:00Z")],
        )
    return app


def _json(client, url: str) -> dict:
    r = client.get(url)
    assert r.status_code == 200, f"{url} → {r.status_code}: {r.data}"
    return r.get_json()["data"]


# ─────────────────────────────────────────────────────────────────────────
# /api/tax — unconditional today's-prices override
# ─────────────────────────────────────────────────────────────────────────


def test_tax_uses_todays_close_unconditionally(app_seeded_daily):
    """No ?resolution flag — /tax must always reflect today's prices."""
    data = _json(app_seeded_daily.test_client(), "/api/tax")
    by_code = {r["code"]: r for r in data["by_ticker"]}
    # 2330: month-end ref was 1050, daily close is 1100. unrealized must
    # reflect 1100, not 1050.
    tsmc = by_code["2330"]
    # qty=1000, avg_cost=800 → unrealized_at_today = 1000 * (1100 - 800) = 300_000
    assert tsmc["unrealized_pnl_twd"] == pytest.approx(300_000)


def test_tax_falls_back_to_month_end_when_no_daily(app_empty_daily):
    """No daily prices → fall back to PDF month-end (250_000)."""
    data = _json(app_empty_daily.test_client(), "/api/tax")
    by_code = {r["code"]: r for r in data["by_ticker"]}
    assert by_code["2330"]["unrealized_pnl_twd"] == pytest.approx(250_000)


# ─────────────────────────────────────────────────────────────────────────
# /api/summary — KPI hero reflects today's prices
# ─────────────────────────────────────────────────────────────────────────


def test_summary_kpis_repriced_to_today(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/summary")
    kpis = data["kpis"]
    # repriced real_now_twd = mv_today (1_100_000) + bank_cash (0) = 1_100_000
    assert kpis["real_now_twd"] == pytest.approx(1_100_000)
    # profit_twd = real_now - counterfactual = 1_100_000 - 800_000 = 300_000
    assert kpis["profit_twd"] == pytest.approx(300_000)
    assert kpis["repriced_holdings_count"] == 1


def test_summary_kpis_untouched_without_daily(app_empty_daily):
    data = _json(app_empty_daily.test_client(), "/api/summary")
    kpis = data["kpis"]
    # Stays at the parser-provided 1_050_000 / 250_000.
    assert kpis["real_now_twd"] == pytest.approx(1_050_000)
    assert kpis["profit_twd"] == pytest.approx(250_000)
    assert "repriced_holdings_count" not in kpis


def test_summary_daily_curve_carries_cum_twr(app_seeded_daily):
    """Regression: daily branch was returning curve rows without cum_twr,
    so the Overview chart's cumulative TWR line plotted all 0%. The fix
    monthly-anchors cum_twr per row (see _monthly_anchored_cum)."""
    data = _json(
        app_seeded_daily.test_client(), "/api/summary?resolution=daily"
    )
    assert data["resolution"] == "daily"
    curve = data["equity_curve"]
    assert curve, "expected at least one daily row"
    for row in curve:
        assert "cum_twr" in row, "every daily row must carry cum_twr"
    # Anchors are 0% (2026-03-31) and 5% (2026-04-30). Daily dates Apr
    # 22-24 fall between them, so each cum_twr must interpolate strictly
    # between the surrounding monthly anchors — and crucially must NOT
    # be zero (the bug this guards against). Monotonic in date because
    # both anchors are non-decreasing.
    cum_values = [row["cum_twr"] for row in curve]
    assert all(0 < v < 0.05 for v in cum_values)
    assert cum_values == sorted(cum_values)


# ─────────────────────────────────────────────────────────────────────────
# /api/holdings/current — same repricer Phase 7 used for /tax + /summary
# ─────────────────────────────────────────────────────────────────────────


def test_holdings_current_repriced_to_today(app_seeded_daily):
    """Holdings page must agree with /summary + /tax — all three pull
    from reprice_holdings_with_daily(), so the unrealized totals match."""
    holdings = _json(app_seeded_daily.test_client(), "/api/holdings/current")
    summary = _json(app_seeded_daily.test_client(), "/api/summary")
    tax = _json(app_seeded_daily.test_client(), "/api/tax")
    # 2330: 1000 shares × (1100 − 800) = 300_000 unrealized
    assert holdings["total_upnl_twd"] == pytest.approx(300_000)
    assert holdings["repriced_holdings_count"] == 1
    # All three surfaces agree on unrealized
    assert holdings["total_upnl_twd"] == pytest.approx(
        summary["kpis"]["unrealized_pnl_twd"]
    )
    assert holdings["total_upnl_twd"] == pytest.approx(
        tax["totals"]["unrealized_pnl_twd"]
    )


def test_holdings_current_falls_back_to_month_end_without_daily(app_empty_daily):
    holdings = _json(app_empty_daily.test_client(), "/api/holdings/current")
    # Empty daily store → keeps the parser's month-end 250_000 unrealized.
    assert holdings["total_upnl_twd"] == pytest.approx(250_000)


# ─────────────────────────────────────────────────────────────────────────
# /api/holdings/timeline
# ─────────────────────────────────────────────────────────────────────────


def test_holdings_timeline_default_returns_monthly(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/holdings/timeline")
    assert data["resolution"] == "monthly"
    assert all("month" in r for r in data["rows"])


def test_holdings_timeline_daily_returns_daily(app_seeded_daily):
    data = _json(
        app_seeded_daily.test_client(), "/api/holdings/timeline?resolution=daily"
    )
    assert data["resolution"] == "daily"
    assert len(data["rows"]) == 3
    assert all("date" in r for r in data["rows"])


def test_holdings_timeline_daily_falls_back_when_empty(app_empty_daily):
    data = _json(
        app_empty_daily.test_client(), "/api/holdings/timeline?resolution=daily"
    )
    assert data["resolution"] == "monthly"  # graceful fallback


# ─────────────────────────────────────────────────────────────────────────
# /api/performance/timeseries
# ─────────────────────────────────────────────────────────────────────────


def test_performance_default_is_monthly(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/performance/timeseries")
    # No 'resolution' field on the legacy monthly response.
    assert "resolution" not in data or data.get("resolution") != "daily"
    assert all("month" in r for r in data["monthly"])


def test_performance_daily_returns_daily_rows(app_seeded_daily):
    data = _json(
        app_seeded_daily.test_client(),
        "/api/performance/timeseries?resolution=daily",
    )
    assert data["resolution"] == "daily"
    # Method now reflects the chosen monthly weighting (default
    # day_weighted) so headline KPIs stay consistent with /api/summary.
    # The daily series itself still chains via daily_twr; only the KPI
    # source changed.
    assert data["method"] in {"day_weighted", "mid_month", "eom"}
    assert all("date" in r for r in data["monthly"])
    assert len(data["monthly"]) == 3
    # Day 1 forced to 0% per daily_twr contract.
    assert data["monthly"][0]["period_return"] == 0.0
    # Headline KPIs come from the monthly source-of-truth — same Sharpe
    # function that /overview uses, no daily double-scaling.
    assert "sharpe_annualized" in data
    assert "sortino_annualized" in data
    assert "calmar" in data
    assert "cagr" in data


def test_performance_daily_falls_back_when_empty(app_empty_daily):
    """Empty daily store → daily branch falls through to monthly."""
    data = _json(
        app_empty_daily.test_client(),
        "/api/performance/timeseries?resolution=daily",
    )
    assert all("month" in r for r in data["monthly"])


# ─────────────────────────────────────────────────────────────────────────
# /api/risk
# ─────────────────────────────────────────────────────────────────────────


def test_risk_default_drawdown_keyed_by_month(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/risk")
    assert data["resolution"] == "monthly"
    if data["drawdown_curve"]:
        assert "month" in data["drawdown_curve"][0]


def test_risk_daily_drawdown_keyed_by_date(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/risk?resolution=daily")
    assert data["resolution"] == "daily"
    assert all("date" in p for p in data["drawdown_curve"])


def test_risk_daily_falls_back_when_empty(app_empty_daily):
    data = _json(app_empty_daily.test_client(), "/api/risk?resolution=daily")
    assert data["resolution"] == "monthly"


# ─────────────────────────────────────────────────────────────────────────
# /api/fx
# ─────────────────────────────────────────────────────────────────────────


def test_fx_default_rate_curve_monthly(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/fx")
    assert data["resolution"] == "monthly"
    assert all("month" in r for r in data["rate_curve"])
    assert "fx_pnl_daily" not in data


def test_fx_daily_rate_curve_and_pnl(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/fx?resolution=daily")
    assert data["resolution"] == "daily"
    assert all("date" in r for r in data["rate_curve"])
    assert "fx_pnl_daily" in data
    # No USD-denominated holdings in fixture → fx_pnl_daily contribution is 0
    assert data["fx_pnl_daily"]["contribution_twd"] == 0


# ─────────────────────────────────────────────────────────────────────────
# /api/cashflows/monthly — monthly + daily coexist on this endpoint
# ─────────────────────────────────────────────────────────────────────────


def test_cashflows_monthly_default_no_daily_field(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/cashflows/monthly")
    # Default response is the legacy list — no daily array attached.
    assert isinstance(data, list)


def test_cashflows_monthly_with_daily_param_returns_legacy_list(app_seeded_daily):
    """The Phase 6 daily-flow chart was retired in favor of the daily
    Real-vs-Counterfactual chart (now on /cumulative). /monthly is back
    to its original list shape regardless of ?resolution."""
    data = _json(
        app_seeded_daily.test_client(), "/api/cashflows/monthly?resolution=daily"
    )
    assert isinstance(data, list)


def test_cashflows_cumulative_daily_adds_real_and_cf_curves(app_seeded_daily):
    """?resolution=daily on /cumulative attaches per-day real_curve_daily
    + counterfactual_curve_daily so the Real-vs-Counterfactual chart
    can render at daily precision."""
    data = _json(
        app_seeded_daily.test_client(), "/api/cashflows/cumulative?resolution=daily"
    )
    assert data.get("resolution") == "daily"
    assert isinstance(data.get("real_curve_daily"), list)
    assert len(data["real_curve_daily"]) > 0
    assert all("date" in p and "value" in p for p in data["real_curve_daily"])
    cf = data["counterfactual_curve_daily"]
    assert isinstance(cf, list) and len(cf) == len(data["real_curve_daily"])
    # Counterfactual is monotonic (non-decreasing as flows accumulate).
    values = [p["value"] for p in cf]
    assert values == sorted(values)
    # Two salary inflows of 100k each → final cumulative ≥ 200k.
    assert values[-1] >= 200_000


def test_cashflows_cumulative_default_no_daily_curves(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/cashflows/cumulative")
    assert "real_curve_daily" not in data
    assert "counterfactual_curve_daily" not in data


def test_cashflows_daily_excludes_stock_settle(app_seeded_daily):
    """stock_settle_tw must not appear in the daily counterfactual flow."""
    data = _json(
        app_seeded_daily.test_client(), "/api/cashflows/cumulative?resolution=daily"
    )
    cf = data["counterfactual_curve_daily"]
    # Counterfactual is monotonic and rises only on real external flows;
    # stock settlements are internal rotations and must be filtered out.
    by_date = {p["date"]: p["value"] for p in cf}
    if "2026-03-20" in by_date and "2026-03-19" in by_date:
        assert by_date["2026-03-20"] == by_date["2026-03-19"]


# ─────────────────────────────────────────────────────────────────────────
# /api/benchmarks/compare
# ─────────────────────────────────────────────────────────────────────────


def test_benchmarks_compare_default_no_daily_curve(app_seeded_daily):
    data = _json(app_seeded_daily.test_client(), "/api/benchmarks/compare")
    assert "portfolio_daily_curve" not in data


def test_benchmarks_compare_daily_adds_portfolio_daily_curve(app_seeded_daily):
    data = _json(
        app_seeded_daily.test_client(),
        "/api/benchmarks/compare?resolution=daily",
    )
    assert data["resolution"] == "daily"
    assert "portfolio_daily_curve" in data
    assert all("date" in r for r in data["portfolio_daily_curve"])
