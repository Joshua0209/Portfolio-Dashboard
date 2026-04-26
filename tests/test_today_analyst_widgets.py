"""Analyst-grade /today widgets — period-returns, drawdown, risk metrics,
calendar heatmap.

These endpoints are pure derivations from portfolio_daily; they don't
fetch externally. Each test seeds a small synthetic equity series and
asserts the math.
"""
from __future__ import annotations

import math

import pytest

from app.daily_store import DailyStore


@pytest.fixture()
def app(tmp_path, monkeypatch, empty_portfolio_json):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "today_widgets.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    return create_app(empty_portfolio_json)


def _seed(app, rows):
    """rows is [(date, equity_twd), ...]"""
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.executemany(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, n_positions, has_overlay) "
            "VALUES (?, ?, ?, ?, ?)",
            [(d, eq, 31.0, 5, 0) for d, eq in rows],
        )


# --- Period returns ---------------------------------------------------------


def test_period_returns_empty_envelope_when_no_data(app):
    r = app.test_client().get("/api/today/period-returns")
    assert r.status_code == 200
    assert r.get_json()["data"]["empty"] is True


def test_period_returns_computes_mtd_qtd_ytd(app):
    # Latest date 2026-04-15 (Q2 starts 04-01, year starts 01-01)
    _seed(app, [
        ("2025-12-30", 1_000_000.0),  # inception anchor
        ("2026-01-02", 1_010_000.0),  # YTD anchor
        ("2026-04-01", 1_050_000.0),  # QTD anchor
        ("2026-04-15", 1_100_000.0),  # latest
    ])
    body = app.test_client().get("/api/today/period-returns").get_json()["data"]
    windows = {w["label"]: w for w in body["windows"]}
    # MTD: from 2026-04-01 (1.05M → 1.10M) ≈ +4.76%
    assert windows["MTD"]["delta_pct"] == pytest.approx(4.7619, rel=1e-3)
    # YTD: from 2026-01-02 (1.01M → 1.10M) ≈ +8.91%
    assert windows["YTD"]["delta_pct"] == pytest.approx(8.9109, rel=1e-3)
    # Inception: from 2025-12-30 (1.00M → 1.10M) = +10.00%
    assert windows["Inception"]["delta_pct"] == pytest.approx(10.0, rel=1e-3)


# --- Drawdown ---------------------------------------------------------------


def test_drawdown_empty_when_no_data(app):
    r = app.test_client().get("/api/today/drawdown").get_json()["data"]
    assert r["empty"] is True


def test_drawdown_recovers_to_zero_on_new_high(app):
    _seed(app, [
        ("2026-01-02", 1_000_000.0),
        ("2026-01-03",   900_000.0),  # -10% from peak
        ("2026-01-04",   850_000.0),  # -15% from peak (max DD here)
        ("2026-01-05",   950_000.0),  # -5%
        ("2026-01-06", 1_100_000.0),  # new high → 0%
    ])
    body = app.test_client().get("/api/today/drawdown").get_json()["data"]
    points = body["points"]
    assert len(points) == 5
    assert points[0]["drawdown_pct"] == 0.0
    assert points[2]["drawdown_pct"] == pytest.approx(-15.0, rel=1e-6)
    assert points[-1]["drawdown_pct"] == 0.0
    assert body["max_dd"] == pytest.approx(-15.0, rel=1e-6)
    assert body["max_dd_date"] == "2026-01-04"
    assert body["current_dd"] == 0.0


# --- Risk metrics -----------------------------------------------------------


def test_risk_metrics_empty_with_one_row(app):
    _seed(app, [("2026-01-02", 1_000_000.0)])
    r = app.test_client().get("/api/today/risk-metrics").get_json()["data"]
    assert r["empty"] is True


def test_risk_metrics_basic_invariants(app):
    # Strictly monotonic up: vol > 0, hit_rate = 100%, no drawdown.
    rows = [(f"2026-01-{i:02d}", 1_000_000.0 * (1 + 0.001 * i)) for i in range(2, 32)]
    _seed(app, rows)
    r = app.test_client().get("/api/today/risk-metrics").get_json()["data"]
    assert r["empty"] is False
    assert r["n_days"] == 29  # 30 rows → 29 returns
    assert r["hit_rate_pct"] == pytest.approx(100.0)
    assert r["max_drawdown_pct"] == pytest.approx(0.0, abs=1e-6)
    assert r["ann_vol_pct"] >= 0
    assert r["best_day_pct"] >= r["worst_day_pct"]
    # Sortino is undefined when there are no negative days → None
    assert r["sortino"] is None


# --- Calendar heatmap -------------------------------------------------------


def test_calendar_empty_when_one_row(app):
    _seed(app, [("2026-04-01", 1_000_000.0)])
    r = app.test_client().get("/api/today/calendar").get_json()["data"]
    assert r["empty"] is True
    assert r["cells"] == []


def test_calendar_emits_one_cell_per_priced_day_after_first(app):
    _seed(app, [
        ("2026-04-01", 1_000_000.0),  # Wednesday
        ("2026-04-02", 1_010_000.0),  # Thursday — return +1%
        ("2026-04-03",   990_000.0),  # Friday — return ≈ -1.98%
    ])
    body = app.test_client().get("/api/today/calendar").get_json()["data"]
    assert body["empty"] is False
    cells = body["cells"]
    assert len(cells) == 2
    assert cells[0]["date"] == "2026-04-02"
    assert cells[0]["weekday"] == 3  # 0=Mon, so Thu=3
    assert cells[0]["return_pct"] == pytest.approx(1.0, rel=1e-6)
    assert cells[1]["return_pct"] == pytest.approx(-1.9802, rel=1e-3)
    # Months: only April 2026
    assert body["months"] == [{"year": 2026, "month": 4, "label": "Apr 2026"}]


def test_calendar_groups_by_year_month_in_order(app):
    _seed(app, [
        ("2026-03-30", 1_000_000.0),
        ("2026-03-31", 1_005_000.0),
        ("2026-04-01", 1_010_000.0),
        ("2026-04-02", 1_007_000.0),
    ])
    body = app.test_client().get("/api/today/calendar").get_json()["data"]
    assert [(m["year"], m["month"]) for m in body["months"]] == [(2026, 3), (2026, 4)]
