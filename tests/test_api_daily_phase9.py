"""Phase 9 — /api/daily/* warming-up behavior.

Endpoints that depend on the daily layer must:
  - return 202 + progress envelope when state == INITIALIZING,
  - return 503 + error envelope when state == FAILED,
  - serve normally when state == READY.

The /api/summary monthly endpoint is NOT decorated — it must keep
returning 200 throughout cold-start so existing pages stay usable.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app import backfill_state, create_app
from app.daily_store import DailyStore


@pytest.fixture(autouse=True)
def _reset_state():
    backfill_state.get().reset()
    yield
    backfill_state.get().reset()


@pytest.fixture()
def app(tmp_path: Path, monkeypatch, empty_portfolio_json: Path):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "phase9_api.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    return create_app(empty_portfolio_json)


@pytest.fixture()
def client(app):
    return app.test_client()


def _seed_one_row(app):
    """Drop a single portfolio_daily row so endpoints have something to
    return when state == READY."""
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.execute(
            """
            INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, n_positions, has_overlay)
            VALUES ('2026-04-25', 1000.0, 0.032, 1, 0)
            """
        )


def test_daily_equity_returns_202_when_initializing(client):
    state = backfill_state.get()
    state.mark_initializing()
    state.set_progress(total=37, done=12, current="2330")

    resp = client.get("/api/daily/equity")
    assert resp.status_code == 202
    body = resp.get_json()
    assert body["ok"] is True
    assert body["data"]["state"] == "INITIALIZING"
    assert body["data"]["progress"]["total"] == 37
    assert body["data"]["progress"]["done"] == 12
    assert body["data"]["progress"]["current"] == "2330"


def test_daily_equity_returns_503_when_failed(client):
    backfill_state.get().mark_failed("yfinance 503 fire drill")

    resp = client.get("/api/daily/equity")
    assert resp.status_code == 503
    body = resp.get_json()
    assert body["ok"] is False
    assert "yfinance 503 fire drill" in body["error"]


def test_daily_equity_serves_normally_when_ready(client, app):
    _seed_one_row(app)
    backfill_state.get().mark_ready()

    resp = client.get("/api/daily/equity")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["ok"] is True
    assert body["data"]["empty"] is False
    assert len(body["data"]["points"]) == 1


def test_daily_prices_endpoint_is_warmed_up(client):
    """/api/daily/prices/<symbol> shares the same gate."""
    backfill_state.get().mark_initializing()
    resp = client.get("/api/daily/prices/2330")
    assert resp.status_code == 202


def test_summary_endpoint_serves_throughout_warmup(client):
    """/api/summary (monthly default) must NOT be gated — Phase 9 only
    blocks /api/daily/*."""
    backfill_state.get().mark_initializing()
    resp = client.get("/api/summary")
    assert resp.status_code == 200


def test_health_returns_state_machine_fields(client):
    state = backfill_state.get()
    state.mark_initializing()
    state.set_progress(total=37, done=5, current="2330")

    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["data"]["daily_state"] == "INITIALIZING"
    assert body["data"]["daily_progress"]["total"] == 37
    assert body["data"]["daily_progress"]["done"] == 5
    assert body["data"]["daily_progress"]["current"] == "2330"
    assert body["data"]["daily_error"] is None


def test_health_reports_failed_with_error(client):
    backfill_state.get().mark_failed("yfinance rate limited")
    resp = client.get("/api/health")
    body = resp.get_json()
    assert body["data"]["daily_state"] == "FAILED"
    assert "yfinance rate limited" in body["data"]["daily_error"]
