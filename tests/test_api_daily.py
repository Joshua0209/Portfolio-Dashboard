"""Phase 4 acceptance tests for /api/daily/equity and the
?resolution=daily branch on /api/summary."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app import create_app
from app.daily_store import DailyStore


def _seed_daily_data(store: DailyStore) -> None:
    """Insert a tiny portfolio_daily curve for testing."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with store.connect_rw() as conn:
        for d, eq in [("2025-08-15", 100_000.0), ("2025-09-10", 105_000.0),
                      ("2026-03-31", 130_000.0)]:
            conn.execute(
                "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, "
                "n_positions, has_overlay) VALUES (?, ?, ?, ?, ?)",
                (d, eq, 32.0, 3, 0),
            )


@pytest.fixture()
def app(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "dashboard.db"))
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({"months": [], "summary": {}}), encoding="utf-8")
    return create_app(data_path=portfolio_path)


@pytest.fixture()
def app_with_daily_data(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "dashboard.db"))
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({
        "months": [
            {"month": "2025-08", "equity_twd": 100_000, "tw_market_value_twd": 100_000,
             "foreign_market_value_twd": 0, "bank_twd": 0, "bank_usd_in_twd": 0,
             "external_flow_twd": 0, "cum_twr": 1.0, "period_return": 0.0},
            {"month": "2026-03", "equity_twd": 130_000, "tw_market_value_twd": 130_000,
             "foreign_market_value_twd": 0, "bank_twd": 0, "bank_usd_in_twd": 0,
             "external_flow_twd": 0, "cum_twr": 1.30, "period_return": 0.05},
        ],
        "summary": {
            "kpis": {"as_of": "2026-03", "real_now_twd": 130_000, "profit_twd": 30_000,
                     "counterfactual_twd": 100_000, "fx_usd_twd": 32.0},
        },
    }), encoding="utf-8")
    a = create_app(data_path=portfolio_path)
    _seed_daily_data(a.extensions["daily_store"])
    return a


# --- /api/daily/equity ---------------------------------------------------


def test_daily_equity_returns_initializing_on_fresh_db(app) -> None:
    """Empty store → 202 INITIALIZING (require_ready_or_warming).
    Returning empty rows at HTTP 200 would silently let stale-or-never-
    populated state look like 'data is fine, just empty'."""
    client = app.test_client()
    r = client.get("/api/daily/equity")
    assert r.status_code == 202
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["state"] == "INITIALIZING"


def test_daily_equity_returns_seeded_curve(app_with_daily_data) -> None:
    client = app_with_daily_data.test_client()
    r = client.get("/api/daily/equity")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"]["empty"] is False
    pts = body["data"]["points"]
    assert len(pts) == 3
    assert pts[0]["date"] == "2025-08-15"
    assert pts[0]["equity_twd"] == 100_000.0
    assert pts[-1]["equity_twd"] == 130_000.0


def test_daily_equity_filters_by_start_end(app_with_daily_data) -> None:
    client = app_with_daily_data.test_client()
    r = client.get("/api/daily/equity?start=2025-09-01&end=2025-12-31")
    assert r.status_code == 200
    pts = r.get_json()["data"]["points"]
    assert len(pts) == 1
    assert pts[0]["date"] == "2025-09-10"


# --- /api/summary?resolution=daily ---------------------------------------


def test_summary_no_param_is_byte_identical(app_with_daily_data) -> None:
    """Phase 4 hard requirement: existing /api/summary unchanged."""
    client = app_with_daily_data.test_client()
    r1 = client.get("/api/summary")
    r2 = client.get("/api/summary?resolution=monthly")
    assert r1.status_code == r2.status_code == 200
    assert r1.data == r2.data, "monthly default must be byte-identical to no-param"


def test_summary_resolution_daily_returns_daily_curve(app_with_daily_data) -> None:
    client = app_with_daily_data.test_client()
    r = client.get("/api/summary?resolution=daily")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    data = body["data"]
    # Has the same kpis envelope as monthly
    assert "kpis" in data
    # equity_curve should be daily, not monthly
    assert len(data["equity_curve"]) == 3
    assert data["equity_curve"][0]["date"] == "2025-08-15"
    # The daily branch tags resolution back so the frontend can disambiguate
    assert data.get("resolution") == "daily"


def test_summary_resolution_daily_falls_back_to_monthly_when_db_empty(app) -> None:
    """If daily layer hasn't been backfilled, daily request gets graceful
    fallback rather than 500."""
    client = app.test_client()
    r = client.get("/api/summary?resolution=daily")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"]["resolution"] in ("daily", "monthly")
    # equity_curve should at least be a list (possibly empty)
    assert isinstance(body["data"]["equity_curve"], list)


def test_summary_unknown_resolution_treated_as_monthly(app_with_daily_data) -> None:
    """Unknown ?resolution=foo should not 500; fall back to monthly."""
    client = app_with_daily_data.test_client()
    r = client.get("/api/summary?resolution=foo")
    assert r.status_code == 200
    # Same shape as the monthly-default response
    r_default = client.get("/api/summary")
    assert r.data == r_default.data


# --- /api/health daily-readiness -----------------------------------------


def test_health_reports_daily_readiness(app_with_daily_data) -> None:
    """Frontend uses /api/health to decide whether to request ?resolution=daily.
    Phase 4 ships a basic READY state when portfolio_daily has any rows."""
    client = app_with_daily_data.test_client()
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"].get("daily_state") == "READY"


def test_health_reports_initializing_on_empty_db(app) -> None:
    client = app.test_client()
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"].get("daily_state") == "INITIALIZING"
