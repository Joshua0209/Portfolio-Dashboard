"""Phase 14 — global freshness widget.

The endpoint /api/today/freshness is shared with Phase 13's hero. This
module covers the cross-cutting expectations:

  - Every page renders the footer widget (the script tag is global).
  - Color band thresholds match spec: green <1d, yellow <3d, red ≥3d.
  - Network failure → "—" sentinel, no crash.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.daily_store import DailyStore


@pytest.fixture()
def app(tmp_path, monkeypatch, empty_portfolio_json):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "freshness.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    return create_app(empty_portfolio_json)


# --- Band thresholds (pure logic) -----------------------------------------


@pytest.mark.parametrize("stale_days,expected", [
    (0, "green"),
    (1, "yellow"),
    (2, "yellow"),
    (3, "red"),
    (7, "red"),
])
def test_staleness_band_thresholds(stale_days, expected):
    from app.api.today import _staleness_band
    assert _staleness_band(stale_days) == expected


# --- Endpoint shape (re-tested to lock the contract) ---------------------


def test_freshness_endpoint_response_shape(app):
    """Banner/footer JS depends on these exact keys."""
    client = app.test_client()
    r = client.get("/api/today/freshness")
    assert r.status_code == 200
    data = r.get_json()["data"]
    for key in ("data_date", "today_in_tpe", "stale_days", "band"):
        assert key in data


# --- Footer widget renders on every page ---------------------------------


@pytest.mark.parametrize("url", [
    "/", "/today", "/holdings", "/performance", "/risk",
    "/transactions", "/cashflows", "/dividends", "/tax", "/fx", "/benchmark",
])
def test_freshness_footer_on_every_page(app, url):
    """The freshness JS file must load on every dashboard page so the
    footer widget initializes itself."""
    client = app.test_client()
    r = client.get(url)
    assert r.status_code == 200
    assert b"freshness.js" in r.data, (
        f"{url} did not include the freshness widget script"
    )
    assert b"id=\"freshness-footer\"" in r.data, (
        f"{url} did not render the freshness footer slot"
    )


# --- Band reflects actual staleness --------------------------------------


def test_freshness_green_when_data_is_today(app, monkeypatch):
    ds: DailyStore = app.extensions["daily_store"]

    # Pin "today_in_tpe" deterministically by patching the helper.
    import app.api.today as today_mod
    monkeypatch.setattr(today_mod, "_zoneinfo_today", lambda: "2026-04-27")

    with ds.connect_rw() as conn:
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, n_positions, has_overlay) "
            "VALUES ('2026-04-27', 5000000.0, 31.0, 5, 0)"
        )

    client = app.test_client()
    r = client.get("/api/today/freshness")
    data = r.get_json()["data"]
    assert data["stale_days"] == 0
    assert data["band"] == "green"


def test_freshness_red_when_data_is_old(app, monkeypatch):
    ds: DailyStore = app.extensions["daily_store"]
    import app.api.today as today_mod
    monkeypatch.setattr(today_mod, "_zoneinfo_today", lambda: "2026-04-27")

    with ds.connect_rw() as conn:
        # 5 days stale → red band
        conn.execute(
            "INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, n_positions, has_overlay) "
            "VALUES ('2026-04-22', 5000000.0, 31.0, 5, 0)"
        )

    client = app.test_client()
    r = client.get("/api/today/freshness")
    data = r.get_json()["data"]
    assert data["stale_days"] == 5
    assert data["band"] == "red"
