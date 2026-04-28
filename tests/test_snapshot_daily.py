"""Phase 15 — snapshot_daily.py incremental refresh.

Acceptance (per plan §3 Phase 15):
  - After 3-day pause, snapshot writes ~3 trading days' rows; updates
    meta.last_known_date.
  - Re-running immediately is a no-op.
  - Running while Flask is up: Flask picks up new rows on next request
    without restart (covered by the WAL contract; tested at the SQL
    level in test_dlq.py / test_daily_store.py).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.daily_store import DailyStore


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "snapshot.db")
    s.init_schema()
    return s


@pytest.fixture()
def portfolio(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [
            {
                "month": "2026-04",
                "tw": {
                    "holdings": [
                        {"code": "2330", "name": "TSMC", "qty": 1000,
                         "avg_cost": 880.0, "type": "現股"},
                    ],
                },
                "foreign": {"holdings": []},
            }
        ],
        "summary": {
            "all_trades": [
                {"month": "2026-04", "date": "2026/04/15", "venue": "TW",
                 "side": "普買", "code": "2330", "qty": 1000, "price": 880.0, "ccy": "TWD"},
            ],
        },
    }))
    return p


def test_compute_increment_window_starts_from_last_known_plus_one(store, monkeypatch):
    """Plan §3 Phase 15: incremental refresh fetches strictly after the
    last known date."""
    store.set_meta("last_known_date", "2026-04-22")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    out = snap_mod.compute_increment_window(store)
    assert out == ("2026-04-23", "2026-04-25")


def test_compute_increment_window_returns_none_when_already_current(store, monkeypatch):
    """Re-running immediately is a no-op (plan §3 Phase 15 acceptance)."""
    store.set_meta("last_known_date", "2026-04-25")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    assert snap_mod.compute_increment_window(store) is None


def test_compute_increment_window_falls_back_to_floor_when_no_meta(store, monkeypatch):
    """Cold start (no last_known_date set yet) falls back to BACKFILL_FLOOR."""
    # Wipe the meta key
    with store.connect_rw() as conn:
        conn.execute("DELETE FROM meta WHERE key = 'last_known_date'")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    out = snap_mod.compute_increment_window(store)
    floor = store.get_meta("backfill_floor") or "2025-08-01"
    # When no last_known is set, returns the broad cold-start window
    assert out == (floor, "2026-04-25")


def test_run_no_op_when_already_current(store, portfolio, monkeypatch):
    """Running back-to-back: meta.last_known_date already == today →
    early return with new_dates=0."""
    store.set_meta("last_known_date", "2026-04-25")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    portfolio_dict = json.loads(Path(portfolio).read_text())
    summary = snap_mod.run(store, portfolio_dict)
    assert summary["new_dates"] == 0
    assert summary["new_rows"] == 0
    assert summary["skipped_reason"] == "already_current"


def test_run_advances_last_known_date_after_fetch(store, portfolio, monkeypatch):
    """After a successful run, meta.last_known_date == today."""
    store.set_meta("last_known_date", "2026-04-22")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    # Patch the price fetch so the test doesn't hit the network. Returns
    # 3 fresh rows for the 3 newly-in-window days.
    def fake_get_prices(symbol, ccy, start, end, store=None, today=None):
        return [
            {"date": "2026-04-23", "symbol": symbol, "close": 920.0,
             "currency": "TWD", "source": "yfinance"},
            {"date": "2026-04-24", "symbol": symbol, "close": 925.0,
             "currency": "TWD", "source": "yfinance"},
            {"date": "2026-04-25", "symbol": symbol, "close": 930.0,
             "currency": "TWD", "source": "yfinance"},
        ]

    monkeypatch.setattr(snap_mod, "_get_prices", fake_get_prices)
    monkeypatch.setattr(snap_mod, "_get_fx_rates",
                        lambda ccy, start, end, store=None, today=None: [])

    portfolio_dict = json.loads(Path(portfolio).read_text())
    summary = snap_mod.run(store, portfolio_dict)

    assert summary["new_dates"] >= 1
    assert store.get_meta("last_known_date") == "2026-04-25"


def test_run_no_creds_no_overlay_attempted(store, portfolio, monkeypatch):
    """Without Shioaji creds, snapshot must not call list_trades. The
    overlay path is wrapped in try/except inside run() per the same
    'never crash the data layer' contract from Phase 11."""
    store.set_meta("last_known_date", "2026-04-22")
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    monkeypatch.setattr(snap_mod, "_get_prices",
                        lambda *a, **kw: [])
    monkeypatch.setattr(snap_mod, "_get_fx_rates",
                        lambda *a, **kw: [])

    portfolio_dict = json.loads(Path(portfolio).read_text())
    # Should not raise even though we provide no Shioaji impl.
    summary = snap_mod.run(store, portfolio_dict)
    assert "new_dates" in summary


# --- /api/admin/refresh wires to scripts.snapshot_daily.run -------------


@pytest.fixture()
def app(tmp_path, monkeypatch, empty_portfolio_json):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "snapshot_api.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    return create_app(empty_portfolio_json)


def test_admin_refresh_invokes_snapshot_daily_run(app, monkeypatch):
    """The Phase 13 _run_snapshot seam should resolve to the real
    scripts.snapshot_daily.run() once Phase 15 is installed."""
    sentinel: dict[str, Any] = {}

    def fake_run(store, portfolio):
        sentinel["called"] = True
        return {"new_dates": 2, "new_rows": 7}

    import scripts.snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "run", fake_run)

    client = app.test_client()
    r = client.post("/api/admin/refresh")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["new_dates"] == 2
    assert sentinel.get("called") is True


# --- CLI exit codes ------------------------------------------------------


def test_cli_main_exits_zero_on_clean_run(tmp_path, monkeypatch, store, portfolio):
    """python scripts/snapshot_daily.py exits 0 on success (no-op or
    new rows)."""
    store.set_meta("last_known_date", "2026-04-25")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    rc = snap_mod.main([
        "--portfolio", str(portfolio),
        "--db", str(store.path),
    ])
    assert rc == 0
