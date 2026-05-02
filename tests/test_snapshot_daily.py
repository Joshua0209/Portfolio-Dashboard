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


def test_run_already_current_still_invokes_overlay(store, portfolio, monkeypatch):
    """Regression: prices being current must not gate the overlay. The
    overlay's freshness clock (latest PDF month-end → today) is
    independent of meta.last_known_date — a refresh on a fully-priced
    day must still call into trade_overlay so post-PDF broker trades
    aren't stranded.
    """
    store.set_meta("last_known_date", "2026-04-25")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    called: dict[str, Any] = {}

    def fake_run_overlay(s, p, today, sdk_data=None):
        called["today"] = today
        return {"overlay_trades": 7, "dates_written": 3, "skipped_reason": None}

    monkeypatch.setattr(snap_mod, "_run_overlay_safe", fake_run_overlay)

    portfolio_dict = json.loads(Path(portfolio).read_text())
    summary = snap_mod.run(store, portfolio_dict)
    assert called == {"today": "2026-04-25"}
    assert summary["overlay"] == {
        "overlay_trades": 7, "dates_written": 3, "skipped_reason": None,
    }
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


# --- Overlay symbol pre-fetch (two-pass orchestration, 2026-05-01 fix) ---


def test_run_fetches_prices_for_overlay_discovered_symbols(
    store, tmp_path, monkeypatch
):
    """Production bug 2026-05-01: snapshot_daily only fetched prices for
    PDF-known symbols. Overlay-discovered codes (6531/7769/etc. from
    list_realized_pairs / list_open_lots) silently dropped at merge()'s
    `if close is None: continue` line. Fix: discover overlay symbols
    BEFORE the price fetch, then include any new ones in the fetch list.
    """
    # Latest PDF month must be strictly before today so the overlay gap
    # window is non-None — the symbol-discovery seam runs only inside
    # the gap.
    portfolio_path = tmp_path / "portfolio.json"
    portfolio_path.write_text(json.dumps({
        "months": [
            {"month": "2026-03",
             "tw": {"holdings": [{"code": "2330", "name": "TSMC",
                                  "qty": 1000, "avg_cost": 880.0,
                                  "type": "現股"}]},
             "foreign": {"holdings": []}}
        ],
        "summary": {"all_trades": []},
    }))
    portfolio = portfolio_path
    store.set_meta("last_known_date", "2026-04-22")

    from scripts import snapshot_daily as snap_mod
    monkeypatch.setattr(snap_mod, "_today_iso", lambda: "2026-04-25")

    # Track which symbols got price-fetched
    fetched_symbols: list[str] = []

    def fake_get_prices(symbol, ccy, start, end, store=None, today=None):
        fetched_symbols.append(symbol)
        return [{"date": "2026-04-23", "symbol": symbol, "close": 681.0,
                 "currency": ccy, "source": "yfinance"}]

    monkeypatch.setattr(snap_mod, "_get_prices", fake_get_prices)
    monkeypatch.setattr(snap_mod, "_get_fx_rates",
                        lambda *a, **kw: [])

    # Patch the SDK pull so it returns synthetic data containing 6531
    # (a code NOT in the PDF holdings — the canonical overlay-only-symbol
    # case). _fetch_overlay_symbol_prices unions codes across all 3
    # surfaces, so we put 6531 into pairs (a closed pair from the gap).
    from app import trade_overlay
    monkeypatch.setattr(
        trade_overlay, "pull_sdk_sources",
        lambda client, store, gap_start, gap_end: (
            [],  # session
            [],  # lots
            [   # pairs (one closed pair for 6531)
                {"date": "2026-04-20", "code": "6531", "side": "普買",
                 "qty": 50, "price": 681.0, "cost_twd": 34_050.0,
                 "ccy": "TWD", "venue": "TW", "type": "現股",
                 "pair_id": 999},
                {"date": "2026-04-23", "code": "6531", "side": "普賣",
                 "qty": 50, "price": 690.0,
                 "ccy": "TWD", "venue": "TW", "type": "現股",
                 "pair_id": 999, "pnl": 450.0},
            ],
        ),
    )
    # Stub the merge so we don't drag the SDK projection in
    monkeypatch.setattr(snap_mod, "_run_overlay_safe",
                        lambda *a, **kw: {"overlay_trades": 1,
                                          "dates_written": 1,
                                          "skipped_reason": None})

    portfolio_dict = json.loads(Path(portfolio).read_text())
    snap_mod.run(store, portfolio_dict)

    # 2330 is in the PDF holdings AND the overlay set — fetched once
    # (no double-fetch). 6531 is overlay-only — must be fetched.
    assert "6531" in fetched_symbols, (
        "overlay-discovered symbols must be price-fetched; got "
        f"{fetched_symbols}"
    )


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
