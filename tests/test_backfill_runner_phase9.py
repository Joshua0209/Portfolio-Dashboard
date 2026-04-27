"""Phase 9 — background thread + warming-up state machine.

The Phase-9 test surface is narrow: we must verify that
`backfill_runner.start()` spawns a daemon thread, that thread mutates
the shared `backfill_state` singleton on success/failure, and that
exceptions inside the worker are caught and surfaced as FAILED rather
than crashing the Flask process.

We deliberately stub out `run_full_backfill` itself — its end-to-end
behavior is covered by Phase 3/5/6 tests. Phase 9 only owns the
threading wrapper.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from app import backfill_state
from app.daily_store import DailyStore


@pytest.fixture(autouse=True)
def _reset_state():
    backfill_state.get().reset()
    yield
    backfill_state.get().reset()


@pytest.fixture()
def daily_store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "phase9.db")
    s.init_schema()
    return s


def test_start_returns_daemon_thread(daily_store, empty_portfolio_json, monkeypatch):
    from app import backfill_runner

    started = threading.Event()

    def fake_full(store, portfolio_path, **kwargs):
        started.set()
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", fake_full)

    t = backfill_runner.start(daily_store, empty_portfolio_json)
    assert isinstance(t, threading.Thread)
    assert t.daemon is True
    started.wait(timeout=2)
    t.join(timeout=2)
    assert not t.is_alive()


def test_start_transitions_state_to_ready_on_success(
    daily_store, empty_portfolio_json, monkeypatch
):
    from app import backfill_runner

    def fake_full(store, portfolio_path, **kwargs):
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", fake_full)

    t = backfill_runner.start(daily_store, empty_portfolio_json)
    t.join(timeout=2)

    snap = backfill_state.get().snapshot()
    assert snap["state"] == "READY"
    assert snap["error"] is None


def test_start_transitions_state_to_failed_on_exception(
    daily_store, empty_portfolio_json, monkeypatch
):
    from app import backfill_runner

    def boom(store, portfolio_path, **kwargs):
        raise RuntimeError("yfinance 503 fire drill")

    monkeypatch.setattr(backfill_runner, "run_full_backfill", boom)

    t = backfill_runner.start(daily_store, empty_portfolio_json)
    t.join(timeout=2)

    snap = backfill_state.get().snapshot()
    assert snap["state"] == "FAILED"
    assert "yfinance 503 fire drill" in snap["error"]


def test_start_does_nothing_if_already_running(
    daily_store, empty_portfolio_json, monkeypatch
):
    """Two boots in quick succession (e.g. Werkzeug reloader) must not
    spawn duplicate worker threads."""
    from app import backfill_runner

    block = threading.Event()
    started_count = {"n": 0}

    def slow_full(store, portfolio_path, **kwargs):
        started_count["n"] += 1
        block.wait(timeout=2)
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", slow_full)

    t1 = backfill_runner.start(daily_store, empty_portfolio_json)
    t2 = backfill_runner.start(daily_store, empty_portfolio_json)

    assert t2 is t1 or t2 is None  # second call short-circuits
    block.set()
    t1.join(timeout=2)
    assert started_count["n"] == 1


def test_start_skips_if_data_already_ready(
    tmp_path, daily_store, empty_portfolio_json, monkeypatch
):
    """If portfolio_daily already has rows, transition to READY without
    spawning the worker — re-running Flask after a successful backfill
    should not re-fetch."""
    from app import backfill_runner

    # Seed one row so the state machine considers data ready.
    with daily_store.connect_rw() as conn:
        conn.execute(
            """
            INSERT INTO portfolio_daily(date, equity_twd, fx_usd_twd, n_positions, has_overlay)
            VALUES ('2026-04-25', 1000.0, 0.032, 1, 0)
            """
        )

    called = {"n": 0}

    def fake_full(store, portfolio_path, **kwargs):
        called["n"] += 1
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", fake_full)

    t = backfill_runner.start(daily_store, empty_portfolio_json)
    if t is not None:
        t.join(timeout=2)

    assert called["n"] == 0
    assert backfill_state.get().snapshot()["state"] == "READY"


def test_create_app_does_not_start_thread_when_flag_unset(
    tmp_path, monkeypatch, empty_portfolio_json
):
    """BACKFILL_ON_STARTUP=false (default) must keep the existing boot
    behavior — no daemon thread spawn — so prod can ship Phase 9 dark."""
    from app import backfill_runner

    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "phase9.db"))

    called = {"n": 0}

    def fake_full(store, portfolio_path, **kwargs):
        called["n"] += 1
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", fake_full)

    from app import create_app

    create_app(empty_portfolio_json)
    time.sleep(0.05)
    assert called["n"] == 0


def test_create_app_starts_thread_when_flag_set(
    tmp_path, monkeypatch, empty_portfolio_json
):
    """BACKFILL_ON_STARTUP=true triggers the daemon thread on app boot.
    The thread is allowed to run to completion via the stub."""
    from app import backfill_runner

    monkeypatch.setenv("BACKFILL_ON_STARTUP", "true")
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "phase9.db"))
    monkeypatch.delenv("WERKZEUG_RUN_MAIN", raising=False)

    done = threading.Event()

    def fake_full(store, portfolio_path, **kwargs):
        done.set()
        return {"ok": True}

    monkeypatch.setattr(backfill_runner, "run_full_backfill", fake_full)

    from app import create_app

    create_app(empty_portfolio_json)
    assert done.wait(timeout=2)
