"""Cycle 45 RED — pin invest.core.state contract.

The state machine port from app/backfill_state.py is logic-preserving
per Phase 7 risk row ("Keep the state machine intact; just split files").
These tests freeze the legacy behavior so the new module can't drift.

Contract surface (verbatim from app/backfill_state.py):
  - BackfillState class with three states: READY, INITIALIZING, FAILED.
  - Default state on construction is READY (passive — no INITIALIZING
    until start() spawns a worker, otherwise the /api/health endpoint
    would 202-stall every fresh process).
  - Transitions: mark_ready / mark_initializing / mark_failed(error).
  - set_progress(total, done, current=None) writes a dict.
  - snapshot() returns a *deep-copied* dict so readers cannot mutate
    live state — this matters because /api/health threads could race
    a writer and corrupt the progress dict.
  - reset() restores defaults (test helper only).
  - get() returns a process-wide singleton with double-checked locking.

The class is also reused as a generic daily-job state surface — it's
read by the HTTP layer's /api/health and the daily-state gate in the
Phase 6 today/daily routers. Keeping the type name BackfillState
preserves the legacy semantic: "the state of the cold-start backfill
that warms the daily-resolution layer."
"""
from __future__ import annotations

import threading

import pytest

from invest.core import state as state_module
from invest.core.state import BackfillState


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Each test gets a fresh module-level singleton."""
    state_module._singleton = None
    yield
    state_module._singleton = None


class TestDefaultState:
    def test_default_is_ready(self):
        s = BackfillState()
        snap = s.snapshot()
        assert snap["state"] == "READY"
        assert snap["error"] is None
        assert snap["progress"] == {"total": 0, "done": 0, "current": None}


class TestTransitions:
    def test_mark_initializing(self):
        s = BackfillState()
        s.mark_initializing()
        assert s.snapshot()["state"] == "INITIALIZING"
        assert s.snapshot()["error"] is None

    def test_mark_failed_records_error(self):
        s = BackfillState()
        s.mark_failed("yfinance HTTPError 503")
        snap = s.snapshot()
        assert snap["state"] == "FAILED"
        assert snap["error"] == "yfinance HTTPError 503"

    def test_mark_ready_clears_error(self):
        s = BackfillState()
        s.mark_failed("transient blip")
        s.mark_ready()
        snap = s.snapshot()
        assert snap["state"] == "READY"
        assert snap["error"] is None

    def test_mark_initializing_clears_error(self):
        s = BackfillState()
        s.mark_failed("blip")
        s.mark_initializing()
        assert s.snapshot()["error"] is None


class TestProgress:
    def test_set_progress_with_current_symbol(self):
        s = BackfillState()
        s.set_progress(total=42, done=7, current="2330.TW")
        assert s.snapshot()["progress"] == {
            "total": 42,
            "done": 7,
            "current": "2330.TW",
        }

    def test_set_progress_default_current_is_none(self):
        s = BackfillState()
        s.set_progress(total=10, done=5)
        assert s.snapshot()["progress"]["current"] is None


class TestSnapshotIsolation:
    def test_snapshot_returns_deep_copy(self):
        s = BackfillState()
        s.set_progress(total=5, done=2, current="AAPL")
        snap = s.snapshot()
        # Mutate the returned dict; live state must not change.
        snap["progress"]["done"] = 999
        snap["state"] = "TAMPERED"
        live = s.snapshot()
        assert live["progress"]["done"] == 2
        assert live["state"] == "READY"

    def test_snapshot_progress_is_independent_per_call(self):
        s = BackfillState()
        s.set_progress(total=1, done=0)
        snap1 = s.snapshot()
        snap2 = s.snapshot()
        snap1["progress"]["done"] = 999
        assert snap2["progress"]["done"] == 0


class TestReset:
    def test_reset_restores_defaults(self):
        s = BackfillState()
        s.mark_failed("err")
        s.set_progress(total=10, done=5, current="X")
        s.reset()
        snap = s.snapshot()
        assert snap == {
            "state": "READY",
            "progress": {"total": 0, "done": 0, "current": None},
            "error": None,
        }


class TestSingleton:
    def test_get_returns_same_instance(self):
        a = state_module.get()
        b = state_module.get()
        assert a is b

    def test_get_is_thread_safe(self):
        # Race many callers; all must see the same singleton.
        results: list[BackfillState] = []
        barrier = threading.Barrier(20)

        def grab():
            barrier.wait()
            results.append(state_module.get())

        threads = [threading.Thread(target=grab) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        first = results[0]
        assert all(r is first for r in results)
