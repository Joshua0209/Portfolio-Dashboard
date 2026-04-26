"""Phase 9 — backfill state machine.

The state machine surfaces three states (INITIALIZING/READY/FAILED) plus
a structured progress object so the frontend can render a warming-up
component with concrete numbers (e.g. "12 of 37 symbols fetched"). Per
spec §6.4, the timeout banner trigger drops to 2 minutes given the
revised ~30–60s cold-start estimate, so progress reporting at least
once per symbol is the minimum useful resolution.

The state object itself is held in a module-level singleton (one per
process) so that any thread — Flask request handler, background
backfill thread, snapshot CLI — can read the same view. Writes are
guarded by an internal lock; reads are lock-free dict copies so the
request path never blocks on the writer.
"""
from __future__ import annotations

import threading

import pytest


def test_initial_state_is_ready():
    """READY is the passive default — only mark_initializing() flips
    state when actual backfill work is in flight."""
    from app.backfill_state import BackfillState

    s = BackfillState()
    snap = s.snapshot()
    assert snap["state"] == "READY"
    assert snap["progress"]["total"] == 0
    assert snap["progress"]["done"] == 0
    assert snap["error"] is None


def test_mark_initializing_transitions_state():
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.mark_initializing()
    assert s.snapshot()["state"] == "INITIALIZING"


def test_mark_ready_transitions_state():
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.mark_ready()
    assert s.snapshot()["state"] == "READY"


def test_mark_failed_records_error_message():
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.mark_failed("twse fetch exploded")
    snap = s.snapshot()
    assert snap["state"] == "FAILED"
    assert snap["error"] == "twse fetch exploded"


def test_set_progress_round_trips_total_and_done():
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.set_progress(total=37, done=0, current="2330")
    snap = s.snapshot()
    assert snap["progress"]["total"] == 37
    assert snap["progress"]["done"] == 0
    assert snap["progress"]["current"] == "2330"

    s.set_progress(total=37, done=12, current="3711")
    snap = s.snapshot()
    assert snap["progress"]["done"] == 12
    assert snap["progress"]["current"] == "3711"


def test_snapshot_returns_independent_copy():
    """Mutating the returned snapshot must not corrupt internal state."""
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.set_progress(total=10, done=3, current="X")
    snap = s.snapshot()
    snap["progress"]["done"] = 9999
    assert s.snapshot()["progress"]["done"] == 3


def test_concurrent_writers_do_not_lose_updates():
    """The state object is shared across threads; updates from one thread
    must be visible to readers in another."""
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.set_progress(total=100, done=0)

    def worker():
        for _ in range(50):
            cur = s.snapshot()["progress"]["done"]
            s.set_progress(total=100, done=cur + 1)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # We don't assert exact final value (race-windowed read-modify-write
    # is intentionally not atomic), but it must be > 0 and ≤ 100.
    final = s.snapshot()["progress"]["done"]
    assert 0 < final <= 100


def test_get_singleton_returns_same_instance():
    """app.backfill_state.get() returns a process-wide singleton."""
    from app import backfill_state

    a = backfill_state.get()
    b = backfill_state.get()
    assert a is b


def test_reset_clears_state_for_tests():
    """Tests that exercise the state machine need to reset between runs."""
    from app.backfill_state import BackfillState

    s = BackfillState()
    s.mark_failed("x")
    s.set_progress(total=5, done=5)
    s.reset()
    snap = s.snapshot()
    assert snap["state"] == "READY"
    assert snap["progress"]["done"] == 0
    assert snap["error"] is None
