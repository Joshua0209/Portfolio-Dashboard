"""Phase 9 — backfill state machine.

Three states drive the warming-up UX:

    INITIALIZING  → cold-start backfill in flight; data endpoints return
                    202 with progress so the frontend can render a
                    spinner that shows "X of Y symbols fetched".
    READY         → portfolio_daily is populated; all daily endpoints
                    serve 200 normally.
    FAILED        → backfill thread crashed; daily endpoints return 503
                    with the error message so a human can investigate
                    via the Developer Tools accordion (Phase 10).

The state object is a process-wide singleton (`get()`). Readers get a
deep-copied dict (`snapshot()`) so they can't mutate live state, and
writers serialize through an internal RLock. Reads are lock-free except
for the dict copy itself, which keeps the request path responsive even
when a writer thread is busy.
"""
from __future__ import annotations

import copy
import threading
from typing import Any


class BackfillState:
    """Thread-safe holder for backfill-runner status and progress."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Default is READY (passive). The state only flips to INITIALIZING
        # when start() actually spawns a worker thread — i.e. real backfill
        # work is in flight. This way the decorator doesn't 202 in cases
        # where no work is happening (e.g. user ran scripts/backfill_daily.py
        # manually, or the BACKFILL_ON_STARTUP flag is off entirely).
        self._state: str = "READY"
        self._progress: dict[str, Any] = {"total": 0, "done": 0, "current": None}
        self._error: str | None = None

    # --- transitions ----------------------------------------------------

    def mark_ready(self) -> None:
        with self._lock:
            self._state = "READY"
            self._error = None

    def mark_failed(self, error: str) -> None:
        with self._lock:
            self._state = "FAILED"
            self._error = error

    def mark_initializing(self) -> None:
        with self._lock:
            self._state = "INITIALIZING"
            self._error = None

    def set_progress(
        self, total: int, done: int, current: str | None = None
    ) -> None:
        with self._lock:
            self._progress = {"total": total, "done": done, "current": current}

    # --- read helpers ---------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self._state,
                "progress": copy.deepcopy(self._progress),
                "error": self._error,
            }

    def reset(self) -> None:
        """Test helper: restore the initial state."""
        with self._lock:
            self._state = "READY"
            self._progress = {"total": 0, "done": 0, "current": None}
            self._error = None


_singleton: BackfillState | None = None
_singleton_lock = threading.Lock()


def get() -> BackfillState:
    """Return the process-wide BackfillState singleton."""
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = BackfillState()
    return _singleton
