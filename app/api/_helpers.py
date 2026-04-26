"""Shared helpers for API blueprints."""
from __future__ import annotations

from functools import wraps

from flask import current_app

from .. import backfill_state
from ..data_store import DataStore


def store() -> DataStore:
    return current_app.extensions["store"]


def envelope(data, **meta):
    """Consistent JSON envelope: { ok, data, ...meta }."""
    body = {"ok": True, "data": data}
    if meta:
        body["meta"] = meta
    return body


def require_ready_or_warming(handler):
    """Decorator for endpoints that depend on the daily SQLite layer.

    State machine mapping (Phase 9):
      INITIALIZING → 202 + progress envelope (frontend renders spinner).
      FAILED       → 503 + error envelope (frontend deep-links to the
                     Developer Tools accordion via Phase 10).
      READY        → call the wrapped handler normally.

    Per spec §6.5 the frontend's one-shot /api/health probe is what
    decides whether to even ask for daily data. This decorator is the
    backend-side belt that catches anything which slips past — including
    direct curl calls and stale browser tabs.
    """

    @wraps(handler)
    def wrapper(*args, **kwargs):
        snap = backfill_state.get().snapshot()
        if snap["state"] == "READY":
            return handler(*args, **kwargs)
        if snap["state"] == "FAILED":
            return (
                {
                    "ok": False,
                    "error": snap["error"] or "backfill failed",
                    "data": {
                        "state": "FAILED",
                        "progress": snap["progress"],
                    },
                },
                503,
            )
        # INITIALIZING (or any unknown state) → 202 with progress.
        return (
            {
                "ok": True,
                "data": {
                    "state": snap["state"],
                    "progress": snap["progress"],
                },
            },
            202,
        )

    return wrapper
