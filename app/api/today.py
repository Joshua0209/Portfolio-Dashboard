"""/today page + admin endpoints (Phases 10, 12, 13).

Phase 10 ships:
  GET  /api/admin/failed-tasks  → list open DLQ rows
  POST /api/admin/retry-failed  → retry every open DLQ row, summarize result

The /today blueprint is mounted now so that Phases 12 (reconciliation)
and 13 (the page itself) can extend the same module instead of moving
endpoints around. Per spec §6.4, all admin actions live behind a
collapsible Developer Tools accordion on /today; the URL namespace
(`/api/admin/*`) is reserved so a future dedicated /admin page can lift
the endpoints without renaming.
"""
from __future__ import annotations

from flask import Blueprint, current_app

from .. import backfill_runner
from ._helpers import envelope

bp = Blueprint("today", __name__)


def _store():
    return current_app.extensions["daily_store"]


# --- DLQ read endpoint ---------------------------------------------------


@bp.get("/api/admin/failed-tasks")
def failed_tasks():
    tasks = _store().get_failed_tasks()
    return envelope({"tasks": tasks, "count": len(tasks)})


# --- DLQ retry dispatcher ------------------------------------------------


def _build_retry_resolver():
    """Map a failed_tasks row → callable that re-runs the original fetch.

    Lives inside the request scope so each task_type's retry is wired to
    the live fetch helpers (and any test-time monkeypatching applies).
    The resolver is intentionally permissive: unknown task_types raise
    so a future task_type added without a retry plan fails loudly.
    """
    # Lazy imports so the price_sources module is captured per-request,
    # which means monkeypatch.setattr(app.price_sources, ...) in tests
    # is observed by the retry path.
    from .. import price_sources

    store = _store()

    def resolver(row):
        ttype = row["task_type"]
        target = row["target"]
        if ttype == "tw_prices":
            # Phase 10's retry policy: re-fetch the broad window. The DLQ
            # row doesn't persist start/end (they're derivable), so a
            # bounded retry over the BACKFILL_FLOOR..today envelope is
            # the conservative choice — duplicate UPSERTs are cheap.
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_prices(
                target, "TWD", floor, today, store=store
            )
        if ttype == "foreign_prices":
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_prices(
                target, "USD", floor, today, store=store
            )
        if ttype == "fx_rates":
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_fx_rates(target, floor, today)
        raise ValueError(f"unknown task_type: {ttype}")

    return resolver


@bp.post("/api/admin/retry-failed")
def retry_failed():
    summary = backfill_runner.retry_open_tasks(_store(), _build_retry_resolver())
    return envelope(summary)
