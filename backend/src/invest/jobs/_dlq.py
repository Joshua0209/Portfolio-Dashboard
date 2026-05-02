"""DLQ wrapper — captures fetch failures into the failed_tasks table.

Private to invest.jobs (underscore prefix). Both invest.jobs.backfill
and invest.jobs.retry_failed depend on this seam; keeping it
separate avoids a circular import between those siblings (backfill
writes failures during a run; retry_failed drains them later).

De-dup contract:
  Key:   (task_type, payload['target']) WHERE resolved_at IS NULL.
  Reason: a single backfill run can fail the same symbol multiple
          times across retry passes; one row per stuck target keeps
          /api/admin/failed-tasks operator-readable.
  Reset: once a row is marked resolved, a fresh failure for the
          same target inserts a new open row (not a bump).

Why payload['target'] vs a flat column:
  Phase 1 redesigned failed_tasks with payload: JSON. The new shape
  is more flexible — resolvers can stash arbitrary task context
  (date ranges, currencies, etc.) — at the cost of a single
  json_extract index lookup. The DLQ wrapper enforces the
  payload['target'] convention so the de-dup key is uniform across
  resolvers.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable, TypeVar

from sqlmodel import Session

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo

log = logging.getLogger(__name__)

T = TypeVar("T")

_HOME = os.path.expanduser("~")


def sanitize_error_message(msg: str) -> str:
    """Redact $HOME and truncate before persisting.

    failed_tasks rows are exposed via the unauthenticated
    /api/admin/failed-tasks endpoint — full host paths leak
    filesystem layout when the dashboard runs behind a tunnel or LAN.
    The 500-char cap keeps pathological tracebacks from filling the
    row.
    """
    if not msg:
        return msg
    out = msg.replace(_HOME, "~")
    if len(out) > 500:
        out = out[:497] + "..."
    return out


def fetch_with_dlq(
    session: Session,
    task_type: str,
    target: str,
    fn: Callable[..., T],
    *args: Any,
    **kwargs: Any,
) -> T | None:
    """Wrap a fetch so a single-target failure becomes a DLQ row
    instead of aborting the run.

    Returns fn's value on success, None on exception.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — boundary by design
        message = sanitize_error_message(f"{type(exc).__name__}: {exc}")
        log.warning(
            "fetch_with_dlq: %s/%s failed: %s", task_type, target, message
        )
        record_failure(
            session, task_type, target, payload_extra={}, error=message
        )
        return None


def record_failure(
    session: Session,
    task_type: str,
    target: str,
    payload_extra: dict[str, Any],
    error: str,
) -> FailedTask:
    """Insert or bump a DLQ row keyed by (task_type, target).

    The explicit `target` argument always wins — a payload_extra
    that mistakenly carries a 'target' key is overwritten. This
    keeps the de-dup key consistent regardless of caller hygiene.
    """
    repo = FailedTaskRepo(session)
    existing = repo.find_open_by_target(task_type, target)
    if existing is not None:
        repo.bump_attempt(existing.id, error)
        refreshed = repo.find_by_id(existing.id)
        if refreshed is None:
            raise RuntimeError(f"FailedTask {existing.id} vanished after bump_attempt")
        return refreshed
    payload = {**payload_extra, "target": target}
    return repo.insert(
        FailedTask(task_type=task_type, payload=payload, error=error)
    )
