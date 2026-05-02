"""Operator-triggered DLQ drain.

Walks every open failed_tasks row and calls a caller-supplied
resolver. On success, marks the row resolved; on failure, bumps
attempts. Returns {resolved, still_failing} for the HTTP envelope
served at POST /api/admin/retry-failed and the CLI shim
scripts/retry_failed.py.

Resolver contract:
  resolver(task) -> Callable[[], Any]

  The drain inspects task.task_type and task.payload to decide what
  to retry, but doesn't itself know how. The returned callable MUST
  fetch AND persist the rows for the given DLQ entry — the drain
  discards the return value, so a fetch-only callable would mark
  the row resolved while losing the actual data. This mirrors the
  legacy retry_open_tasks contract.

The drain doesn't transition invest.core.state — backfill state is
specifically about the cold-start lifecycle. A retry pass is
operator-initiated and synchronous; if it fails we surface the
counters in the response, not in the global daily-state machine.
"""
from __future__ import annotations

import logging
from typing import Any, Callable

from sqlmodel import Session

from invest.jobs._dlq import sanitize_error_message
from invest.persistence.models.failed_task import FailedTask
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo

log = logging.getLogger(__name__)

Resolver = Callable[[FailedTask], Callable[[], Any]]


def run(session: Session, resolver: Resolver) -> dict[str, int]:
    repo = FailedTaskRepo(session)
    open_tasks = repo.find_open()

    resolved = 0
    still_failing = 0
    for task in open_tasks:
        try:
            retry_fn = resolver(task)
            retry_fn()
        except Exception as exc:  # noqa: BLE001 — boundary by design
            sanitized = sanitize_error_message(
                f"{type(exc).__name__}: {exc}"
            )
            log.warning(
                "retry_failed: task %s (%s/%s) still failing: %s",
                task.id,
                task.task_type,
                task.payload.get("target"),
                sanitized,
            )
            repo.bump_attempt(task.id, sanitized)
            still_failing += 1
            continue
        repo.mark_resolved(task.id)
        resolved += 1

    return {"resolved": resolved, "still_failing": still_failing}
