"""Phase 10 — failed-tasks DLQ wrapper.

`fetch_with_dlq(store, task_type, target, fn, *args, **kwargs)`:
  - On success: return fn(*args, **kwargs); no DB write.
  - On exception: insert a `failed_tasks` row (or bump `attempts` if a
    matching open row already exists) and return None — the caller is
    expected to keep going so other symbols still get fetched.
  - The (task_type, target) pair is the de-dupe key for "open" rows
    (resolved_at IS NULL), so retrying the same target after a failure
    does not create N rows.
  - retry_open_tasks(store, fn_resolver) walks open rows and retries
    each one; on success sets resolved_at, on failure increments attempts.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from app.daily_store import DailyStore


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dlq.db")
    s.init_schema()
    return s


def _open_rows(store: DailyStore) -> list[dict[str, Any]]:
    with store.connect_ro() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM failed_tasks ORDER BY id"
        ).fetchall()]


def test_fetch_with_dlq_returns_value_on_success(store):
    from app.backfill_runner import fetch_with_dlq

    out = fetch_with_dlq(
        store, "tw_prices", "2330", lambda: [{"date": "2026-04-25"}]
    )
    assert out == [{"date": "2026-04-25"}]
    assert _open_rows(store) == []


def test_fetch_with_dlq_writes_row_on_exception(store):
    from app.backfill_runner import fetch_with_dlq

    def boom():
        raise RuntimeError("twse 503")

    out = fetch_with_dlq(store, "tw_prices", "2330", boom)
    assert out is None
    rows = _open_rows(store)
    assert len(rows) == 1
    assert rows[0]["task_type"] == "tw_prices"
    assert rows[0]["target"] == "2330"
    assert "twse 503" in rows[0]["error_message"]
    assert rows[0]["attempts"] == 1
    assert rows[0]["first_seen_at"] == rows[0]["last_attempt_at"]
    assert rows[0]["resolved_at"] is None


def test_fetch_with_dlq_dedupes_by_task_target(store):
    """Retrying the same target after failure must NOT insert a second
    open row — it bumps `attempts` and updates `last_attempt_at`."""
    from app.backfill_runner import fetch_with_dlq

    def boom():
        raise RuntimeError("twse 503")

    fetch_with_dlq(store, "tw_prices", "2330", boom)
    fetch_with_dlq(store, "tw_prices", "2330", boom)
    fetch_with_dlq(store, "tw_prices", "2330", boom)

    rows = _open_rows(store)
    assert len(rows) == 1
    assert rows[0]["attempts"] == 3


def test_fetch_with_dlq_separate_targets_separate_rows(store):
    from app.backfill_runner import fetch_with_dlq

    def boom_for(t):
        def _():
            raise RuntimeError(f"fail {t}")
        return _

    fetch_with_dlq(store, "tw_prices", "2330", boom_for("a"))
    fetch_with_dlq(store, "tw_prices", "2454", boom_for("b"))

    rows = _open_rows(store)
    assert len(rows) == 2
    assert {r["target"] for r in rows} == {"2330", "2454"}


def test_resolved_failure_does_not_block_new_open_row(store):
    """If a row was resolved, a new failure for the same target must
    create a fresh open row (not bump the resolved one)."""
    from app.backfill_runner import fetch_with_dlq

    def boom():
        raise RuntimeError("first")

    fetch_with_dlq(store, "tw_prices", "2330", boom)
    # Manually mark resolved
    with store.connect_rw() as conn:
        conn.execute(
            "UPDATE failed_tasks SET resolved_at = '2026-04-25T00:00:00Z' "
            "WHERE id = 1"
        )

    def boom2():
        raise RuntimeError("second")

    fetch_with_dlq(store, "tw_prices", "2330", boom2)

    with store.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM failed_tasks ORDER BY id"
        ).fetchall()]
    assert len(rows) == 2
    assert rows[0]["resolved_at"] is not None
    assert rows[1]["resolved_at"] is None
    assert rows[1]["attempts"] == 1


def test_retry_open_tasks_resolves_on_success(store):
    """retry_open_tasks(store, resolver): walks every open row, calls
    resolver(row) which returns a callable to retry; on success sets
    resolved_at."""
    from app.backfill_runner import fetch_with_dlq, retry_open_tasks

    def boom():
        raise RuntimeError("first")

    fetch_with_dlq(store, "tw_prices", "2330", boom)
    fetch_with_dlq(store, "tw_prices", "2454", boom)

    def resolver(row):
        # First task succeeds, second still fails.
        if row["target"] == "2330":
            return lambda: [{"ok": True}]
        return lambda: (_ for _ in ()).throw(RuntimeError("still failing"))

    summary = retry_open_tasks(store, resolver)
    assert summary["resolved"] == 1
    assert summary["still_failing"] == 1

    with store.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT target, resolved_at, attempts FROM failed_tasks ORDER BY target"
        ).fetchall()]
    assert rows[0]["target"] == "2330"
    assert rows[0]["resolved_at"] is not None
    assert rows[1]["target"] == "2454"
    assert rows[1]["resolved_at"] is None
    assert rows[1]["attempts"] == 2


def test_retry_open_tasks_skips_resolved_rows(store):
    from app.backfill_runner import fetch_with_dlq, retry_open_tasks

    def boom():
        raise RuntimeError("x")

    fetch_with_dlq(store, "tw_prices", "2330", boom)
    with store.connect_rw() as conn:
        conn.execute(
            "UPDATE failed_tasks SET resolved_at='2026-04-25T00:00:00Z'"
        )

    calls = {"n": 0}

    def resolver(row):
        calls["n"] += 1
        return lambda: []

    summary = retry_open_tasks(store, resolver)
    assert summary["resolved"] == 0
    assert summary["still_failing"] == 0
    assert calls["n"] == 0
