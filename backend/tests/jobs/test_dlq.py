"""Cycle 46 RED — pin invest.jobs._dlq contract.

The wrapper is the seam between "external fetch failed" and "row in
the DLQ." Phase 1 redesigned the failed_tasks schema (legacy flat
columns → SQLModel with payload: JSON), so this isn't a verbatim
port — the de-dup key migrates from a legacy `target` column to
`payload['target']` (locked decision Q4-a).

Contract:
  fetch_with_dlq(session, task_type, target, fn, *args, **kwargs)
    - Returns fn's value on success.
    - On exception: records/updates a DLQ row, returns None.
    - De-dup key: (task_type, payload['target']) WHERE resolved_at
      IS NULL. Re-failure for the same target bumps `attempts`
      instead of inserting a duplicate.
    - Sanitizes $HOME from error_message before persisting (the DLQ
      is exposed via /api/admin/failed-tasks; we don't leak host
      filesystem layout).

  record_failure(session, task_type, target, payload_extra, error)
    - Same de-dup logic, but the caller already has the error string
      (used by retry_failed.run when bumping). Returns the persisted
      FailedTask.
"""
from __future__ import annotations

import os

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.jobs import _dlq
from invest.persistence.models.failed_task import FailedTask
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return FailedTaskRepo(session)


# ---------------------------------------------------------------------------
# fetch_with_dlq — happy path
# ---------------------------------------------------------------------------


class TestFetchWithDLQSuccess:
    def test_returns_fn_value_on_success(self, session):
        result = _dlq.fetch_with_dlq(
            session, "fetch_price", "2330.TW", lambda: [{"close": 100.0}]
        )
        assert result == [{"close": 100.0}]

    def test_no_dlq_row_written_on_success(self, session, repo):
        _dlq.fetch_with_dlq(
            session, "fetch_price", "2330.TW", lambda: "ok"
        )
        assert repo.count_open() == 0

    def test_passes_args_and_kwargs_to_fn(self, session):
        def fn(a, b, *, c):
            return (a, b, c)

        result = _dlq.fetch_with_dlq(
            session, "fetch_price", "2330.TW", fn, 1, 2, c=3
        )
        assert result == (1, 2, 3)


# ---------------------------------------------------------------------------
# fetch_with_dlq — failure path
# ---------------------------------------------------------------------------


class TestFetchWithDLQFailure:
    def test_returns_none_on_exception(self, session):
        result = _dlq.fetch_with_dlq(
            session,
            "fetch_price",
            "2330.TW",
            lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        assert result is None

    def test_writes_dlq_row_on_first_failure(self, session, repo):
        def boom():
            raise RuntimeError("yfinance unreachable")

        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 1
        assert open_tasks[0].task_type == "fetch_price"
        assert open_tasks[0].payload["target"] == "2330.TW"
        assert open_tasks[0].attempts == 1
        assert "RuntimeError" in open_tasks[0].error
        assert "yfinance unreachable" in open_tasks[0].error

    def test_second_failure_bumps_attempts_not_duplicates(self, session, repo):
        def boom():
            raise RuntimeError("transient blip")

        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)
        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)
        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 1
        assert open_tasks[0].attempts == 3

    def test_different_targets_get_separate_rows(self, session, repo):
        def boom():
            raise RuntimeError("err")

        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)
        _dlq.fetch_with_dlq(session, "fetch_price", "2317.TW", boom)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 2
        targets = {t.payload["target"] for t in open_tasks}
        assert targets == {"2330.TW", "2317.TW"}

    def test_different_task_types_get_separate_rows(self, session, repo):
        def boom():
            raise RuntimeError("err")

        _dlq.fetch_with_dlq(session, "fetch_price", "USD", boom)
        _dlq.fetch_with_dlq(session, "fetch_fx", "USD", boom)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 2

    def test_resolved_row_does_not_block_new_failure(
        self, session, repo
    ):
        def boom():
            raise RuntimeError("err")

        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)
        opened = repo.find_open()[0]
        repo.mark_resolved(opened.id)

        # Fresh failure for same target → new open row, not a bump.
        _dlq.fetch_with_dlq(session, "fetch_price", "2330.TW", boom)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 1
        assert open_tasks[0].id != opened.id
        assert open_tasks[0].attempts == 1


class TestErrorSanitization:
    def test_home_path_redacted(self, session, repo):
        home = os.path.expanduser("~")

        def boom():
            raise FileNotFoundError(f"{home}/secret/path/data.json")

        _dlq.fetch_with_dlq(session, "fetch_price", "X", boom)
        row = repo.find_open()[0]
        assert home not in row.error
        assert "~" in row.error

    def test_long_error_truncated_to_500(self, session, repo):
        def boom():
            raise RuntimeError("x" * 5000)

        _dlq.fetch_with_dlq(session, "fetch_price", "X", boom)
        row = repo.find_open()[0]
        assert len(row.error) <= 500
        assert row.error.endswith("...")


# ---------------------------------------------------------------------------
# record_failure — direct API
# ---------------------------------------------------------------------------


class TestRecordFailure:
    def test_inserts_with_target_in_payload(self, session, repo):
        _dlq.record_failure(
            session,
            "fetch_price",
            "2330.TW",
            payload_extra={},
            error="HTTPError",
        )
        row = repo.find_open()[0]
        assert row.payload["target"] == "2330.TW"

    def test_payload_extra_merges_with_target(self, session, repo):
        _dlq.record_failure(
            session,
            "fetch_price",
            "2330.TW",
            payload_extra={"start": "2025-08-01", "end": "2026-05-01"},
            error="HTTPError",
        )
        row = repo.find_open()[0]
        assert row.payload == {
            "target": "2330.TW",
            "start": "2025-08-01",
            "end": "2026-05-01",
        }

    def test_payload_extra_cannot_override_target(self, session, repo):
        # If a caller mistakenly passes target in payload_extra,
        # the explicit target argument wins.
        _dlq.record_failure(
            session,
            "fetch_price",
            "2330.TW",
            payload_extra={"target": "DIFFERENT"},
            error="err",
        )
        row = repo.find_open()[0]
        assert row.payload["target"] == "2330.TW"

    def test_idempotent_for_same_open_target(self, session, repo):
        first = _dlq.record_failure(
            session, "fetch_price", "X", payload_extra={}, error="e1"
        )
        second = _dlq.record_failure(
            session, "fetch_price", "X", payload_extra={}, error="e2"
        )
        # Same row, attempts bumped, latest error wins.
        assert first.id == second.id
        refreshed = repo.find_by_id(first.id)
        assert refreshed.attempts == 2
        assert refreshed.error == "e2"
