"""Cycle 46 RED — pin invest.jobs.retry_failed contract.

Operator-triggered DLQ drain. Walks every open failed_tasks row and
calls a caller-supplied resolver. On success, marks resolved; on
failure, bumps attempts. Returns {resolved, still_failing} for the
HTTP envelope (/api/admin/retry-failed).

The Resolver protocol is the seam: callers (admin endpoint, CLI shim)
inspect `task.task_type` and `task.payload` and return a no-arg
callable that fetches AND persists the rows. The drain doesn't know
about prices, FX, or benchmarks — that's the resolver's job. This
mirrors the legacy retry_open_tasks pattern and keeps invest.jobs
free of fetch-side knowledge.

Critical contract: the resolver's callable MUST persist on its own.
The drain discards return values. A resolver that only fetches will
silently mark a row resolved while losing the actual data.
"""
from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.jobs import retry_failed
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


def _seed(repo, target: str = "X", task_type: str = "fetch_price") -> FailedTask:
    return repo.insert(
        FailedTask(
            task_type=task_type,
            payload={"target": target},
            error="HTTPError 502",
        )
    )


class TestEmptyDLQ:
    def test_returns_zero_counters(self, session):
        result = retry_failed.run(session, lambda task: lambda: None)
        assert result == {"resolved": 0, "still_failing": 0}


class TestSuccessfulRetry:
    def test_marks_row_resolved_on_success(self, session, repo):
        task = _seed(repo, "2330.TW")

        def resolver(t):
            return lambda: None  # success

        result = retry_failed.run(session, resolver)
        assert result == {"resolved": 1, "still_failing": 0}

        row = repo.find_by_id(task.id)
        assert row.resolved_at is not None

    def test_resolved_rows_excluded_from_subsequent_drain(self, session, repo):
        _seed(repo, "2330.TW")
        retry_failed.run(session, lambda task: lambda: None)
        # Second drain finds no open rows.
        result = retry_failed.run(session, lambda task: lambda: None)
        assert result == {"resolved": 0, "still_failing": 0}


class TestFailedRetry:
    def test_bumps_attempts_on_failure(self, session, repo):
        task = _seed(repo, "2330.TW")

        def resolver(t):
            def _retry():
                raise RuntimeError("still down")
            return _retry

        result = retry_failed.run(session, resolver)
        assert result == {"resolved": 0, "still_failing": 1}

        row = repo.find_by_id(task.id)
        assert row.resolved_at is None
        assert row.attempts == 2  # 1 (insert) + 1 (this drain)

    def test_attempts_increment_by_one_per_drain(self, session, repo):
        task = _seed(repo, "2330.TW")

        def resolver(t):
            def _retry():
                raise RuntimeError("err")
            return _retry

        retry_failed.run(session, resolver)
        retry_failed.run(session, resolver)
        retry_failed.run(session, resolver)

        row = repo.find_by_id(task.id)
        assert row.attempts == 4  # 1 + 3 drains

    def test_failed_retry_updates_error_message(self, session, repo):
        task = _seed(repo, "2330.TW")

        def resolver(t):
            def _retry():
                raise ValueError("new failure mode")
            return _retry

        retry_failed.run(session, resolver)
        row = repo.find_by_id(task.id)
        assert "ValueError" in row.error
        assert "new failure mode" in row.error


class TestMixedOutcomes:
    def test_counts_resolved_and_still_failing_independently(
        self, session, repo
    ):
        good = _seed(repo, "GOOD")
        bad = _seed(repo, "BAD")

        def resolver(t):
            target = t.payload["target"]
            if target == "GOOD":
                return lambda: None
            return lambda: (_ for _ in ()).throw(RuntimeError("still down"))

        result = retry_failed.run(session, resolver)
        assert result == {"resolved": 1, "still_failing": 1}

        assert repo.find_by_id(good.id).resolved_at is not None
        assert repo.find_by_id(bad.id).resolved_at is None
        assert repo.find_by_id(bad.id).attempts == 2


class TestResolverContract:
    def test_resolver_receives_full_task_row(self, session, repo):
        _seed(repo, "2330.TW", task_type="fetch_price")
        seen = []

        def resolver(task):
            seen.append((task.task_type, task.payload, task.attempts))
            return lambda: None

        retry_failed.run(session, resolver)
        assert len(seen) == 1
        task_type, payload, attempts = seen[0]
        assert task_type == "fetch_price"
        assert payload == {"target": "2330.TW"}
        assert attempts == 1

class TestDrainWalksAllRows:
    def test_drain_visits_every_open_row(self, session, repo):
        _seed(repo, "A")
        _seed(repo, "B")
        _seed(repo, "C")
        seen = []

        def resolver(task):
            seen.append(task.payload["target"])
            return lambda: None

        result = retry_failed.run(session, resolver)
        assert sorted(seen) == ["A", "B", "C"]
        assert result == {"resolved": 3, "still_failing": 0}
