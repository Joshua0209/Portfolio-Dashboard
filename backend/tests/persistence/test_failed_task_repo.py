from datetime import datetime, timezone

import pytest
from sqlmodel import Session, SQLModel, create_engine

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


def _task(**overrides) -> FailedTask:
    defaults = dict(
        task_type="fetch_price",
        payload={"symbol": "2330.TW", "date": "2026-05-01"},
        error="HTTPError 502 from yfinance",
    )
    defaults.update(overrides)
    return FailedTask(**defaults)


class TestInsert:
    def test_insert_assigns_id_and_timestamps(self, repo):
        saved = repo.insert(_task())
        assert saved.id is not None
        assert saved.first_failed_at is not None
        assert saved.last_failed_at is not None
        assert saved.attempts == 1
        assert saved.resolved_at is None


class TestFindOpen:
    def test_find_open_excludes_resolved(self, repo):
        a = repo.insert(_task())
        repo.insert(_task())
        repo.mark_resolved(a.id)

        open_tasks = repo.find_open()
        assert len(open_tasks) == 1
        assert open_tasks[0].resolved_at is None

    def test_find_open_returns_chronological_by_first_failed(self, repo):
        repo.insert(_task(payload={"symbol": "A"}))
        repo.insert(_task(payload={"symbol": "B"}))
        tasks = repo.find_open()
        # First inserted appears first.
        assert tasks[0].payload["symbol"] == "A"


class TestFindByType:
    def test_filters_by_task_type(self, repo):
        repo.insert(_task(task_type="fetch_price"))
        repo.insert(_task(task_type="fetch_fx"))
        repo.insert(_task(task_type="fetch_price"))
        prices = repo.find_by_type("fetch_price")
        assert len(prices) == 2
        assert all(t.task_type == "fetch_price" for t in prices)


class TestBumpAttempt:
    def test_bump_increments_attempts_and_updates_error(self, repo):
        saved = repo.insert(_task())
        original_first = saved.first_failed_at
        repo.bump_attempt(saved.id, error="HTTPError 503 next time")

        after = repo.find_by_id(saved.id)
        assert after.attempts == 2
        assert after.error == "HTTPError 503 next time"
        # first_failed_at must NOT change.
        assert after.first_failed_at == original_first
        # last_failed_at SHOULD have advanced.
        assert after.last_failed_at >= original_first

    def test_bump_unknown_id_silent(self, repo):
        repo.bump_attempt(99999, error="anything")
        # No raise.


class TestMarkResolved:
    def test_resolved_sets_timestamp_and_excludes_from_open(self, repo):
        saved = repo.insert(_task())
        repo.mark_resolved(saved.id)
        after = repo.find_by_id(saved.id)
        assert after.resolved_at is not None
        assert repo.find_open() == []

    def test_mark_resolved_unknown_id_silent(self, repo):
        repo.mark_resolved(99999)
        # No raise.


class TestCountOpen:
    def test_count_open(self, repo):
        a = repo.insert(_task())
        repo.insert(_task())
        repo.insert(_task())
        repo.mark_resolved(a.id)
        assert repo.count_open() == 2
