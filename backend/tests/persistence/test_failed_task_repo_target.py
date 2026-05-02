"""Cycle 46 RED — extend FailedTaskRepo with find_open_by_target.

Phase 7's DLQ wrapper de-dups by (task_type, payload['target']) WHERE
resolved_at IS NULL. The lookup runs on every wrapped fetch failure,
so it lives on the repo (not in jobs/_dlq) to keep DB-touching code
in the repository layer per Phase 1's clean-architecture stance.

The query uses SQLite's json_extract on the JSON column. Resolved
rows are excluded so a re-failure for the same target after manual
resolution gets a fresh open row (instead of a phantom "0 attempts"
on a closed one).
"""
from __future__ import annotations

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


def _task(task_type="fetch_price", target="2330.TW", **extra):
    return FailedTask(
        task_type=task_type,
        payload={"target": target, **extra},
        error="HTTPError 502",
    )


class TestFindOpenByTarget:
    def test_returns_matching_open_row(self, repo):
        repo.insert(_task(target="2330.TW"))
        found = repo.find_open_by_target("fetch_price", "2330.TW")
        assert found is not None
        assert found.payload["target"] == "2330.TW"

    def test_returns_none_when_no_match(self, repo):
        repo.insert(_task(target="2330.TW"))
        found = repo.find_open_by_target("fetch_price", "MISSING")
        assert found is None

    def test_filters_by_task_type(self, repo):
        repo.insert(_task(task_type="fetch_price", target="USD"))
        repo.insert(_task(task_type="fetch_fx", target="USD"))
        found = repo.find_open_by_target("fetch_fx", "USD")
        assert found.task_type == "fetch_fx"

    def test_excludes_resolved_rows(self, repo):
        saved = repo.insert(_task(target="2330.TW"))
        repo.mark_resolved(saved.id)
        found = repo.find_open_by_target("fetch_price", "2330.TW")
        assert found is None

    def test_returns_first_open_when_multiple(self, repo):
        # Defensive: per de-dup invariant only one open row per
        # (task_type, target) should ever exist, but if a manual edit
        # creates two we return the first by id (stable lookup).
        a = repo.insert(_task(target="2330.TW"))
        repo.insert(_task(target="2330.TW"))
        found = repo.find_open_by_target("fetch_price", "2330.TW")
        assert found.id == a.id
