"""Cycle 49 RED — pin invest.jobs.snapshot contract.

Incremental gap-fill, operator-triggered (POST /api/admin/refresh
+ scripts/snapshot.py). Synchronous — does NOT touch the daily-state
machine because:
  - The state machine is specifically about cold-start lifecycle
    (READY/INITIALIZING/FAILED).
  - Snapshot runs against an already-warm daily layer and returns
    counters in the response, like retry_failed.run.

Skip semantics:
  - last_known_date IS NULL: refuse incremental, ask operator to run
    backfill instead. (A snapshot from inception would re-fetch the
    entire history, which is what backfill is for.)
  - last_known_date >= today: already up-to-date, no work.
  - Otherwise: fetch_orchestrator(session, last+1, today) →
    _positions.build_daily.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.jobs import snapshot
from invest.persistence.models.portfolio_daily import PortfolioDaily


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _seed_portfolio(session, d: date, equity: str = "100000"):
    session.add(
        PortfolioDaily(
            date=d, equity=Decimal(equity), cost_basis=Decimal("90000"),
            currency="TWD", source="computed",
        )
    )
    session.commit()


class TestFindLastKnownDate:
    def test_none_when_empty(self, session):
        assert snapshot.find_last_known_date(session) is None

    def test_returns_latest_date(self, session):
        _seed_portfolio(session, date(2026, 1, 5))
        _seed_portfolio(session, date(2026, 1, 7))
        _seed_portfolio(session, date(2026, 1, 6))
        assert snapshot.find_last_known_date(session) == date(2026, 1, 7)


class TestRunIncrementalSkip:
    def test_skip_when_no_prior_data(self, session):
        result = snapshot.run_incremental(
            session,
            today=date(2026, 1, 10),
            fetch_orchestrator=lambda s, start, end: None,
        )
        assert result["skipped_reason"] == "no_prior_data_call_backfill"
        assert result["positions_rows"] == 0
        assert result["portfolio_rows"] == 0

    def test_skip_when_already_up_to_date(self, session):
        d = date(2026, 1, 10)
        _seed_portfolio(session, d)

        result = snapshot.run_incremental(
            session,
            today=d,
            fetch_orchestrator=lambda s, start, end: None,
        )
        assert result["skipped_reason"] == "already_up_to_date"
        assert result["last_known_date"] == d.isoformat()

    def test_skip_when_last_known_in_future(self, session):
        # Defensive: if last_known somehow advances past today (clock
        # skew, manual seed), still skip cleanly.
        _seed_portfolio(session, date(2026, 2, 1))
        result = snapshot.run_incremental(
            session,
            today=date(2026, 1, 10),
            fetch_orchestrator=lambda s, start, end: None,
        )
        assert result["skipped_reason"] == "already_up_to_date"


class TestRunIncrementalFill:
    def test_orchestrator_called_with_gap_window(self, session):
        _seed_portfolio(session, date(2026, 1, 5))
        captured: dict = {}

        def orchestrator(s, start, end):
            captured["start"] = start
            captured["end"] = end

        snapshot.run_incremental(
            session,
            today=date(2026, 1, 10),
            fetch_orchestrator=orchestrator,
        )
        # Gap is (last+1, today)
        assert captured["start"] == date(2026, 1, 6)
        assert captured["end"] == date(2026, 1, 10)

    def test_summary_includes_gap_window(self, session):
        _seed_portfolio(session, date(2026, 1, 5))
        result = snapshot.run_incremental(
            session,
            today=date(2026, 1, 10),
            fetch_orchestrator=lambda s, start, end: None,
        )
        assert result["skipped_reason"] is None
        assert result["gap_start"] == "2026-01-06"
        assert result["gap_end"] == "2026-01-10"
