"""Cycle 48 RED — pin invest.jobs.backfill state-machine + spawn contract.

Phase 7 splits app/backfill_runner.py's daemon-thread wrapper into
invest.jobs.backfill. The wrapper's job is to:

  1. Mark daily-state INITIALIZING for the duration of the run.
  2. Call a fetch_orchestrator to populate Price/FxRate rows.
  3. Call invest.jobs._positions.build_daily to materialize the
     daily layer.
  4. Mark daily-state READY on success, FAILED on exception.
  5. Singleton-thread guard: a second start() call while a worker
     is running must return the live thread instead of spawning.

The fetch orchestrator is dependency-injected (Protocol-typed) so
tests don't need yfinance. Phase 2's invest.prices.price_service
and invest.prices.fx_provider satisfy the orchestrator contract for
real runs (Cycle 51 wires them up).
"""
from __future__ import annotations

import threading
import time
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.core import state as state_module
from invest.jobs import _positions, backfill
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.price import Price
from invest.persistence.models.trade import Trade


CASH_BUY = 1


@pytest.fixture(autouse=True)
def _reset_state():
    state_module._singleton = None
    yield
    state_module._singleton = None


@pytest.fixture
def engine():
    # StaticPool shares the in-memory DB across every Session opened
    # against this engine — required when the worker thread's session
    # must see tables created on the test thread's session.
    e = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(e)
    return e


@pytest.fixture
def session(engine):
    with Session(engine) as s:
        yield s


def _seed_one_day(session, d: date):
    session.add(
        Trade(
            date=d, code="2330", side=CASH_BUY, qty=100,
            price=Decimal("500"), currency="TWD",
            source="shioaji", venue="TW",
        )
    )
    session.add(
        Price(
            date=d, symbol="2330", close=Decimal("510"),
            currency="TWD", source="yfinance",
        )
    )
    session.commit()


# ---------------------------------------------------------------------------
# run_full_backfill — state transitions + orchestrator hand-off
# ---------------------------------------------------------------------------


class TestRunFullBackfillState:
    def test_marks_initializing_during_run(self, session):
        observed_states: list[str] = []

        def orchestrator(s, start, end):
            observed_states.append(state_module.get().snapshot()["state"])

        backfill.run_full_backfill(
            session,
            start=date(2026, 1, 1),
            end=date(2026, 1, 1),
            fetch_orchestrator=orchestrator,
        )

        assert observed_states == ["INITIALIZING"]

    def test_marks_ready_after_success(self, session):
        backfill.run_full_backfill(
            session,
            start=date(2026, 1, 1),
            end=date(2026, 1, 1),
            fetch_orchestrator=lambda s, start, end: None,
        )

        assert state_module.get().snapshot()["state"] == "READY"

    def test_marks_failed_on_orchestrator_exception(self, session):
        def orchestrator(s, start, end):
            raise RuntimeError("yfinance unreachable")

        with pytest.raises(RuntimeError, match="yfinance unreachable"):
            backfill.run_full_backfill(
                session,
                start=date(2026, 1, 1),
                end=date(2026, 1, 1),
                fetch_orchestrator=orchestrator,
            )

        snap = state_module.get().snapshot()
        assert snap["state"] == "FAILED"
        assert "yfinance unreachable" in snap["error"]

    def test_marks_failed_on_build_daily_exception(
        self, session, monkeypatch
    ):
        # If build_daily itself raises (e.g. corrupt schema), the
        # state machine must still flip to FAILED rather than leaving
        # the daily-state stuck on INITIALIZING.
        def boom(*args, **kwargs):
            raise RuntimeError("schema corrupt")

        monkeypatch.setattr(_positions, "build_daily", boom)

        with pytest.raises(RuntimeError, match="schema corrupt"):
            backfill.run_full_backfill(
                session,
                start=date(2026, 1, 1),
                end=date(2026, 1, 1),
                fetch_orchestrator=lambda s, start, end: None,
            )

        snap = state_module.get().snapshot()
        assert snap["state"] == "FAILED"
        assert "schema corrupt" in snap["error"]


class TestRunFullBackfillOrchestrator:
    def test_orchestrator_called_with_session_and_window(self, session):
        captured: dict = {}

        def orchestrator(s, start, end):
            captured["session"] = s
            captured["start"] = start
            captured["end"] = end

        backfill.run_full_backfill(
            session,
            start=date(2026, 1, 1),
            end=date(2026, 1, 31),
            fetch_orchestrator=orchestrator,
        )

        assert captured["session"] is session
        assert captured["start"] == date(2026, 1, 1)
        assert captured["end"] == date(2026, 1, 31)

    def test_writes_portfolio_daily_after_orchestrator(self, session):
        d = date(2026, 1, 5)

        def orchestrator(s, start, end):
            _seed_one_day(s, d)

        result = backfill.run_full_backfill(
            session,
            start=d,
            end=d,
            fetch_orchestrator=orchestrator,
        )

        assert result["portfolio_rows"] >= 1
        port = session.query(PortfolioDaily).all()
        assert len(port) == 1
        assert port[0].date == d


# ---------------------------------------------------------------------------
# data_already_ready — READY shortcut
# ---------------------------------------------------------------------------


class TestDataAlreadyReady:
    def test_empty_portfolio_daily_means_not_ready(self, session):
        assert backfill.data_already_ready(session) is False

    def test_populated_portfolio_daily_means_ready(self, session):
        session.add(
            PortfolioDaily(
                date=date(2026, 1, 5),
                equity=Decimal("100000"),
                cost_basis=Decimal("90000"),
                currency="TWD",
                source="computed",
            )
        )
        session.commit()
        assert backfill.data_already_ready(session) is True


# ---------------------------------------------------------------------------
# start — daemon-thread spawn + singleton guard
# ---------------------------------------------------------------------------


class TestStart:
    def test_no_spawn_when_data_ready(self, engine):
        # Seed PortfolioDaily so data_already_ready returns True.
        with Session(engine) as s:
            s.add(
                PortfolioDaily(
                    date=date(2026, 1, 5),
                    equity=Decimal("100000"),
                    cost_basis=Decimal("90000"),
                    currency="TWD",
                    source="computed",
                )
            )
            s.commit()

        thread = backfill.start(
            session_factory=lambda: Session(engine),
            start=date(2026, 1, 1),
            end=date(2026, 1, 31),
            fetch_orchestrator=lambda s, start, end: None,
        )

        # No thread spawned; state stays READY.
        assert thread is None
        assert state_module.get().snapshot()["state"] == "READY"
        backfill._reset_thread_for_test()

    def test_spawns_when_data_empty(self, engine):
        ran = threading.Event()

        def orchestrator(s, start, end):
            ran.set()

        thread = backfill.start(
            session_factory=lambda: Session(engine),
            start=date(2026, 1, 1),
            end=date(2026, 1, 31),
            fetch_orchestrator=orchestrator,
        )

        assert thread is not None
        thread.join(timeout=5.0)
        assert ran.is_set()
        assert state_module.get().snapshot()["state"] == "READY"
        backfill._reset_thread_for_test()

    def test_singleton_guard_returns_live_thread(self, engine):
        # First call spawns; second call while alive returns same thread.
        gate = threading.Event()
        release = threading.Event()

        def orchestrator(s, start, end):
            gate.set()
            release.wait(timeout=5.0)

        t1 = backfill.start(
            session_factory=lambda: Session(engine),
            start=date(2026, 1, 1),
            end=date(2026, 1, 31),
            fetch_orchestrator=orchestrator,
        )
        gate.wait(timeout=5.0)

        t2 = backfill.start(
            session_factory=lambda: Session(engine),
            start=date(2026, 1, 1),
            end=date(2026, 1, 31),
            fetch_orchestrator=orchestrator,
        )

        assert t2 is t1  # singleton — second call returns the live thread
        release.set()
        t1.join(timeout=5.0)
        backfill._reset_thread_for_test()
