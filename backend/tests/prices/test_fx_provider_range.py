"""Phase 14.3b — invest.prices.fx_provider.fetch_and_store_range.

Range version of fetch_and_store_fx. Same DLQ rules as the single-date
function but at the (ccy, range) granularity — one DLQ row per failed
range, not per failed date:

  Outcome A  exception                  -> DLQ insert + bump on retry
  Outcome B  empty, has prior data      -> silent miss
  Outcome C  empty, no prior data       -> DLQ insert ONCE
                                            (no auto-bump on repeat)
  TWD identity                          -> 0 returned, no client call,
                                            no DLQ row, no persistence

Plus: a successful fetch resolves any open DLQ row for the ccy.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.models.fx_rate import FxRate
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.fx_repo import FxRepo
from invest.prices import fx_provider


class StubFxClient:
    """Replays queued behaviors in order. Each behavior is either a
    list of rows (returned) or an Exception instance (raised). After
    the queue drains, returns []."""

    def __init__(self, behaviors=None):
        self.behaviors: list = list(behaviors or [])
        self.calls: list[dict] = []

    def fetch_fx(self, ccy: str, start: str, end: str) -> list[dict]:
        self.calls.append({"ccy": ccy, "start": start, "end": end})
        if not self.behaviors:
            return []
        item = self.behaviors.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def fx_repo(session):
    return FxRepo(session)


@pytest.fixture
def dlq(session):
    return FailedTaskRepo(session)


# --- Happy path ----------------------------------------------------------


def test_happy_path_persists_multiple_rows_and_returns_count(fx_repo, dlq):
    """The range fetch should persist every row the client returns and
    return the count. No DLQ writes when the range is non-empty."""
    client = StubFxClient([[
        {"date": "2026-04-28", "rate": 32.40},
        {"date": "2026-04-29", "rate": 32.45},
        {"date": "2026-04-30", "rate": 32.50},
    ]])
    n = fx_provider.fetch_and_store_range(
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 3
    rates = fx_repo.find_rates("USD", "TWD")
    assert len(rates) == 3
    assert rates[0].rate == Decimal("32.40")
    assert rates[2].rate == Decimal("32.50")
    assert dlq.find_open() == []


# --- Outcome A: exception ------------------------------------------------


def test_exception_inserts_one_dlq_row_and_returns_zero(fx_repo, dlq):
    """A network failure mid-range writes ONE DLQ row keyed on the
    (ccy, range) tuple — not one row per missing date."""
    client = StubFxClient([RuntimeError("HTTPError 502 mid-range")])
    n = fx_provider.fetch_and_store_range(
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    tasks = dlq.find_open()
    assert len(tasks) == 1
    t = tasks[0]
    assert t.task_type == "fetch_fx"
    assert t.payload == {
        "ccy": "USD",
        "start": "2026-04-28",
        "end": "2026-04-30",
    }
    assert "502" in t.error
    assert t.attempts == 1


# --- Outcome C: empty, no prior history ----------------------------------


def test_empty_result_no_prior_inserts_dlq_once(fx_repo, dlq):
    """Range comes back empty for a ccy we've never priced —
    Outcome C: log once, no auto-bump on subsequent calls."""
    client = StubFxClient()  # always returns []

    n = fx_provider.fetch_and_store_range(
        "ZZZ",
        date(2026, 4, 28),
        date(2026, 4, 30),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    tasks = dlq.find_open()
    assert len(tasks) == 1
    assert tasks[0].payload["ccy"] == "ZZZ"
    assert tasks[0].attempts == 1

    # Repeat: must NOT bump attempts (middle-path rule).
    fx_provider.fetch_and_store_range(
        "ZZZ",
        date(2026, 5, 1),
        date(2026, 5, 3),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    tasks = dlq.find_open()
    assert len(tasks) == 1
    assert tasks[0].attempts == 1


# --- Outcome B: empty, has prior history ---------------------------------


def test_empty_result_has_prior_is_silent(fx_repo, dlq):
    """Range empty for a ccy with prior priced data — almost always
    a holiday weekend. No DLQ noise."""
    # Seed prior history.
    fx_provider.fetch_and_store_range(
        "USD",
        date(2026, 4, 21),
        date(2026, 4, 21),
        fx_repo=fx_repo,
        dlq=dlq,
        client=StubFxClient([[{"date": "2026-04-21", "rate": 32.0}]]),
    )

    # Now fetch a range that's empty.
    client = StubFxClient()
    n = fx_provider.fetch_and_store_range(
        "USD",
        date(2026, 4, 25),
        date(2026, 4, 26),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    assert dlq.find_open() == []


# --- TWD identity short-circuit -----------------------------------------


def test_twd_identity_short_circuits_to_zero(fx_repo, dlq):
    """Asking fx_provider for TWD->TWD must return 0 immediately
    without touching the client, the fx_repo, or the DLQ.

    This guards against a transient TWD=X yfinance hiccup ever
    entering the DLQ on a same-currency snapshot.
    """
    client = StubFxClient([RuntimeError("must not be called")])
    n = fx_provider.fetch_and_store_range(
        "TWD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    assert client.calls == []
    assert dlq.find_open() == []
    assert fx_repo.find_rates("TWD", "TWD") == []


# --- Recovery ------------------------------------------------------------


def test_successful_fetch_resolves_open_dlq_row(fx_repo, dlq):
    """If the ccy previously failed, a successful range fetch should
    mark the open DLQ row resolved — otherwise the operator sees a
    stale 'failed' indicator after recovery."""
    dlq.insert(
        FailedTask(
            task_type="fetch_fx",
            payload={
                "ccy": "USD",
                "start": "2026-04-25",
                "end": "2026-04-25",
            },
            error="HTTPError 502 (yesterday)",
        )
    )
    client = StubFxClient([[
        {"date": "2026-04-28", "rate": 32.40},
        {"date": "2026-04-29", "rate": 32.45},
    ]])
    n = fx_provider.fetch_and_store_range(
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 29),
        fx_repo=fx_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 2
    assert dlq.count_open() == 0
