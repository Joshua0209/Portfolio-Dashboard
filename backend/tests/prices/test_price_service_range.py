"""Phase 14.3a — invest.prices.price_service.fetch_and_store_range.

Range version of fetch_and_store. Same DLQ rules as the single-date
function but at the (symbol, range) granularity — one DLQ row per
failed range, not per failed date:

  Outcome A  exception                  -> DLQ insert + bump on retry
  Outcome B  empty, has prior data      -> silent miss
  Outcome C  empty, no prior data       -> DLQ insert ONCE
                                            (no auto-bump on repeat)

Plus: a successful fetch resolves any open DLQ row for the symbol.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.price_repo import PriceRepo
from invest.persistence.repositories.symbol_market_repo import (
    SymbolMarketRepo,
)
from invest.prices import price_service


class StubClient:
    """Replays queued behaviors in order. Each behavior is either a
    list of rows (returned) or an Exception instance (raised). After
    the queue drains, returns []."""

    def __init__(self, behaviors=None):
        self.behaviors: list = list(behaviors or [])
        self.calls: list[dict] = []

    def fetch_prices(self, symbol: str, start: str, end: str) -> list[dict]:
        self.calls.append({"symbol": symbol, "start": start, "end": end})
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
def price_repo(session):
    return PriceRepo(session)


@pytest.fixture
def dlq(session):
    return FailedTaskRepo(session)


@pytest.fixture
def market_repo(session):
    return SymbolMarketRepo(session)


# --- Happy path ----------------------------------------------------------


def test_happy_path_persists_multiple_rows_and_returns_count(price_repo, dlq):
    """The range fetch should persist every row the client returns and
    return the count. No DLQ writes when the range is non-empty."""
    client = StubClient([[
        {"date": "2026-04-28", "close": 180.0, "volume": 1_000_000},
        {"date": "2026-04-29", "close": 181.0, "volume": 1_100_000},
        {"date": "2026-04-30", "close": 182.5, "volume": 1_050_000},
    ]])
    n = price_service.fetch_and_store_range(
        "AAPL",
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 3
    prices = price_repo.find_prices("AAPL")
    assert len(prices) == 3
    assert prices[0].close == Decimal("180.0")
    assert prices[2].close == Decimal("182.5")
    assert dlq.find_open() == []


# --- Outcome A: exception ------------------------------------------------


def test_exception_inserts_one_dlq_row_and_returns_zero(price_repo, dlq):
    """A network failure mid-range writes ONE DLQ row keyed on the
    (symbol, range) tuple — not one row per missing date."""
    client = StubClient([RuntimeError("HTTPError 502 mid-range")])
    n = price_service.fetch_and_store_range(
        "AAPL",
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    tasks = dlq.find_open()
    assert len(tasks) == 1
    t = tasks[0]
    assert t.task_type == "fetch_price"
    assert t.payload == {
        "symbol": "AAPL",
        "currency": "USD",
        "start": "2026-04-28",
        "end": "2026-04-30",
    }
    assert "502" in t.error
    assert t.attempts == 1


# --- Outcome C: empty, no prior history ----------------------------------


def test_empty_result_no_prior_inserts_dlq_once(price_repo, dlq):
    """Range comes back empty for a symbol we've never priced —
    Outcome C: log once, no auto-bump on subsequent calls."""
    client = StubClient()  # always returns []

    n = price_service.fetch_and_store_range(
        "ZZZZ",
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    tasks = dlq.find_open()
    assert len(tasks) == 1
    assert tasks[0].payload["symbol"] == "ZZZZ"
    assert tasks[0].attempts == 1

    # Repeat: must NOT bump attempts (middle-path rule).
    price_service.fetch_and_store_range(
        "ZZZZ",
        "USD",
        date(2026, 5, 1),
        date(2026, 5, 3),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    tasks = dlq.find_open()
    assert len(tasks) == 1
    assert tasks[0].attempts == 1


# --- Outcome B: empty, has prior history ---------------------------------


def test_empty_result_has_prior_is_silent(price_repo, dlq):
    """Range empty for a symbol with prior priced data — almost always
    a holiday weekend. No DLQ noise."""
    # Seed prior history.
    price_service.fetch_and_store_range(
        "AAPL",
        "USD",
        date(2026, 4, 21),
        date(2026, 4, 21),
        price_repo=price_repo,
        dlq=dlq,
        client=StubClient([[
            {"date": "2026-04-21", "close": 175.0, "volume": 1},
        ]]),
    )

    # Now fetch a range that's empty.
    client = StubClient()
    n = price_service.fetch_and_store_range(
        "AAPL",
        "USD",
        date(2026, 4, 25),
        date(2026, 4, 26),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 0
    assert dlq.find_open() == []


# --- Recovery ------------------------------------------------------------


def test_successful_fetch_resolves_open_dlq_row(price_repo, dlq):
    """If the symbol previously failed, a successful range fetch should
    mark the open DLQ row resolved — otherwise the operator sees a
    stale 'failed' indicator after recovery."""
    dlq.insert(
        FailedTask(
            task_type="fetch_price",
            payload={
                "symbol": "AAPL",
                "currency": "USD",
                "start": "2026-04-25",
                "end": "2026-04-25",
            },
            error="HTTPError 502 (yesterday)",
        )
    )
    client = StubClient([[
        {"date": "2026-04-28", "close": 180.0, "volume": 1},
        {"date": "2026-04-29", "close": 181.0, "volume": 1},
    ]])
    n = price_service.fetch_and_store_range(
        "AAPL",
        "USD",
        date(2026, 4, 28),
        date(2026, 4, 29),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
    )
    assert n == 2
    assert dlq.count_open() == 0
def test_warrant_empty_range_skips_dlq(price_repo, dlq, market_repo):
    """Taiwan warrant codes (e.g. 042900) may legitimately have no
    trades in a window — empty results are the steady state, not a
    failure. The DLQ must stay empty so the operator isn't paged for
    something that's working as designed."""
    client = StubClient()  # both .TW and .TWO probes empty
    n = price_service.fetch_and_store_range(
        "042900",
        "TWD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
        market_repo=market_repo,
    )
    assert n == 0
    assert dlq.find_open() == []
    assert market_repo.find("042900") is None
def test_warrant_empty_resolves_existing_dlq(price_repo, dlq, market_repo):
    """A DLQ row written before warrant detection existed should be
    auto-resolved on the next backfill — otherwise the operator sees a
    stale 'failed' indicator forever for an expected-empty symbol."""
    dlq.insert(
        FailedTask(
            task_type="fetch_price",
            payload={
                "symbol": "042900",
                "currency": "TWD",
                "start": "2026-03-03",
                "end": "2026-05-03",
            },
            error="no rows for 042900 in [2026-03-03..2026-05-03]; "
                  "symbol may be delisted or unknown to yfinance",
        )
    )
    client = StubClient()  # both probes empty
    n = price_service.fetch_and_store_range(
        "042900",
        "TWD",
        date(2026, 4, 28),
        date(2026, 4, 30),
        price_repo=price_repo,
        dlq=dlq,
        client=client,
        market_repo=market_repo,
    )
    assert n == 0
    assert dlq.count_open() == 0
