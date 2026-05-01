"""Phase 2 reproducer for invest.prices.price_service.fetch_and_store.

Pins the *middle-path* DLQ rule chosen during the planning pause:

  Outcome A  exception                   -> DLQ insert + bump on retry
  Outcome B  empty, has prior data       -> silent miss
  Outcome C  empty, no prior data        -> DLQ insert ONCE
                                            (no auto-bump on repeat)

Plus: a successful fetch resolves any open DLQ row for the same symbol
(otherwise the operator sees stale "missing" rows indefinitely after a
transient outage clears).

Composes invest.prices.yfinance_client (stubbed here) + PriceRepo +
FailedTaskRepo. The client is duck-typed: anything with a
fetch_prices(symbol, start, end) method works.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.models.price import Price
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.price_repo import PriceRepo
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


def _seed_price(price_repo: PriceRepo, *, symbol: str, on: date, close: str = "100"):
    price_repo.upsert(
        Price(
            date=on,
            symbol=symbol,
            close=Decimal(close),
            currency="USD",
            source="yfinance",
        )
    )


# --- Outcome A: exception ------------------------------------------------


class TestExceptionPath:
    def test_first_exception_inserts_dlq_row_and_returns_none(
        self, price_repo, dlq
    ):
        client = StubClient([RuntimeError("HTTPError 502 from yfinance")])
        result = price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        assert result is None
        tasks = dlq.find_open()
        assert len(tasks) == 1
        t = tasks[0]
        assert t.task_type == "fetch_price"
        assert t.payload == {
            "symbol": "AAPL",
            "currency": "USD",
            "date": "2026-04-30",
        }
        assert "502" in t.error
        assert t.attempts == 1

    def test_second_exception_bumps_existing_row(self, price_repo, dlq):
        """Same symbol fails again -> existing row, attempts=2, latest
        error message overwrites. No new row."""
        client = StubClient(
            [RuntimeError("HTTPError 502"), RuntimeError("HTTPError 503")]
        )
        price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 5, 1),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].attempts == 2
        assert "503" in tasks[0].error  # latest error wins


# --- Outcome B: empty result, symbol has prior history -------------------


class TestEmptyWithHistory:
    def test_empty_after_prior_success_is_silent(self, price_repo, dlq):
        """yfinance returns [] for a symbol we've priced before — almost
        always a holiday or a non-trading day. Don't pollute the DLQ."""
        _seed_price(
            price_repo, symbol="AAPL", on=date(2026, 4, 29), close="180"
        )
        client = StubClient()  # default empty
        result = price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        assert result is None
        assert dlq.find_open() == []


# --- Outcome C: empty, no prior history (the middle path) ----------------


class TestEmptyWithoutHistory:
    def test_first_empty_with_no_history_inserts_dlq_row(
        self, price_repo, dlq
    ):
        client = StubClient()
        result = price_service.fetch_and_store(
            "ZZZZ",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        assert result is None
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].payload["symbol"] == "ZZZZ"
        assert tasks[0].attempts == 1

    def test_repeated_empty_does_NOT_bump_attempts(self, price_repo, dlq):
        """The middle-path rule: an unknown symbol logs once. Calling
        fetch_and_store every snapshot for the same dead ticker MUST
        NOT bump attempts — that would generate daily noise on the
        /today banner for a permanently-delisted stock."""
        client = StubClient()
        for d in (date(2026, 4, 30), date(2026, 5, 1), date(2026, 5, 2)):
            price_service.fetch_and_store(
                "ZZZZ",
                "USD",
                d,
                price_repo=price_repo,
                dlq=dlq,
                client=client,
            )
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].attempts == 1


# --- Happy path + recovery ----------------------------------------------


class TestHappyPath:
    def test_returns_decimal_close_and_persists_with_currency(
        self, price_repo, dlq
    ):
        client = StubClient(
            [[{"date": "2026-04-30", "close": 180.5, "volume": 1_000_000}]]
        )
        result = price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        assert result == Decimal("180.5")
        prices = price_repo.find_prices("AAPL")
        assert len(prices) == 1
        assert prices[0].close == Decimal("180.5")
        assert prices[0].currency == "USD"
        assert prices[0].source == "yfinance"
        assert prices[0].date == date(2026, 4, 30)
        assert dlq.find_open() == []

    def test_recovery_resolves_open_dlq_row(self, price_repo, dlq):
        """If the symbol previously failed (DLQ row open), a successful
        fetch should mark it resolved — otherwise the operator sees a
        stuck row indefinitely. Without this, the /today banner would
        light up every snapshot until the operator manually dismisses."""
        dlq.insert(
            FailedTask(
                task_type="fetch_price",
                payload={
                    "symbol": "AAPL",
                    "currency": "USD",
                    "date": "2026-04-29",
                },
                error="HTTPError 502 (yesterday)",
            )
        )
        client = StubClient(
            [[{"date": "2026-04-30", "close": 181.2, "volume": 500_000}]]
        )
        result = price_service.fetch_and_store(
            "AAPL",
            "USD",
            date(2026, 4, 30),
            price_repo=price_repo,
            dlq=dlq,
            client=client,
        )
        assert result == Decimal("181.2")
        assert dlq.count_open() == 0
