"""Reproducer for invest.prices.fx_provider.fetch_and_store_fx.

Mirrors the PriceService middle-path DLQ rule (Cycle 21) but for
FX rates instead of equity prices:

  Outcome A (exception)               -> DLQ insert + bump on retry
  Outcome B (empty + prior data)      -> silent miss
  Outcome C (empty + no prior data)   -> DLQ insert ONCE, no bump

Plus a recovery: success after a queued failure resolves the DLQ
row.

What's different from PriceService:
  - No .TW/.TWO probe — currencies map 1:1 to Yahoo pairs
  - TWD is an identity short-circuit returning Decimal('1.0')
    without any client call (saves a useless yfinance round-trip
    on every USD-position-in-TWD-account snapshot)
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
    """Replays queued behaviors. Each is either a list of fx rows
    (returned from fetch_fx) or an Exception (raised). After drain,
    returns []."""

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


def _seed_fx(fx_repo, *, ccy: str, on: date, rate: str = "32.0"):
    fx_repo.upsert(
        FxRate(
            date=on,
            base=ccy,
            quote="TWD",
            rate=Decimal(rate),
            source="yfinance",
        )
    )


# --- TWD identity (the cheap short-circuit) ------------------------------


class TestTwdIdentity:
    def test_twd_returns_one_without_client_call(self, fx_repo, dlq):
        """TWD->TWD is unity. Don't waste a fetch (and don't pollute
        the DLQ if yfinance's TWD=X has a transient hiccup)."""
        client = StubFxClient([RuntimeError("boom")])  # would raise

        result = fx_provider.fetch_and_store_fx(
            "TWD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        assert result == Decimal("1")
        assert client.calls == []
        assert dlq.find_open() == []

    def test_twd_does_not_persist_an_fx_row(self, fx_repo, dlq):
        """Identity rate is implicit — don't pollute the FxRate table
        with rows for the unit conversion. Consumers that need 'TWD
        in TWD' can treat it as Decimal(1) at the call site."""
        client = StubFxClient()
        fx_provider.fetch_and_store_fx(
            "TWD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        rates = fx_repo.find_rates("TWD", "TWD")
        assert rates == []


# --- Outcome A: exception ------------------------------------------------


class TestExceptionPath:
    def test_first_exception_inserts_dlq_and_returns_none(
        self, fx_repo, dlq
    ):
        client = StubFxClient([RuntimeError("HTTPError 502 from yfinance")])
        result = fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        assert result is None
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].task_type == "fetch_fx"
        assert tasks[0].payload == {
            "ccy": "USD",
            "date": "2026-04-30",
        }
        assert tasks[0].attempts == 1

    def test_repeated_exception_bumps_existing_row(self, fx_repo, dlq):
        client = StubFxClient(
            [RuntimeError("HTTPError 502"), RuntimeError("HTTPError 503")]
        )
        fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 5, 1),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].attempts == 2
        assert "503" in tasks[0].error


# --- Outcome B: empty + history ------------------------------------------


class TestEmptyWithHistory:
    def test_empty_with_prior_rate_is_silent(self, fx_repo, dlq):
        _seed_fx(fx_repo, ccy="USD", on=date(2026, 4, 29), rate="32.0")
        client = StubFxClient()  # default empty
        result = fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        assert result is None
        assert dlq.find_open() == []


# --- Outcome C: empty + no history (middle path) -------------------------


class TestEmptyWithoutHistory:
    def test_repeated_empty_does_NOT_bump_attempts(self, fx_repo, dlq):
        """An exotic currency that yfinance can't price logs once.
        The /today banner shouldn't escalate just because we keep
        retrying a never-listed pair."""
        client = StubFxClient()
        for d in (
            date(2026, 4, 30),
            date(2026, 5, 1),
            date(2026, 5, 2),
        ):
            fx_provider.fetch_and_store_fx(
                "ZZZ",
                d,
                fx_repo=fx_repo,
                dlq=dlq,
                client=client,
            )
        tasks = dlq.find_open()
        assert len(tasks) == 1
        assert tasks[0].attempts == 1
        assert tasks[0].payload["ccy"] == "ZZZ"


# --- Happy path + recovery ----------------------------------------------


class TestHappyPath:
    def test_persists_fx_row_and_returns_decimal_rate(
        self, fx_repo, dlq
    ):
        client = StubFxClient(
            [[{"date": "2026-04-30", "rate": 32.45}]]
        )
        result = fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        assert result == Decimal("32.45")
        rates = fx_repo.find_rates("USD", "TWD")
        assert len(rates) == 1
        assert rates[0].rate == Decimal("32.45")
        assert rates[0].source == "yfinance"
        assert dlq.find_open() == []

    def test_recovery_resolves_open_dlq(self, fx_repo, dlq):
        dlq.insert(
            FailedTask(
                task_type="fetch_fx",
                payload={"ccy": "USD", "date": "2026-04-29"},
                error="HTTPError 502 (yesterday)",
            )
        )
        client = StubFxClient(
            [[{"date": "2026-04-30", "rate": 32.45}]]
        )
        result = fx_provider.fetch_and_store_fx(
            "USD",
            date(2026, 4, 30),
            fx_repo=fx_repo,
            dlq=dlq,
            client=client,
        )
        assert result == Decimal("32.45")
        assert dlq.count_open() == 0
