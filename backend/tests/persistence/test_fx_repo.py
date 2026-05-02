from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.fx_rate import FxRate
from invest.persistence.repositories.fx_repo import FxRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return FxRepo(session)


def _rate(**overrides) -> FxRate:
    defaults = dict(
        date=date(2026, 5, 1),
        base="USD",
        quote="TWD",
        rate=Decimal("32.5"),
        source="yahoo",
    )
    defaults.update(overrides)
    return FxRate(**defaults)


class TestUpsert:
    def test_first_upsert_inserts(self, repo):
        saved = repo.upsert(_rate())
        assert saved.id is not None
        assert saved.rate == Decimal("32.5")

    def test_second_upsert_replaces_same_key(self, repo):
        repo.upsert(_rate(rate=Decimal("32.5")))
        repo.upsert(_rate(rate=Decimal("32.7")))  # corrected rate
        assert repo.find_rate(date(2026, 5, 1), "USD", "TWD") == Decimal("32.7")
        assert len(repo.find_rates("USD", "TWD")) == 1

    def test_different_pair_does_not_collide(self, repo):
        repo.upsert(_rate(base="USD", quote="TWD", rate=Decimal("32.5")))
        repo.upsert(_rate(base="HKD", quote="TWD", rate=Decimal("4.15")))
        assert len(repo.find_rates("USD", "TWD")) == 1
        assert len(repo.find_rates("HKD", "TWD")) == 1


class TestFindRate:
    def test_returns_none_when_missing(self, repo):
        assert repo.find_rate(date(2026, 5, 1), "USD", "TWD") is None

    def test_finds_exact_date(self, repo):
        repo.upsert(_rate(date=date(2026, 4, 30), rate=Decimal("32.4")))
        repo.upsert(_rate(date=date(2026, 5, 1), rate=Decimal("32.5")))
        assert repo.find_rate(date(2026, 5, 1), "USD", "TWD") == Decimal("32.5")
        assert repo.find_rate(date(2026, 4, 30), "USD", "TWD") == Decimal("32.4")


class TestFindRates:
    def test_returns_chronological(self, repo):
        repo.upsert(_rate(date=date(2026, 5, 3), rate=Decimal("32.7")))
        repo.upsert(_rate(date=date(2026, 5, 1), rate=Decimal("32.5")))
        repo.upsert(_rate(date=date(2026, 5, 2), rate=Decimal("32.6")))

        rates = repo.find_rates("USD", "TWD")
        assert [r.date for r in rates] == [
            date(2026, 5, 1),
            date(2026, 5, 2),
            date(2026, 5, 3),
        ]

    def test_filters_by_pair(self, repo):
        repo.upsert(_rate(base="USD", quote="TWD"))
        repo.upsert(_rate(base="HKD", quote="TWD"))
        usd = repo.find_rates("USD", "TWD")
        assert len(usd) == 1
        assert usd[0].base == "USD"


class TestFindRatesInRange:
    def test_filters_by_date_range_inclusive(self, repo):
        for day in (1, 5, 10, 15, 20):
            repo.upsert(_rate(date=date(2026, 5, day)))
        in_range = repo.find_rates_in_range(
            "USD", "TWD",
            start=date(2026, 5, 5),
            end=date(2026, 5, 15),
        )
        days = sorted(r.date.day for r in in_range)
        assert days == [5, 10, 15]
