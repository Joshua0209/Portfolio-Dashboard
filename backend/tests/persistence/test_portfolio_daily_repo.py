from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.repositories.portfolio_repo import PortfolioRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return PortfolioRepo(session)


def _row(**overrides) -> PortfolioDaily:
    defaults = dict(
        date=date(2026, 5, 1),
        equity=Decimal("1500000.00"),
        cost_basis=Decimal("1000000.00"),
        currency="TWD",
        source="snapshot",
    )
    defaults.update(overrides)
    return PortfolioDaily(**defaults)


class TestUpsert:
    def test_first_upsert_inserts(self, repo):
        repo.upsert(_row())
        assert repo.find_by_date(date(2026, 5, 1)).equity == Decimal("1500000.00")

    def test_upsert_replaces_same_date(self, repo):
        repo.upsert(_row(equity=Decimal("1500000.00")))
        repo.upsert(_row(equity=Decimal("1525000.00")))  # corrected
        assert repo.find_by_date(date(2026, 5, 1)).equity == Decimal("1525000.00")
        # Still only one row for that date
        assert len(repo.find_in_range(date(2026, 5, 1), date(2026, 5, 1))) == 1


class TestFindByDate:
    def test_returns_none_when_missing(self, repo):
        assert repo.find_by_date(date(2026, 5, 1)) is None

    def test_returns_row_when_present(self, repo):
        repo.upsert(_row())
        row = repo.find_by_date(date(2026, 5, 1))
        assert row is not None
        assert row.equity == Decimal("1500000.00")
        assert row.currency == "TWD"


class TestFindInRange:
    def test_returns_chronological_inclusive(self, repo):
        for day in (1, 5, 10, 15, 20):
            repo.upsert(_row(date=date(2026, 5, day)))
        in_range = repo.find_in_range(date(2026, 5, 5), date(2026, 5, 15))
        days = [r.date.day for r in in_range]
        assert days == [5, 10, 15]


class TestFindLatest:
    def test_none_on_empty(self, repo):
        assert repo.find_latest() is None

    def test_returns_max_date(self, repo):
        repo.upsert(_row(date=date(2026, 5, 1), equity=Decimal("1000000")))
        repo.upsert(_row(date=date(2026, 5, 15), equity=Decimal("1500000")))
        repo.upsert(_row(date=date(2026, 5, 7), equity=Decimal("1200000")))
        latest = repo.find_latest()
        assert latest.date == date(2026, 5, 15)
        assert latest.equity == Decimal("1500000")


class TestFindAllDates:
    def test_empty(self, repo):
        assert repo.find_all_dates() == []

    def test_returns_sorted(self, repo):
        for day in (3, 1, 2):
            repo.upsert(_row(date=date(2026, 5, day)))
        assert repo.find_all_dates() == [
            date(2026, 5, 1),
            date(2026, 5, 2),
            date(2026, 5, 3),
        ]
