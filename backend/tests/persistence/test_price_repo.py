from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.price import Price
from invest.persistence.repositories.price_repo import PriceRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return PriceRepo(session)


def _price(**overrides) -> Price:
    defaults = dict(
        date=date(2026, 5, 1),
        symbol="2330",
        close=Decimal("920.00"),
        currency="TWD",
        source="yahoo",
    )
    defaults.update(overrides)
    return Price(**defaults)


class TestUpsert:
    def test_first_upsert_inserts(self, repo):
        saved = repo.upsert(_price())
        assert saved.id is not None
        assert saved.close == Decimal("920.00")

    def test_upsert_replaces_same_date_symbol(self, repo):
        repo.upsert(_price(close=Decimal("920.00")))
        repo.upsert(_price(close=Decimal("925.00")))
        assert repo.find_price(date(2026, 5, 1), "2330") == Decimal("925.00")
        assert len(repo.find_prices("2330")) == 1

    def test_different_symbol_does_not_collide(self, repo):
        repo.upsert(_price(symbol="2330"))
        repo.upsert(_price(symbol="2454"))
        assert len(repo.find_prices("2330")) == 1
        assert len(repo.find_prices("2454")) == 1


class TestFindPrice:
    def test_returns_none_when_missing(self, repo):
        assert repo.find_price(date(2026, 5, 1), "2330") is None

    def test_finds_exact_date_and_symbol(self, repo):
        repo.upsert(_price(date=date(2026, 4, 30), close=Decimal("915")))
        repo.upsert(_price(date=date(2026, 5, 1), close=Decimal("920")))
        assert repo.find_price(date(2026, 5, 1), "2330") == Decimal("920")
        assert repo.find_price(date(2026, 4, 30), "2330") == Decimal("915")


class TestFindPrices:
    def test_returns_chronological_for_one_symbol(self, repo):
        repo.upsert(_price(date=date(2026, 5, 3), close=Decimal("930")))
        repo.upsert(_price(date=date(2026, 5, 1), close=Decimal("920")))
        repo.upsert(_price(date=date(2026, 5, 2), close=Decimal("925")))
        prices = repo.find_prices("2330")
        assert [p.date for p in prices] == [
            date(2026, 5, 1),
            date(2026, 5, 2),
            date(2026, 5, 3),
        ]

    def test_filters_by_symbol(self, repo):
        repo.upsert(_price(symbol="2330"))
        repo.upsert(_price(symbol="AAPL", currency="USD"))
        tw = repo.find_prices("2330")
        assert len(tw) == 1
        assert tw[0].symbol == "2330"


class TestFindPricesInRange:
    def test_filters_by_date_range_inclusive(self, repo):
        for day in (1, 5, 10, 15, 20):
            repo.upsert(_price(date=date(2026, 5, day)))
        in_range = repo.find_prices_in_range(
            "2330",
            start=date(2026, 5, 5),
            end=date(2026, 5, 15),
        )
        days = sorted(p.date.day for p in in_range)
        assert days == [5, 10, 15]


class TestForeignTicker:
    def test_foreign_ticker_uses_local_currency(self, repo):
        repo.upsert(_price(symbol="AAPL", close=Decimal("180.50"), currency="USD"))
        row = repo.find_prices("AAPL")[0]
        assert row.currency == "USD"
