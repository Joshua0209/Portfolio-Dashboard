from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.domain.trade import Side
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.trade_repo import TradeRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return TradeRepo(session)


def _make_trade(**overrides) -> Trade:
    defaults = dict(
        date=date(2026, 5, 1),
        code="2330",
        side=Side.CASH_BUY,
        qty=1000,
        price=Decimal("920.00"),
        currency="TWD",
        source="pdf",
        venue="TW",
    )
    defaults.update(overrides)
    return Trade(**defaults)


class TestInsert:
    def test_insert_assigns_synthetic_id(self, repo):
        saved = repo.insert(_make_trade())
        assert saved.id is not None

    def test_insert_two_identical_fills_creates_two_rows(self, repo):
        # Scaling into a position with two identical fills on the same
        # day at the same price/qty/side is legitimate — synthetic PK
        # must allow it (the natural tuple cannot distinguish them).
        repo.insert(_make_trade())
        repo.insert(_make_trade())
        rows = repo.find_by_code("2330")
        assert len(rows) == 2
        assert rows[0].id != rows[1].id

    def test_insert_persists_all_columns(self, repo):
        saved = repo.insert(
            _make_trade(
                code="2454",
                side=Side.MARGIN_BUY,
                qty=500,
                price=Decimal("1180.50"),
                fee=Decimal("84.21"),
                tax=Decimal("0"),
                rebate=Decimal("12.00"),
                source="shioaji",
            )
        )
        assert saved.code == "2454"
        assert saved.side == Side.MARGIN_BUY
        assert saved.qty == 500
        assert saved.price == Decimal("1180.50")
        assert saved.fee == Decimal("84.21")
        assert saved.rebate == Decimal("12.00")
        assert saved.source == "shioaji"
        assert saved.ingested_at is not None


class TestFind:
    def test_find_by_month_returns_only_that_month(self, repo):
        repo.insert(_make_trade(date=date(2026, 4, 15)))
        repo.insert(_make_trade(date=date(2026, 5, 1)))
        repo.insert(_make_trade(date=date(2026, 5, 31)))
        repo.insert(_make_trade(date=date(2026, 6, 1)))

        rows = repo.find_by_month("2026-05")
        assert len(rows) == 2
        assert all(r.date.month == 5 for r in rows)

    def test_find_by_month_handles_december_year_boundary(self, repo):
        repo.insert(_make_trade(date=date(2026, 12, 31)))
        repo.insert(_make_trade(date=date(2027, 1, 1)))

        rows = repo.find_by_month("2026-12")
        assert len(rows) == 1
        assert rows[0].date == date(2026, 12, 31)

    def test_find_by_code_returns_only_that_code(self, repo):
        repo.insert(_make_trade(code="2330"))
        repo.insert(_make_trade(code="2454"))
        rows = repo.find_by_code("2330")
        assert len(rows) == 1
        assert rows[0].code == "2330"

    def test_find_since_returns_rows_on_or_after_date(self, repo):
        repo.insert(_make_trade(date=date(2026, 4, 30)))
        repo.insert(_make_trade(date=date(2026, 5, 1)))
        repo.insert(_make_trade(date=date(2026, 5, 2)))
        rows = repo.find_since(date(2026, 5, 1))
        assert len(rows) == 2

    def test_find_by_source_filters_by_lineage(self, repo):
        repo.insert(_make_trade(source="pdf"))
        repo.insert(_make_trade(source="shioaji"))
        repo.insert(_make_trade(source="pdf"))
        rows = repo.find_by_source("pdf")
        assert len(rows) == 2
        assert all(r.source == "pdf" for r in rows)


class TestReplaceForPeriod:
    """Idempotency contract: writers truncate-and-replace by (source, period).

    Compensates for the absence of a source_ref column: if a writer is
    re-run for the same window, replace_for_period guarantees no duplicates
    and no leftover rows from a prior, possibly-buggier parse.
    """

    def test_replaces_existing_rows_in_window(self, repo):
        repo.insert(_make_trade(date=date(2026, 5, 1), source="pdf", qty=1000))
        repo.insert(_make_trade(date=date(2026, 5, 15), source="pdf", qty=1000))

        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[
                _make_trade(date=date(2026, 5, 1), source="pdf", qty=2000),
                _make_trade(date=date(2026, 5, 15), source="pdf", qty=2000),
            ],
        )

        rows = repo.find_by_month("2026-05")
        assert len(rows) == 2
        assert all(r.qty == 2000 for r in rows)

    def test_does_not_touch_other_sources(self, repo):
        repo.insert(_make_trade(date=date(2026, 5, 1), source="pdf"))
        repo.insert(_make_trade(date=date(2026, 5, 1), source="shioaji"))

        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[],
        )

        all_rows = repo.find_by_month("2026-05")
        assert len(all_rows) == 1
        assert all_rows[0].source == "shioaji"

    def test_does_not_touch_rows_outside_window(self, repo):
        repo.insert(_make_trade(date=date(2026, 4, 30), source="pdf"))
        repo.insert(_make_trade(date=date(2026, 5, 15), source="pdf"))
        repo.insert(_make_trade(date=date(2026, 6, 1), source="pdf"))

        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[],
        )

        rows = repo.find_by_source("pdf")
        dates = sorted(r.date for r in rows)
        assert dates == [date(2026, 4, 30), date(2026, 6, 1)]

    def test_inclusive_window_boundaries(self, repo):
        # start and end dates themselves must be inside the window.
        repo.insert(_make_trade(date=date(2026, 5, 1), source="pdf"))
        repo.insert(_make_trade(date=date(2026, 5, 31), source="pdf"))

        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[],
        )

        assert repo.find_by_source("pdf") == []
