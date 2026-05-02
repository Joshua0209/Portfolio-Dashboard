from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.position_daily import PositionDaily
from invest.persistence.repositories.position_daily_repo import PositionDailyRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return PositionDailyRepo(session)


def _row(**overrides) -> PositionDaily:
    defaults = dict(
        date=date(2026, 5, 1),
        code="2330",
        qty=1000,
        close=Decimal("920.00"),
        currency="TWD",
        market_value=Decimal("920000.00"),
        source="pdf",
    )
    defaults.update(overrides)
    return PositionDaily(**defaults)


class TestUpsert:
    def test_first_upsert_inserts(self, repo):
        saved = repo.upsert(_row())
        assert saved.id is not None

    def test_upsert_replaces_same_date_code_source(self, repo):
        repo.upsert(_row(qty=1000))
        repo.upsert(_row(qty=1500))  # corrected snapshot
        rows = repo.find_by_date(date(2026, 5, 1))
        assert len(rows) == 1
        assert rows[0].qty == 1500

    def test_different_source_does_not_collide(self, repo):
        # The Phase E overlay invariant: pdf and overlay rows for the
        # same (date, code) coexist on different `source` values.
        repo.upsert(_row(source="pdf", qty=1000))
        repo.upsert(_row(source="overlay", qty=1500))
        rows = repo.find_by_date(date(2026, 5, 1))
        assert len(rows) == 2
        sources = sorted(r.source for r in rows)
        assert sources == ["overlay", "pdf"]


class TestReadMethods:
    def test_find_by_date_returns_all_codes(self, repo):
        repo.upsert(_row(date=date(2026, 5, 1), code="2330"))
        repo.upsert(_row(date=date(2026, 5, 1), code="2454"))
        repo.upsert(_row(date=date(2026, 5, 2), code="2330"))
        rows = repo.find_by_date(date(2026, 5, 1))
        assert len(rows) == 2
        codes = sorted(r.code for r in rows)
        assert codes == ["2330", "2454"]

    def test_find_for_code_returns_time_series(self, repo):
        repo.upsert(_row(date=date(2026, 5, 1), code="2330"))
        repo.upsert(_row(date=date(2026, 5, 2), code="2330"))
        repo.upsert(_row(date=date(2026, 5, 1), code="2454"))
        rows = repo.find_for_code("2330")
        assert len(rows) == 2
        assert all(r.code == "2330" for r in rows)
        # chronological
        assert [r.date for r in rows] == [date(2026, 5, 1), date(2026, 5, 2)]


class TestReplaceForPeriod:
    """Same idempotency contract as TradeRepo: writers truncate-and-
    replace by (source, [start, end] inclusive). Critical for the
    Phase E overlay re-write path."""

    def test_replaces_existing_rows_in_window_only(self, repo):
        repo.upsert(_row(date=date(2026, 5, 1), source="pdf", qty=1000))
        repo.upsert(_row(date=date(2026, 5, 15), source="pdf", qty=1000))

        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[
                _row(date=date(2026, 5, 1), source="pdf", qty=2000),
                _row(date=date(2026, 5, 15), source="pdf", qty=2000),
            ],
        )
        rows = repo.find_by_date(date(2026, 5, 1))
        assert rows[0].qty == 2000

    def test_does_not_touch_other_sources(self, repo):
        # PDF re-parse must not delete overlay rows.
        repo.upsert(_row(source="pdf", qty=1000))
        repo.upsert(_row(source="overlay", qty=1500))
        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[],
        )
        rows = repo.find_by_date(date(2026, 5, 1))
        assert len(rows) == 1
        assert rows[0].source == "overlay"

    def test_does_not_touch_outside_window(self, repo):
        repo.upsert(_row(date=date(2026, 4, 30), source="pdf"))
        repo.upsert(_row(date=date(2026, 5, 15), source="pdf"))
        repo.upsert(_row(date=date(2026, 6, 1), source="pdf"))
        repo.replace_for_period(
            source="pdf",
            start=date(2026, 5, 1),
            end=date(2026, 5, 31),
            rows=[],
        )
        all_dates = sorted(
            r.date for r in repo.find_for_code("2330")
        )
        assert all_dates == [date(2026, 4, 30), date(2026, 6, 1)]
