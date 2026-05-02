from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.reconcile_event import ReconcileEvent
from invest.persistence.repositories.reconcile_repo import ReconcileRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return ReconcileRepo(session)


def _event(**overrides) -> ReconcileEvent:
    defaults = dict(
        pdf_month="2026-04",
        event_type="broker_pdf_buy_leg_mismatch",
        detail={
            "pair_id": 12345,
            "code": "7769",
            "sdk_leg_count": 5,
            "pdf_trade_count": 3,
        },
        status="open",
    )
    defaults.update(overrides)
    return ReconcileEvent(**defaults)


class TestInsert:
    def test_insert_assigns_id_and_detected_at(self, repo):
        saved = repo.insert(_event())
        assert saved.id is not None
        assert saved.detected_at is not None

    def test_detail_round_trips_as_dict(self, repo):
        saved = repo.insert(
            _event(detail={"pair_id": 12345, "code": "7769", "qty": 1000})
        )
        # Re-read from DB
        roundtrip = repo.find_by_id(saved.id)
        assert roundtrip.detail == {"pair_id": 12345, "code": "7769", "qty": 1000}


class TestFindOpen:
    def test_open_events_listed(self, repo):
        repo.insert(_event(status="open"))
        repo.insert(_event(status="open"))
        repo.insert(_event(status="dismissed"))
        assert len(repo.find_open()) == 2

    def test_filters_by_pdf_month(self, repo):
        repo.insert(_event(pdf_month="2026-03"))
        repo.insert(_event(pdf_month="2026-04"))
        repo.insert(_event(pdf_month="2026-04"))
        events = repo.find_open_for_month("2026-04")
        assert len(events) == 2
        assert all(e.pdf_month == "2026-04" for e in events)


class TestDismiss:
    def test_dismiss_changes_status_and_sets_timestamp(self, repo):
        saved = repo.insert(_event())
        repo.dismiss(saved.id)
        after = repo.find_by_id(saved.id)
        assert after.status == "dismissed"
        assert after.dismissed_at is not None

    def test_dismissed_events_excluded_from_find_open(self, repo):
        a = repo.insert(_event())
        b = repo.insert(_event())
        repo.dismiss(a.id)
        open_events = repo.find_open()
        assert len(open_events) == 1
        assert open_events[0].id == b.id

    def test_dismiss_unknown_id_does_nothing(self, repo):
        # No raise, no state change.
        repo.dismiss(99999)
        assert repo.find_open() == []


class TestFindById:
    def test_returns_none_when_missing(self, repo):
        assert repo.find_by_id(99999) is None

    def test_returns_event_when_present(self, repo):
        saved = repo.insert(_event())
        found = repo.find_by_id(saved.id)
        assert found is not None
        assert found.id == saved.id
