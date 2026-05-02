"""Cycle 50 RED — pin invest.jobs.verify_month contract.

Thin wrapper around invest.ingestion.trade_verifier — adapts the
(securities, foreign, trade_repo, reconcile_repo) call shape to a
(session, month, securities, foreign) shape suitable for CLI shims
and the FastAPI admin endpoint.

The wrapper does NOT parse PDFs — that's the caller's job (the CLI
shim discovers files in sinopac_pdfs/decrypted/, the admin endpoint
takes pre-parsed inputs). Keeping PDF discovery out of the jobs layer
makes the wrapper trivial to test without test fixtures of real
encrypted PDFs.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.ingestion import trade_verifier
from invest.jobs import verify_month


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@dataclass(frozen=True)
class _FakeDiff:
    matched: int
    pdf_only: tuple
    shioaji_only: tuple


@dataclass(frozen=True)
class _FakeResult:
    diff: _FakeDiff
    events_inserted: int


class TestRunSummaryEnvelope:
    def test_returns_summary_envelope(self, session, monkeypatch):
        captured: dict = {}

        def fake_verify(**kwargs):
            captured.update(kwargs)
            return _FakeResult(
                diff=_FakeDiff(matched=5, pdf_only=("p",), shioaji_only=()),
                events_inserted=1,
            )

        monkeypatch.setattr(
            trade_verifier, "verify_trades_against_statements", fake_verify
        )

        result = verify_month.run(
            session,
            month="2026-04",
            securities=[],
            foreign=[],
            apply=False,
        )
        assert result == {
            "month": "2026-04",
            "matched": 5,
            "pdf_only": 1,
            "shioaji_only": 0,
            "events_inserted": 1,
            "applied": False,
        }

    def test_passes_repos_lifted_from_session(self, session, monkeypatch):
        # The wrapper should construct TradeRepo and ReconcileRepo from
        # the session — not require the caller to pass them.
        captured: dict = {}

        def fake_verify(**kwargs):
            captured.update(kwargs)
            return _FakeResult(
                diff=_FakeDiff(matched=0, pdf_only=(), shioaji_only=()),
                events_inserted=0,
            )

        monkeypatch.setattr(
            trade_verifier, "verify_trades_against_statements", fake_verify
        )

        verify_month.run(
            session, month="2026-04", securities=[], foreign=[]
        )
        assert "trade_repo" in captured
        assert "reconcile_repo" in captured
        assert captured["trade_repo"].session is session
        assert captured["reconcile_repo"].session is session

    def test_apply_flag_propagates(self, session, monkeypatch):
        captured: dict = {}

        def fake_verify(**kwargs):
            captured.update(kwargs)
            return _FakeResult(
                diff=_FakeDiff(matched=0, pdf_only=(), shioaji_only=()),
                events_inserted=0,
            )

        monkeypatch.setattr(
            trade_verifier, "verify_trades_against_statements", fake_verify
        )

        result = verify_month.run(
            session,
            month="2026-04",
            securities=[],
            foreign=[],
            apply=True,
        )
        assert captured["apply"] is True
        assert result["applied"] is True


class TestRunMonthValidation:
    @pytest.mark.parametrize("bad", ["2026-13", "20260401", "april-2026", ""])
    def test_rejects_malformed_month(self, session, bad):
        with pytest.raises(ValueError, match="month"):
            verify_month.run(session, month=bad, securities=[], foreign=[])

    def test_accepts_well_formed_month(self, session, monkeypatch):
        monkeypatch.setattr(
            trade_verifier,
            "verify_trades_against_statements",
            lambda **kw: _FakeResult(
                diff=_FakeDiff(matched=0, pdf_only=(), shioaji_only=()),
                events_inserted=0,
            ),
        )
        # No raise.
        verify_month.run(session, month="2026-04", securities=[], foreign=[])
