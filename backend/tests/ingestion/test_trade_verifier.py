"""Reproducer for invest.ingestion.trade_verifier.

The audit half of Phase 4. Same parse pipeline as trade_seeder, but
instead of writing parsed trades it diffs them against existing
Trade rows and emits reconcile_events for divergence.

Match key: (date, code, side, qty) — the same key the legacy
trade_overlay uses for dedup. Price is intentionally NOT part of
the key because PDFs and Shioaji can have rounding differences in
micro-FX-converted prices; price drift would be a separate event
type if/when needed.

Two event types fired:
  pdf_trade_missing_from_shioaji   PDF has it, Shioaji didn't write
  shioaji_trade_missing_from_pdf   Shioaji wrote it, PDF doesn't have

Dry-run mode (apply=False, default): emits events ONLY. The
operator reviews via /today reconcile banner.

Apply mode (apply=True): additionally inserts missing PDF rows
with source='pdf'. NEVER deletes shioaji rows — operator decides
those via dismiss/edit.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.domain.trade import Side
from invest.ingestion.foreign_parser import (
    ParsedForeignStatement,
    ParsedForeignTrade,
)
from invest.ingestion.trade_verifier import (
    VerifyResult,
    verify_trades_against_statements,
)
from invest.ingestion.tw_parser import (
    ParsedSecuritiesStatement,
    ParsedTwHolding,
    ParsedTwTrade,
)
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def trade_repo(session):
    return TradeRepo(session)


@pytest.fixture
def reconcile_repo(session):
    return ReconcileRepo(session)


# Test helpers ----------------------------------------------------------


def _holding(code: str, name: str) -> ParsedTwHolding:
    return ParsedTwHolding(
        type="現股", code=code, name=name, qty=100,
        avg_cost=Decimal("0"), cost=Decimal("0"),
        ref_price=Decimal("0"), mkt_value=Decimal("0"),
        unrealized_pnl=Decimal("0"), unrealized_pct=Decimal("0"),
        cum_dividend=Decimal("0"),
        unrealized_pnl_with_div=Decimal("0"),
        unrealized_pct_with_div=Decimal("0"),
    )


def _tw_trade(name: str, d: date, qty: int = 100, side: Side = Side.CASH_BUY) -> ParsedTwTrade:
    return ParsedTwTrade(
        date=d, name=name, side=side, qty=qty,
        price=Decimal("100"), gross=Decimal("10000"),
        fee=Decimal("14"), tax=Decimal("0"), net_twd=Decimal("-10014"),
    )


def _tw_statement(
    month: str, holdings: list[ParsedTwHolding], trades: list[ParsedTwTrade]
) -> ParsedSecuritiesStatement:
    return ParsedSecuritiesStatement(
        month=month,
        holdings=tuple(holdings),
        subtotal=None,
        trades=tuple(trades),
        rebates=(),
        net_cashflow_twd=Decimal("0"),
    )


def _trade_row(
    *, code: str, d: date, source: str = "shioaji",
    qty: int = 100, side: Side = Side.CASH_BUY, venue: str = "TW",
    currency: str = "TWD",
) -> Trade:
    return Trade(
        date=d, code=code, side=int(side), qty=qty,
        price=Decimal("100"), currency=currency,
        source=source, venue=venue,
    )


# --- happy paths -------------------------------------------------------


class TestPerfectMatch:
    def test_pdf_matches_shioaji_no_events(self, trade_repo, reconcile_repo):
        """PDF says (2024-03-15, 2330, CASH_BUY, 100); Shioaji wrote
        the same. Match → no reconcile events, dry-run reports
        matched=1."""
        trade_repo.insert(_trade_row(code="2330", d=date(2024, 3, 15)))

        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        result = verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result.diff.matched == 1
        assert result.diff.pdf_only == ()
        assert result.diff.shioaji_only == ()
        assert result.events_inserted == 0
        assert reconcile_repo.find_open() == []


# --- divergence: PDF-only ----------------------------------------------


class TestPdfOnly:
    def test_pdf_trade_missing_from_shioaji_emits_event(
        self, trade_repo, reconcile_repo
    ):
        """PDF has trade; Shioaji table is empty → fire
        pdf_trade_missing_from_shioaji event."""
        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        result = verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert len(result.diff.pdf_only) == 1
        assert result.diff.pdf_only[0].code == "2330"
        assert result.events_inserted == 1

        events = reconcile_repo.find_open()
        assert len(events) == 1
        assert events[0].event_type == "pdf_trade_missing_from_shioaji"
        assert events[0].pdf_month == "2024-03"
        assert events[0].detail["code"] == "2330"
        assert events[0].detail["date"] == "2024-03-15"

    def test_dry_run_does_not_insert_trade_row(self, trade_repo, reconcile_repo):
        """INVARIANT: dry-run mode (apply=False) emits events but
        does NOT write Trade rows. Only the reconcile_event is
        persisted."""
        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
            apply=False,
        )
        # No Trade rows were inserted in dry-run.
        assert trade_repo.find_by_source("pdf") == []

    def test_apply_mode_inserts_missing_pdf_row(self, trade_repo, reconcile_repo):
        """INVARIANT: apply=True inserts pdf_only rows with
        source='pdf'. The reconcile event is still emitted for
        audit trail."""
        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
            apply=True,
        )
        rows = trade_repo.find_by_source("pdf")
        assert len(rows) == 1
        assert rows[0].code == "2330"
        # Event still recorded for audit even in apply mode.
        assert len(reconcile_repo.find_open()) == 1


# --- divergence: Shioaji-only ------------------------------------------


class TestShioajiOnly:
    def test_shioaji_trade_missing_from_pdf_emits_event(
        self, trade_repo, reconcile_repo
    ):
        """Shioaji has a trade; PDF for that month doesn't list it.
        Could be: PDF format change, parser bug, or genuine Shioaji
        data error. Fire event for operator review."""
        trade_repo.insert(_trade_row(code="9999", d=date(2024, 3, 15)))

        # PDF for the SAME month exists but has no trades.
        stmt = _tw_statement("2024-03", holdings=[], trades=[])
        result = verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert len(result.diff.shioaji_only) == 1
        assert result.diff.shioaji_only[0].code == "9999"
        events = reconcile_repo.find_open()
        assert len(events) == 1
        assert events[0].event_type == "shioaji_trade_missing_from_pdf"

    def test_apply_mode_does_not_delete_shioaji_only(
        self, trade_repo, reconcile_repo
    ):
        """INVARIANT: apply mode NEVER deletes shioaji_only rows.
        The operator decides what to do via /today reconcile
        banner — automatic deletion would risk data loss on a
        legitimate Shioaji trade that the PDF parser missed."""
        trade_repo.insert(_trade_row(code="9999", d=date(2024, 3, 15)))
        stmt = _tw_statement("2024-03", holdings=[], trades=[])
        verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
            apply=True,
        )
        # Shioaji row survives.
        assert len(trade_repo.find_by_source("shioaji")) == 1


# --- match-key invariants ----------------------------------------------


class TestMatchKey:
    def test_match_ignores_price_difference(self, trade_repo, reconcile_repo):
        """INVARIANT: match key is (date, code, side, qty). Price
        is NOT part of the key — Shioaji and PDF can differ in
        micro-FX-rounded prices for the same trade. If we keyed on
        price, every trade would mismatch."""
        # Shioaji row has price 850; PDF will have price 100 (helper
        # default). Same date/code/side/qty.
        sh = _trade_row(code="2330", d=date(2024, 3, 15), qty=100)
        sh.price = Decimal("850")
        trade_repo.insert(sh)

        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15), qty=100)],
        )
        result = verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result.diff.matched == 1
        assert result.diff.pdf_only == ()
        assert result.diff.shioaji_only == ()

    def test_qty_difference_does_not_match(self, trade_repo, reconcile_repo):
        """Conversely: same date/code/side but different qty IS a
        mismatch. Shioaji 100 vs PDF 200 → both pdf_only AND
        shioaji_only events fire."""
        trade_repo.insert(_trade_row(code="2330", d=date(2024, 3, 15), qty=100))
        stmt = _tw_statement(
            "2024-03",
            holdings=[_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15), qty=200)],
        )
        result = verify_trades_against_statements(
            securities=[stmt], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert len(result.diff.pdf_only) == 1
        assert len(result.diff.shioaji_only) == 1


# --- foreign track -----------------------------------------------------


class TestForeign:
    def test_pdf_foreign_only_emits_event(self, trade_repo, reconcile_repo):
        """Foreign trades: source='pdf-foreign' on insert (apply mode)."""
        ftrade = ParsedForeignTrade(
            date=date(2024, 3, 15), code="NVDA", market="NASDAQ",
            exchange="NMS", side=Side.CASH_BUY, ccy="USD", qty=10,
            price=Decimal("500"), gross=Decimal("5000"),
            fee=Decimal("5"), other_fee=Decimal("0"), net_ccy=Decimal("-5005"),
        )
        fstmt = ParsedForeignStatement(
            month="2024-03",
            holdings=(), trades=(ftrade,), dividends=(),
            cashflow_by_ccy={},
        )
        result = verify_trades_against_statements(
            securities=[], foreign=[fstmt],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
            apply=True,
        )
        assert result.diff.pdf_only[0].code == "NVDA"
        rows = trade_repo.find_by_source("pdf-foreign")
        assert len(rows) == 1
        assert rows[0].venue == "US"


# --- scope: only diffs months in input -------------------------------


class TestScope:
    def test_only_diffs_months_present_in_input(
        self, trade_repo, reconcile_repo
    ):
        """INVARIANT: the verifier only diffs months for which we
        HAVE a parsed statement. A shioaji row in March must NOT
        fire an event when only the February PDF is in input —
        we can't audit a month we haven't parsed."""
        trade_repo.insert(_trade_row(code="2330", d=date(2024, 3, 15)))

        feb = _tw_statement("2024-02", holdings=[], trades=[])
        result = verify_trades_against_statements(
            securities=[feb], foreign=[],
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        # March's shioaji row is OUT OF SCOPE for the Feb-only audit.
        assert result.diff.shioaji_only == ()
        assert reconcile_repo.find_open() == []
