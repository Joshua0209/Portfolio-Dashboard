"""Reproducer for invest.ingestion.trade_seeder.

Maps parsed TW + foreign trades into Trade rows in the persistence
layer with source='pdf' (TW) or source='pdf-foreign' (foreign).
Idempotent via TradeRepo.replace_for_period.

Public surface:
  SeedResult                          tuple-ish summary
  seed_trades_from_statements(...)    main entry point

The seeder does NOT decide which months to ingest — that's the
orchestrator's job (cutoff logic for pre-Shioaji history vs the
post-cutoff handoff to Shioaji). The seeder just maps and writes.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.domain.trade import Side
from invest.ingestion.foreign_parser import (
    ParsedForeignHolding,
    ParsedForeignStatement,
    ParsedForeignTrade,
)
from invest.ingestion.trade_seeder import (
    SeedResult,
    seed_trades_from_statements,
)
from invest.ingestion.tw_parser import (
    ParsedSecuritiesStatement,
    ParsedTwHolding,
    ParsedTwTrade,
)
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


def _tw_holding(code: str, name: str) -> ParsedTwHolding:
    """Test helper — minimal holding dict for code-resolution input."""
    return ParsedTwHolding(
        type="現股", code=code, name=name, qty=100,
        avg_cost=Decimal("0"), cost=Decimal("0"),
        ref_price=Decimal("0"), mkt_value=Decimal("0"),
        unrealized_pnl=Decimal("0"), unrealized_pct=Decimal("0"),
        cum_dividend=Decimal("0"),
        unrealized_pnl_with_div=Decimal("0"),
        unrealized_pct_with_div=Decimal("0"),
    )


def _tw_trade(name: str, d: date, side: Side = Side.CASH_BUY) -> ParsedTwTrade:
    return ParsedTwTrade(
        date=d, name=name, side=side, qty=100,
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


def _foreign_trade(code: str, d: date, ccy: str = "USD") -> ParsedForeignTrade:
    return ParsedForeignTrade(
        date=d, code=code, market="NASDAQ", exchange="NMS",
        side=Side.CASH_BUY, ccy=ccy, qty=10,
        price=Decimal("100"), gross=Decimal("1000"),
        fee=Decimal("5"), other_fee=Decimal("0"), net_ccy=Decimal("-1005"),
    )


def _foreign_statement(month: str, trades: list[ParsedForeignTrade]) -> ParsedForeignStatement:
    return ParsedForeignStatement(
        month=month,
        holdings=(),
        trades=tuple(trades),
        dividends=(),
        cashflow_by_ccy={},
    )


# --- happy path ----------------------------------------------------------


class TestTwSeed:
    def test_writes_tw_trades_with_pdf_source(self, repo):
        stmt = _tw_statement(
            "2024-03",
            holdings=[_tw_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        result = seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
        )
        assert result.tw_inserted == 1
        rows = repo.find_by_source("pdf")
        assert len(rows) == 1
        r = rows[0]
        assert r.code == "2330"
        assert r.source == "pdf"
        assert r.venue == "TW"
        assert r.currency == "TWD"
        assert r.side == Side.CASH_BUY

    def test_resolves_codes_across_multiple_statements(self, repo):
        """INVARIANT: code resolution uses the union of holdings
        across ALL parsed statements. A trade in Jan whose name
        doesn't appear in Jan's holdings can still resolve via
        Feb's holdings."""
        jan = _tw_statement(
            "2024-01",
            holdings=[],  # no holdings in Jan
            trades=[_tw_trade("台積電", date(2024, 1, 15))],
        )
        feb = _tw_statement(
            "2024-02",
            holdings=[_tw_holding("2330", "台積電")],  # name appears in Feb
            trades=[],
        )
        result = seed_trades_from_statements(
            securities=[jan, feb], foreign=[], trade_repo=repo,
        )
        assert result.tw_inserted == 1
        assert result.tw_unresolved_codes == ()
        assert repo.find_by_source("pdf")[0].code == "2330"

    def test_unresolved_name_is_skipped_and_reported(self, repo):
        """INVARIANT: a trade whose name can't be resolved is
        SKIPPED (not written with code='') and its name is reported
        in tw_unresolved_codes. This is the operator's signal to
        add an entry to tw_ticker_map.json."""
        stmt = _tw_statement(
            "2024-03",
            holdings=[_tw_holding("2330", "台積電")],
            trades=[
                _tw_trade("台積電", date(2024, 3, 15)),
                _tw_trade("未知股票", date(2024, 3, 20)),
            ],
        )
        result = seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
        )
        assert result.tw_inserted == 1
        assert "未知股票" in result.tw_unresolved_codes
        assert len(repo.find_by_source("pdf")) == 1

    def test_uses_overrides_file_when_provided(self, repo, tmp_path):
        """INVARIANT: overrides win over holdings (matches tw_naming
        contract). A name with NO holdings binding still resolves
        via overrides."""
        overrides = tmp_path / "tw_overrides.json"
        overrides.write_text('{"邁科": "6831"}')
        stmt = _tw_statement(
            "2024-03",
            holdings=[],
            trades=[_tw_trade("邁科", date(2024, 3, 15))],
        )
        result = seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
            overrides_path=overrides,
        )
        assert result.tw_inserted == 1
        assert repo.find_by_source("pdf")[0].code == "6831"


class TestForeignSeed:
    def test_writes_foreign_trades_with_pdf_foreign_source(self, repo):
        stmt = _foreign_statement(
            "2024-03",
            trades=[_foreign_trade("NVDA", date(2024, 3, 15))],
        )
        result = seed_trades_from_statements(
            securities=[], foreign=[stmt], trade_repo=repo,
        )
        assert result.foreign_inserted == 1
        rows = repo.find_by_source("pdf-foreign")
        assert len(rows) == 1
        r = rows[0]
        assert r.code == "NVDA"
        assert r.source == "pdf-foreign"
        assert r.venue == "US"  # USD → US
        assert r.currency == "USD"

    def test_currency_to_venue_mapping(self, repo):
        """USD → US, HKD → HK, JPY → JP. The mapping is intentional:
        the legacy parser treats venue as ccy-derived since dual-
        listed/ADR cases don't appear in the dataset."""
        stmt = _foreign_statement(
            "2024-03",
            trades=[
                _foreign_trade("0700", date(2024, 3, 15), ccy="HKD"),
                _foreign_trade("7203", date(2024, 3, 16), ccy="JPY"),
            ],
        )
        seed_trades_from_statements(
            securities=[], foreign=[stmt], trade_repo=repo,
        )
        rows = repo.find_by_source("pdf-foreign")
        venues_by_code = {r.code: r.venue for r in rows}
        assert venues_by_code["0700"] == "HK"
        assert venues_by_code["7203"] == "JP"


# --- idempotency --------------------------------------------------------


class TestIdempotency:
    def test_reseeding_replaces_not_duplicates(self, repo):
        """INVARIANT: re-running the seeder with the same parsed
        statements does NOT create duplicate rows. The repo's
        replace_for_period bounds replacement to (source, date
        range) so external sources (shioaji) are untouched."""
        stmt = _tw_statement(
            "2024-03",
            holdings=[_tw_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
        )
        seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
        )
        assert len(repo.find_by_source("pdf")) == 1

    def test_reseed_does_not_touch_other_sources(self, repo, session):
        """INVARIANT: replace_for_period filters on source='pdf'
        (or 'pdf-foreign'). A pre-existing source='shioaji' row
        in the same date range MUST survive a re-seed."""
        from invest.persistence.models.trade import Trade

        # Plant a shioaji row.
        sh = Trade(
            date=date(2024, 3, 20),
            code="2330",
            side=Side.CASH_BUY,
            qty=100,
            price=Decimal("900"),
            currency="TWD",
            source="shioaji",
            venue="TW",
        )
        repo.insert(sh)

        stmt = _tw_statement(
            "2024-03",
            holdings=[_tw_holding("2330", "台積電")],
            trades=[_tw_trade("台積電", date(2024, 3, 15))],
        )
        seed_trades_from_statements(
            securities=[stmt], foreign=[], trade_repo=repo,
        )
        assert len(repo.find_by_source("shioaji")) == 1
        assert len(repo.find_by_source("pdf")) == 1


# --- empty input --------------------------------------------------------


class TestEmpty:
    def test_no_statements_no_writes(self, repo):
        result = seed_trades_from_statements(
            securities=[], foreign=[], trade_repo=repo,
        )
        assert result.tw_inserted == 0
        assert result.foreign_inserted == 0
        assert result.tw_unresolved_codes == ()


# --- result shape -------------------------------------------------------


class TestSeedResultShape:
    def test_result_is_frozen(self, repo):
        result = seed_trades_from_statements(
            securities=[], foreign=[], trade_repo=repo,
        )
        try:
            result.tw_inserted = 999  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("SeedResult must be frozen")
