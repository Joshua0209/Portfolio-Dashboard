"""Phase 11 - Trade-table backfill from data/portfolio.json.

The Trade SQLModel table is the long-term source of truth for the
Shioaji-canonical PLAN section 4 design. Phase 11 starts the migration
by populating the table from the parsed-PDF aggregate (the same data
PortfolioStore exposes today via summary.all_trades). Analytics keep
reading PortfolioStore until the per-metric byte-equality verifier
ships in Phase 11.2.

Idempotency contract:
  - Re-running the backfill against the same portfolio.json yields the
    same row count for source='pdf' rows.
  - source='overlay' rows (written by trade_overlay) are NEVER touched.
    Same invariant pattern as positions_daily - PDFs are canonical for
    historical trades, overlay is canonical for post-PDF broker
    activity, and neither writer crosses the boundary.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from invest.domain.trade import Side
from invest.jobs import trade_backfill
from invest.persistence.models.trade import Trade


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


class TestSideMapping:
    @pytest.mark.parametrize("side_str,expected", [
        ("普買", Side.CASH_BUY),
        ("普賣", Side.CASH_SELL),
        ("資買", Side.MARGIN_BUY),
        ("資賣", Side.MARGIN_SELL),
        ("櫃買", Side.CASH_BUY),
        ("櫃賣", Side.CASH_SELL),
        ("買進", Side.CASH_BUY),
        ("賣出", Side.CASH_SELL),
    ])
    def test_known_strings(self, side_str, expected):
        assert trade_backfill.side_from_string(side_str) is expected

    def test_unknown_string_raises(self):
        with pytest.raises(ValueError, match="unknown side"):
            trade_backfill.side_from_string("foobar")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            trade_backfill.side_from_string("")


def _portfolio(*trades) -> dict:
    return {"summary": {"all_trades": list(trades)}}


def _tw_trade(date_str, code, side, qty, price):
    return {
        "date": date_str, "venue": "TW", "side": side, "code": code,
        "qty": qty, "price": price, "ccy": "TWD",
        "fee_twd": 75.0, "tax_twd": 0.0,
    }


def _foreign_trade(date_str, code, side, qty, price):
    return {
        "date": date_str, "venue": "Foreign", "side": side, "code": code,
        "qty": qty, "price": price, "ccy": "USD",
        "fee_twd": 21.43, "tax_twd": 0.0,
    }


CASH_BUY_TW = "普買"
CASH_SELL_TW = "普賣"
OTC_BUY_TW = "櫃買"
FOREIGN_BUY = "買進"


class TestBackfillEmpty:
    def test_empty_portfolio_inserts_zero(self, session):
        result = trade_backfill.run(session, _portfolio())
        assert result["pdf_rows_inserted"] == 0
        assert result["pdf_rows_deleted"] == 0
        assert session.exec(select(Trade)).all() == []

    def test_missing_summary_inserts_zero(self, session):
        result = trade_backfill.run(session, {})
        assert result["pdf_rows_inserted"] == 0


class TestBackfillBasic:
    def test_single_tw_trade_persisted(self, session):
        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0)),
        )
        rows = session.exec(select(Trade)).all()
        assert len(rows) == 1
        t = rows[0]
        assert t.date == date(2026, 4, 15)
        assert t.code == "2330"
        assert t.side == int(Side.CASH_BUY)
        assert t.qty == 1000
        assert t.price == Decimal("600.0")
        assert t.currency == "TWD"
        assert t.venue == "TW"
        assert t.source == "pdf"

    def test_iso_date_format_accepted(self, session):
        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026/04/15", "2330", CASH_BUY_TW, 1000, 600.0)),
        )
        t = session.exec(select(Trade)).one()
        assert t.date == date(2026, 4, 15)

    def test_foreign_trade_uses_usd(self, session):
        trade_backfill.run(
            session,
            _portfolio(_foreign_trade("2026-04-15", "NVDA", FOREIGN_BUY, 4, 174.0)),
        )
        t = session.exec(select(Trade)).one()
        assert t.code == "NVDA"
        assert t.currency == "USD"
        assert t.venue == "Foreign"
        assert t.side == int(Side.CASH_BUY)

    def test_otc_trade_keeps_tw_venue(self, session):
        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "6531", OTC_BUY_TW, 1000, 50.0)),
        )
        t = session.exec(select(Trade)).one()
        assert t.side == int(Side.CASH_BUY)
        assert t.venue == "TW"

    def test_qty_rounded_to_int(self, session):
        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "0050", CASH_BUY_TW, 1000.0, 52.95)),
        )
        t = session.exec(select(Trade)).one()
        assert t.qty == 1000
        assert isinstance(t.qty, int)

    def test_fee_and_tax_persisted_as_decimal(self, session):
        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0)),
        )
        t = session.exec(select(Trade)).one()
        assert t.fee == Decimal("75.0")
        assert t.tax == Decimal("0.0")


class TestBackfillIdempotency:
    def test_rerun_yields_same_row_count(self, session):
        portfolio = _portfolio(
            _tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0),
            _tw_trade("2026-04-16", "2330", CASH_SELL_TW, 1000, 605.0),
        )
        trade_backfill.run(session, portfolio)
        first = session.exec(select(Trade)).all()
        trade_backfill.run(session, portfolio)
        second = session.exec(select(Trade)).all()
        assert len(first) == len(second) == 2

    def test_rerun_after_portfolio_shrink_drops_old_rows(self, session):
        big = _portfolio(
            _tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0),
            _tw_trade("2026-04-16", "2330", CASH_SELL_TW, 1000, 605.0),
        )
        small = _portfolio(
            _tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0),
        )
        trade_backfill.run(session, big)
        assert len(session.exec(select(Trade)).all()) == 2
        result = trade_backfill.run(session, small)
        assert len(session.exec(select(Trade)).all()) == 1
        assert result["pdf_rows_deleted"] == 2

    def test_overlay_rows_preserved(self, session):
        session.add(Trade(
            date=date(2026, 4, 20), code="2330", side=int(Side.CASH_BUY),
            qty=1000, price=Decimal("610.0"), currency="TWD",
            fee=Decimal("0"), tax=Decimal("0"), rebate=Decimal("0"),
            source="overlay", venue="TW",
        ))
        session.commit()

        trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0)),
        )
        rows = session.exec(select(Trade).order_by(Trade.date)).all()
        assert len(rows) == 2
        sources = sorted(r.source for r in rows)
        assert sources == ["overlay", "pdf"]


class TestBackfillSummary:
    def test_summary_keys(self, session):
        result = trade_backfill.run(
            session,
            _portfolio(_tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0)),
        )
        assert set(result.keys()) >= {
            "pdf_rows_inserted", "pdf_rows_deleted", "skipped_count",
        }

    def test_unknown_side_skipped_with_count(self, session):
        portfolio = _portfolio(
            _tw_trade("2026-04-15", "2330", CASH_BUY_TW, 1000, 600.0),
            _tw_trade("2026-04-16", "2330", "WTF_NEW", 1000, 605.0),
        )
        result = trade_backfill.run(session, portfolio)
        assert result["pdf_rows_inserted"] == 1
        assert result["skipped_count"] == 1
