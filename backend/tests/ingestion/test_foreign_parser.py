"""Reproducer for invest.ingestion.foreign_parser.parse_foreign_text.

Pure text-blob parser for the 複委託 (foreign brokerage) monthly
statement. Three section types:

  海外股票庫存及投資損益    holdings table (12-column rows; USD/HKD/JPY)
  海外股票交易明細           trades table (12-column rows; 買進/賣出)
  海外股票現金股利明細       dividend table (tail-walking parse)

Plus per-currency cashflow aggregation across trades + dividends.

Foreign trades are always cash (no margin, no shorting in the
dataset), so 買進/賣出 map to Side.CASH_BUY / Side.CASH_SELL.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from invest.domain.trade import Side
from invest.ingestion.foreign_parser import (
    ParsedForeignDividend,
    ParsedForeignHolding,
    ParsedForeignStatement,
    ParsedForeignTrade,
    parse_foreign_text,
)


_FULL_FOREIGN = """\
永豐金證券 複委託對帳單
對帳單日期：2024/03/31

海外股票庫存及投資損益
NVDA NVIDIA Corp NASDAQ NMS USD 100 USD 50,000.00 2024/03/29 880.00 88,000.00 38,000.00 76.00%
TSLA Tesla Inc NASDAQ NMS USD 50 USD 10,000.00 2024/03/29 175.50 8,775.00 -1,225.00 -12.25%

債券商品庫存及投資損益
無庫存明細

海外股票交易明細
2024/03/15 NVDA NASDAQ NMS 買進 USD 100 500.00 50,000.00 5.00 50.00 -50,055.00
2024/03/20 TSLA NASDAQ NMS 賣出 USD 25 200.00 5,000.00 5.00 25.00 4,970.00

債券商品交易明細
無交易明細

海外股票現金股利明細
NASDAQ 2024/03/10 NVDA 2024/02/28 100 USD 5.00 0 0 0 0 4.00 31.50

【截至2024/03/31】
"""


# --- happy path ----------------------------------------------------------


class TestFullStatement:
    def test_parses_period(self):
        out = parse_foreign_text(_FULL_FOREIGN)
        assert out.month == "2024-03"

    def test_parses_holdings_with_currency(self):
        out = parse_foreign_text(_FULL_FOREIGN)
        assert len(out.holdings) == 2
        nvda = out.holdings[0]
        assert nvda.code == "NVDA"
        assert nvda.name == "NVIDIA Corp"
        assert nvda.market == "NASDAQ"
        assert nvda.exchange == "NMS"
        assert nvda.ccy == "USD"
        assert nvda.qty == 100
        assert nvda.cost == Decimal("50000.00")
        assert nvda.ref_date == date(2024, 3, 29)
        assert nvda.close == Decimal("880.00")
        assert nvda.mkt_value == Decimal("88000.00")
        assert nvda.unrealized_pnl == Decimal("38000.00")

    def test_parses_trades_with_side_mapping(self):
        out = parse_foreign_text(_FULL_FOREIGN)
        assert len(out.trades) == 2

        buy, sell = out.trades
        assert buy.date == date(2024, 3, 15)
        assert buy.side == Side.CASH_BUY  # 買進 → CASH_BUY
        assert buy.code == "NVDA"
        assert buy.ccy == "USD"
        assert buy.qty == 100
        assert buy.price == Decimal("500.00")
        assert buy.gross == Decimal("50000.00")
        assert buy.fee == Decimal("5.00")
        # net_ccy is SIGNED: negative for buys (cash out).
        assert buy.net_ccy == Decimal("-50055.00")

        assert sell.side == Side.CASH_SELL  # 賣出 → CASH_SELL
        assert sell.net_ccy == Decimal("4970.00")  # positive for sells

    def test_parses_dividends_tail_walking(self):
        """Dividend rows have variable middle-column counts. The
        parser walks from the right: last token is FX rate, the
        token before it is the dividend net (股利淨額)."""
        out = parse_foreign_text(_FULL_FOREIGN)
        assert len(out.dividends) == 1
        d = out.dividends[0]
        assert d.date == date(2024, 3, 10)
        assert d.code == "NVDA"
        assert d.qty == 100
        assert d.ccy == "USD"
        assert d.net_amount == Decimal("4.00")

    def test_aggregates_cashflow_by_currency(self):
        """INVARIANT: cashflow_by_ccy sums BOTH trades AND dividends
        per currency. The legacy code treats them uniformly because
        downstream the consumer cares about per-ccy net flow into
        the broker account, not the sub-component."""
        out = parse_foreign_text(_FULL_FOREIGN)
        # USD: -50055 (buy) + 4970 (sell) + 4.00 (dividend) = -45081
        assert out.cashflow_by_ccy["USD"] == Decimal("-45081")

    def test_returns_immutable_tuples(self):
        out = parse_foreign_text(_FULL_FOREIGN)
        assert isinstance(out.holdings, tuple)
        assert isinstance(out.trades, tuple)
        assert isinstance(out.dividends, tuple)


# --- empty / sparse statements -------------------------------------------


class TestEmptySections:
    def test_no_holdings_when_marker_says_empty(self):
        text = """\
對帳單日期：2024/03/31
海外股票庫存及投資損益
無庫存明細
債券商品庫存及投資損益
"""
        out = parse_foreign_text(text)
        assert out.holdings == ()

    def test_no_trades_when_marker_says_empty(self):
        text = """\
對帳單日期：2024/03/31
海外股票交易明細
無交易明細
債券商品交易明細
"""
        out = parse_foreign_text(text)
        assert out.trades == ()
        assert out.cashflow_by_ccy == {}

    def test_period_only(self):
        out = parse_foreign_text("對帳單日期：2024/03/31\n")
        assert out.month == "2024-03"
        assert out.holdings == ()
        assert out.trades == ()
        assert out.dividends == ()
        assert out.cashflow_by_ccy == {}


# --- section boundary invariants -----------------------------------------


class TestSectionBoundaries:
    def test_holdings_section_bounded_by_bond_marker(self):
        """INVARIANT: holdings parsing must STOP at '債券商品庫存及
        投資損益' (the bond holdings section). Without this guard, a
        bond holding row could accidentally match the foreign-stock
        regex."""
        text = """\
對帳單日期：2024/03/31
海外股票庫存及投資損益
NVDA NVIDIA Corp NASDAQ NMS USD 100 USD 50,000.00 2024/03/29 880.00 88,000.00 38,000.00 76.00%
債券商品庫存及投資損益
USTREAS US Treasury 10Y NASDAQ NMS USD 100 USD 99,000.00 2024/03/29 99.50 99,500.00 500.00 0.50%
"""
        out = parse_foreign_text(text)
        # Only the row in the stock section counts.
        assert len(out.holdings) == 1
        assert out.holdings[0].code == "NVDA"

    def test_trades_section_bounded_by_bond_marker(self):
        """Same INVARIANT for trades: 海外股票交易明細 → 債券商品交易
        明細 brackets. A bond trade row with the right shape must NOT
        leak into stock trades."""
        text = """\
對帳單日期：2024/03/31
海外股票交易明細
2024/03/15 NVDA NASDAQ NMS 買進 USD 100 500.00 50,000.00 5.00 50.00 -50,055.00
債券商品交易明細
2024/03/16 USTREAS NASDAQ NMS 買進 USD 100 99.50 9,950.00 5.00 0.00 -9,955.00
"""
        out = parse_foreign_text(text)
        assert len(out.trades) == 1
        assert out.trades[0].code == "NVDA"


# --- error paths ---------------------------------------------------------


class TestErrorPaths:
    def test_missing_period_raises(self):
        with pytest.raises(ValueError):
            parse_foreign_text("無對帳單日期\n")


# --- dataclass shape -----------------------------------------------------


class TestDataclassShapes:
    def test_holding_frozen(self):
        h = ParsedForeignHolding(
            code="X",
            name="X",
            market="X",
            exchange="X",
            ccy="USD",
            qty=1,
            cost=Decimal("1"),
            ref_date=date(2024, 1, 1),
            close=Decimal("1"),
            mkt_value=Decimal("1"),
            unrealized_pnl=Decimal("0"),
        )
        try:
            h.qty = 2  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedForeignHolding must be frozen")

    def test_trade_frozen(self):
        t = ParsedForeignTrade(
            date=date(2024, 1, 1),
            code="X",
            market="X",
            exchange="X",
            side=Side.CASH_BUY,
            ccy="USD",
            qty=1,
            price=Decimal("1"),
            gross=Decimal("1"),
            fee=Decimal("0"),
            other_fee=Decimal("0"),
            net_ccy=Decimal("-1"),
        )
        try:
            t.qty = 2  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedForeignTrade must be frozen")

    def test_dividend_frozen(self):
        d = ParsedForeignDividend(
            date=date(2024, 1, 1),
            code="X",
            qty=1,
            ccy="USD",
            net_amount=Decimal("0"),
        )
        try:
            d.code = "Y"  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedForeignDividend must be frozen")

    def test_statement_frozen(self):
        out = parse_foreign_text("對帳單日期：2024/03/31\n")
        try:
            out.month = "X"  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedForeignStatement must be frozen")
