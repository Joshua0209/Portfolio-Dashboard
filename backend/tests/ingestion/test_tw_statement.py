"""Reproducer for invest.ingestion.tw_parser.parse_securities_text.

Pure text-blob parser: full TW 證券月對帳單 text → ParsedSecuritiesStatement.
The pdfplumber I/O wrapper (parse_securities) is a separate cycle; this
function takes already-extracted text so tests can use literal fixtures
instead of fixture PDFs.

The state machine:
  - Top-of-statement: extract 成交年月 → 'YYYY-MM' and rebates
    (電子折讓金 / 一般折讓金).
  - Enter holdings section on '證券庫存' marker.
  - Switch to trades section on '證券交易明細' marker.
  - Exit both on '客戶淨收付' or '電子折讓金額明細'.
  - In holdings section: try parse_tw_holding_row, then
    parse_tw_subtotal_row.
  - In trades section: try parse_tw_trade_line.
  - End-of-statement: extract 客戶淨收付：幣別：臺幣 X.

What's pinned:
  - All sections (period, rebates, holdings, subtotal, trades,
    cashflow) populate from a representative full statement.
  - INVARIANT: section routing — a trade-shaped line in the holdings
    section is NOT picked up as a trade (and vice versa). The state
    machine is the only source of truth for which parser tries.
  - Optional sections degrade gracefully (no rebates, no holdings,
    no trades, no cashflow) without crashing.
  - Period extraction is required — missing 成交年月 raises ValueError
    (it's the canonical month key downstream; silent fallback would
    let bad months merge into one bucket).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from invest.domain.trade import Side
from invest.ingestion.tw_parser import (
    ParsedSecuritiesStatement,
    ParsedTwRebate,
    parse_securities_text,
)


# Representative full statement — minimal but covers every section.
_FULL_STATEMENT = """\
永豐金證券 證券月對帳單
成交年月：202403
電子折讓金： 150
一般折讓金： 0

證券庫存
現股 2330 台積電 1,000 850.00 850,000 870.00 870,000 20,000 2.35% 12,000 32,000 3.76%
融資 6488 環球晶 100 600.00 60,000 650.00 65,000 5,000 8.33% 0 5,000 8.33%
小計 1,100 910,000 935,000 25,000 2.75%

證券交易明細
2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213
2024/03/20 普賣 台積電 500 870.00 435,000 620 1,305 433,075

電子折讓金額明細
（明細表）

客戶淨收付：幣別：臺幣 -418,138
"""


class TestFullStatement:
    def test_parses_period(self):
        out = parse_securities_text(_FULL_STATEMENT)
        assert out.month == "2024-03"

    def test_parses_holdings_in_order(self):
        out = parse_securities_text(_FULL_STATEMENT)
        assert len(out.holdings) == 2
        assert out.holdings[0].code == "2330"
        assert out.holdings[0].type == "現股"
        assert out.holdings[1].code == "6488"
        assert out.holdings[1].type == "融資"

    def test_parses_subtotal(self):
        out = parse_securities_text(_FULL_STATEMENT)
        assert out.subtotal is not None
        assert out.subtotal.qty == 1100
        assert out.subtotal.cost == Decimal("910000")

    def test_parses_trades_in_order(self):
        out = parse_securities_text(_FULL_STATEMENT)
        assert len(out.trades) == 2
        assert out.trades[0].side == Side.CASH_BUY
        assert out.trades[0].date == date(2024, 3, 15)
        assert out.trades[1].side == Side.CASH_SELL
        assert out.trades[1].date == date(2024, 3, 20)

    def test_parses_rebates(self):
        out = parse_securities_text(_FULL_STATEMENT)
        # Only nonzero rebates appear; 一般折讓金 is 0 so excluded.
        assert len(out.rebates) == 1
        assert out.rebates[0].type == "電子折讓金"
        assert out.rebates[0].amount_twd == Decimal("150")

    def test_parses_net_cashflow(self):
        out = parse_securities_text(_FULL_STATEMENT)
        assert out.net_cashflow_twd == Decimal("-418138")

    def test_returns_immutable_tuples(self):
        """INVARIANT: holdings/trades/rebates are tuples, not lists.
        ParsedSecuritiesStatement is a frozen aggregate; mutable
        collections inside would let callers silently mutate state."""
        out = parse_securities_text(_FULL_STATEMENT)
        assert isinstance(out.holdings, tuple)
        assert isinstance(out.trades, tuple)
        assert isinstance(out.rebates, tuple)


# --- section routing invariants ------------------------------------------


class TestSectionRouting:
    def test_trade_shaped_line_in_holdings_section_ignored(self):
        """INVARIANT: a line that LOOKS like a trade row but appears
        in the holdings section must not be picked up as a trade.
        Section state is the only source of truth."""
        text = """\
成交年月：202403
證券庫存
2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213
客戶淨收付：幣別：臺幣 0
"""
        out = parse_securities_text(text)
        assert len(out.trades) == 0

    def test_holding_shaped_line_in_trades_section_ignored(self):
        """INVARIANT: the inverse — a holding-shaped line appearing
        in the trades section is NOT picked up. The state machine
        owns dispatch."""
        text = """\
成交年月：202403
證券交易明細
現股 2330 台積電 1,000 850.00 850,000 870.00 870,000 20,000 2.35% 12,000 32,000 3.76%
客戶淨收付：幣別：臺幣 0
"""
        out = parse_securities_text(text)
        assert len(out.holdings) == 0

    def test_section_marker_inside_text_does_not_break(self):
        """A line containing both 證券庫存 and 證券交易明細 (e.g. a
        page header listing both) should NOT enter holdings.
        Legacy guard: 'and 證券交易明細 not in line'."""
        text = """\
成交年月：202403
本月 證券庫存 與 證券交易明細 摘要
現股 2330 台積電 100 850.00 85,000 870.00 87,000 2,000 2.35% 0 2,000 2.35%
客戶淨收付：幣別：臺幣 0
"""
        out = parse_securities_text(text)
        # The holdings line came AFTER the dual-marker line; section
        # state is still 'none', so the line is dropped.
        assert len(out.holdings) == 0

    def test_end_marker_exits_holdings(self):
        """客戶淨收付 marker must end the holdings section. Trailing
        holding-shaped lines after that marker are post-statement
        commentary, not data."""
        text = """\
成交年月：202403
證券庫存
現股 2330 台積電 100 850.00 85,000 870.00 87,000 2,000 2.35% 0 2,000 2.35%
客戶淨收付：幣別：臺幣 0
現股 9999 假股票 100 100.00 10,000 100.00 10,000 0 0.00% 0 0 0.00%
"""
        out = parse_securities_text(text)
        # Only the pre-marker holding counts.
        assert len(out.holdings) == 1
        assert out.holdings[0].code == "2330"


# --- empty / minimal statements ------------------------------------------


class TestMinimalStatements:
    def test_minimum_with_only_period(self):
        """A statement with only a period and nothing else: empty
        sections, no cashflow. Must not crash."""
        text = "成交年月：202403\n"
        out = parse_securities_text(text)
        assert out.month == "2024-03"
        assert out.holdings == ()
        assert out.trades == ()
        assert out.rebates == ()
        assert out.subtotal is None
        assert out.net_cashflow_twd == Decimal("0")

    def test_no_subtotal_when_holdings_empty(self):
        """No 小計 row → subtotal is None, not a zero-filled struct.
        Lets consumers distinguish 'no holdings' from 'zero qty'."""
        text = """\
成交年月：202403
證券庫存

客戶淨收付：幣別：臺幣 0
"""
        out = parse_securities_text(text)
        assert out.subtotal is None
        assert out.holdings == ()

    def test_zero_rebates_excluded(self):
        """Both rebate fields print as 0 → no ParsedTwRebate entries.
        Empty tuple is the canonical 'nothing here' signal."""
        text = """\
成交年月：202403
電子折讓金： 0
一般折讓金： 0
"""
        out = parse_securities_text(text)
        assert out.rebates == ()

    def test_both_rebates_present(self):
        text = """\
成交年月：202403
電子折讓金： 100
一般折讓金： 50
"""
        out = parse_securities_text(text)
        assert len(out.rebates) == 2
        types = {r.type for r in out.rebates}
        assert types == {"電子折讓金", "一般折讓金"}


# --- error paths ---------------------------------------------------------


class TestErrorPaths:
    def test_missing_period_raises(self):
        """INVARIANT: 成交年月 is the canonical month key downstream
        (every record carries it). Missing → loud failure, not silent
        bucket-merge into a default month."""
        with pytest.raises(ValueError):
            parse_securities_text("just some text\nwith no period\n")


# --- dataclass shape -----------------------------------------------------


class TestDataclassShapes:
    def test_statement_is_frozen(self):
        out = parse_securities_text("成交年月：202403\n")
        try:
            out.month = "2024-04"  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedSecuritiesStatement must be frozen")

    def test_rebate_is_frozen(self):
        r = ParsedTwRebate(type="電子折讓金", amount_twd=Decimal("100"))
        try:
            r.amount_twd = Decimal("0")  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedTwRebate must be frozen")
