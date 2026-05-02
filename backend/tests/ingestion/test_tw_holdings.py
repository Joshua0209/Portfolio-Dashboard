"""Reproducer for invest.ingestion.tw_parser holdings-row parsers.

Two pure single-line parsers for the TW 證券庫存 (holdings) section
of a monthly 證券月對帳單:

  parse_tw_holding_row(line)   -> ParsedTwHolding | None
      One row per held position. 13 columns: type code name qty
      avg_cost cost ref_price mkt_value unrealized_pnl unrealized_pct
      cum_dividend unrealized_pnl_with_div unrealized_pct_with_div.

  parse_tw_subtotal_row(line)  -> ParsedTwSubtotal | None
      The 小計 row at the bottom of the holdings table. 5 columns:
      qty cost mkt_value unrealized_pnl unrealized_pct. (No dividend
      columns — those are per-row only.)

These are the row-level building blocks; the section state machine
(in Cycle 30) decides WHICH lines are tried against which parser
based on which 證券庫存 / 證券交易明細 boundary marker was last seen.
"""
from __future__ import annotations

from decimal import Decimal

from invest.ingestion.tw_parser import (
    ParsedTwHolding,
    ParsedTwSubtotal,
    parse_tw_holding_row,
    parse_tw_subtotal_row,
)


# --- parse_tw_holding_row ------------------------------------------------


class TestHoldingRowCash:
    """現股 (cash holding) — the most common type."""

    def test_parses_normalized_cash_row(self):
        # type code name      qty   avg_cost cost     ref_price mkt_value u_pnl     u_pct cum_div u_pnl_div u_pct_div
        line = "現股 2330 台積電 1,000 850.00 850,000 870.00 870,000 20,000 2.35% 12,000 32,000 3.76%"
        out = parse_tw_holding_row(line)
        assert out is not None
        assert out.type == "現股"
        assert out.code == "2330"
        assert out.name == "台積電"
        assert out.qty == 1000
        assert out.avg_cost == Decimal("850.00")
        assert out.cost == Decimal("850000")
        assert out.ref_price == Decimal("870.00")
        assert out.mkt_value == Decimal("870000")
        assert out.unrealized_pnl == Decimal("20000")
        # Percentages stored as fractions (2.35% → 0.0235).
        assert out.unrealized_pct == Decimal("0.0235")
        assert out.cum_dividend == Decimal("12000")
        assert out.unrealized_pnl_with_div == Decimal("32000")
        assert out.unrealized_pct_with_div == Decimal("0.0376")

    def test_zero_cumulative_dividend(self):
        """A new holding may have no dividends declared yet. The
        cum_dividend column prints 0 — must parse, not crash."""
        line = "現股 2330 台積電 100 850.00 85,000 870.00 87,000 2,000 2.35% 0 2,000 2.35%"
        out = parse_tw_holding_row(line)
        assert out is not None
        assert out.cum_dividend == Decimal("0")
        assert out.unrealized_pnl == out.unrealized_pnl_with_div

    def test_negative_unrealized_pnl(self):
        """Down-position prints negative pnl + negative pct."""
        line = "現股 2330 台積電 100 900.00 90,000 870.00 87,000 -3,000 -3.33% 0 -3,000 -3.33%"
        out = parse_tw_holding_row(line)
        assert out is not None
        assert out.unrealized_pnl == Decimal("-3000")
        assert out.unrealized_pct == Decimal("-0.0333")


class TestHoldingRowMargin:
    """融資 (margin) and 融券 (short) holdings share the cash row shape;
    only the leading type keyword differs."""

    def test_parses_margin_long_row(self):
        line = "融資 6488 環球晶 1,000 600.00 600,000 650.00 650,000 50,000 8.33% 5,000 55,000 9.17%"
        out = parse_tw_holding_row(line)
        assert out is not None
        assert out.type == "融資"
        assert out.qty == 1000

    def test_parses_short_position_row(self):
        line = "融券 2330 台積電 100 870.00 87,000 850.00 85,000 2,000 2.30% 0 2,000 2.30%"
        out = parse_tw_holding_row(line)
        assert out is not None
        assert out.type == "融券"


class TestHoldingRowNonMatches:
    def test_empty_returns_none(self):
        assert parse_tw_holding_row("") is None

    def test_subtotal_row_returns_none(self):
        """The 小計 row sits in the holdings section but has a
        different shape — must not match the holding regex."""
        assert parse_tw_holding_row("小計 1,000 850,000 870,000 20,000 2.35%") is None

    def test_section_marker_returns_none(self):
        assert parse_tw_holding_row("證券庫存") is None

    def test_trade_row_returns_none(self):
        """A row from the trades section must not accidentally match.
        The leading token there is a date, not 現股/融資/融券."""
        line = "2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213"
        assert parse_tw_holding_row(line) is None

    def test_unknown_type_keyword_returns_none(self):
        """Defensive: if Sinopac introduces a new holding type, the
        row returns None until we extend the regex."""
        line = "借券 2330 台積電 100 850.00 85,000 870.00 87,000 2,000 2.35% 0 2,000 2.35%"
        assert parse_tw_holding_row(line) is None


# --- parse_tw_subtotal_row -----------------------------------------------


class TestSubtotalRow:
    def test_parses_normalized_subtotal(self):
        # 小計 qty       cost    mkt_value u_pnl u_pct
        line = "小計 1,500 1,250,000 1,310,000 60,000 4.80%"
        out = parse_tw_subtotal_row(line)
        assert out is not None
        assert out.qty == 1500
        assert out.cost == Decimal("1250000")
        assert out.mkt_value == Decimal("1310000")
        assert out.unrealized_pnl == Decimal("60000")

    def test_drops_percentage_column(self):
        """The percentage column is derived from the other four; we
        store the raw values and recompute on demand. Don't carry
        a redundant field that could go out of sync."""
        line = "小計 100 85,000 87,000 2,000 2.35%"
        out = parse_tw_subtotal_row(line)
        assert out is not None
        # No 'unrealized_pct' field on ParsedTwSubtotal.
        assert not hasattr(out, "unrealized_pct")

    def test_negative_subtotal(self):
        line = "小計 100 90,000 87,000 -3,000 -3.33%"
        out = parse_tw_subtotal_row(line)
        assert out is not None
        assert out.unrealized_pnl == Decimal("-3000")

    def test_empty_returns_none(self):
        assert parse_tw_subtotal_row("") is None

    def test_holding_row_returns_none(self):
        line = "現股 2330 台積電 100 850.00 85,000 870.00 87,000 2,000 2.35% 0 2,000 2.35%"
        assert parse_tw_subtotal_row(line) is None

    def test_unrelated_text_returns_none(self):
        assert parse_tw_subtotal_row("成交年月：202403") is None


# --- dataclass shape ----------------------------------------------------


class TestParsedTwHoldingShape:
    def test_is_frozen(self):
        h = ParsedTwHolding(
            type="現股",
            code="2330",
            name="台積電",
            qty=100,
            avg_cost=Decimal("850"),
            cost=Decimal("85000"),
            ref_price=Decimal("870"),
            mkt_value=Decimal("87000"),
            unrealized_pnl=Decimal("2000"),
            unrealized_pct=Decimal("0.0235"),
            cum_dividend=Decimal("0"),
            unrealized_pnl_with_div=Decimal("2000"),
            unrealized_pct_with_div=Decimal("0.0235"),
        )
        try:
            h.qty = 999  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedTwHolding must be frozen")


class TestParsedTwSubtotalShape:
    def test_is_frozen(self):
        s = ParsedTwSubtotal(
            qty=100,
            cost=Decimal("85000"),
            mkt_value=Decimal("87000"),
            unrealized_pnl=Decimal("2000"),
        )
        try:
            s.qty = 999  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedTwSubtotal must be frozen")
