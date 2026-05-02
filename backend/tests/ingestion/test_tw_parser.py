"""Reproducer for invest.ingestion.tw_parser.parse_tw_trade_line.

Pure single-line regex parser: a trade-detail row from the TW
證券交易明細 section → typed ParsedTwTrade or None.

Four trade families, each with its own column layout:

  普買/櫃買 (cash buy):
    date side name qty price gross fee 客戶應付
  普賣/櫃賣 (cash sell):
    date side name qty price gross fee tax 客戶應收
  資買 (margin buy):
    date side name qty price gross fee 起息日 融資金額 資自備款 客戶應付
  資賣 (margin sell):
    date side name qty price gross fee tax 起息日 融資金額 擔保價款 客戶應收

What's pinned:
  - All 4 families parse correctly with representative real-shape
    examples.
  - INVARIANT: 普 (TWSE) and 櫃 (OTC) side strings normalize into
    the same Side value at this layer. Exchange distinction is
    NOT trade-row metadata.
  - INVARIANT: net_twd carries a signed value — negative when client
    pays out, positive when client receives. Reconciliation logic
    elsewhere relies on this sign convention.
  - Margin trades carry interest_start (起息日, the day the financing
    starts accruing — distinct from the trade date when settlement
    crosses a weekend).
  - Numerics return Decimal, not float. Sufficient precision for
    cents and avoids accumulated FP error in fee/tax aggregation.
  - Non-trade lines (column headers, page numbers, blank rows)
    return None — they should be skipped, not crash the loop.

What's NOT pinned:
  - The exact regex source. The test exercises the BEHAVIOR (this
    line shape produces this output) so a regex rewrite can refactor
    freely without breaking the contract.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from invest.domain.trade import Side
from invest.ingestion.tw_parser import ParsedTwTrade, parse_tw_trade_line


# --- happy-path: cash trades ---------------------------------------------


class TestCashBuy:
    """普買 (TWSE cash buy) — 8 columns."""

    def test_parses_normalized_shape(self):
        # date side name  qty   price   gross    fee  客戶應付
        line = "2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.date == date(2024, 3, 15)
        assert out.name == "台積電"
        assert out.side == Side.CASH_BUY
        assert out.qty == 1000
        assert out.price == Decimal("850.00")
        assert out.gross == Decimal("850000")
        assert out.fee == Decimal("1213")
        assert out.tax == Decimal("0")
        # Cash buy: client pays out → net_twd negative.
        assert out.net_twd == Decimal("-851213")

    def test_otc_buy_normalizes_to_same_side(self):
        """INVARIANT: 普 (TWSE) and 櫃 (OTC) collapse to one Side
        at this layer. The TWSE/OTC distinction lives on holdings
        metadata, not on the trade row."""
        twse = "2024/03/15 普買 台積電 100 850.00 85,000 121 85,121"
        otc = "2024/03/15 櫃買 台積電 100 850.00 85,000 121 85,121"
        a = parse_tw_trade_line(twse)
        b = parse_tw_trade_line(otc)
        assert a is not None and b is not None
        assert a.side == b.side == Side.CASH_BUY


class TestCashSell:
    """普賣 (TWSE cash sell) — 9 columns (extra: tax)."""

    def test_parses_normalized_shape(self):
        # date side name  qty  price  gross   fee  tax  客戶應收
        line = "2024/04/01 普賣 台積電 500 870.00 435,000 620 1,305 433,075"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.date == date(2024, 4, 1)
        assert out.side == Side.CASH_SELL
        assert out.qty == 500
        assert out.price == Decimal("870.00")
        assert out.fee == Decimal("620")
        assert out.tax == Decimal("1305")
        # Cash sell: client receives → net_twd positive.
        assert out.net_twd == Decimal("433075")

    def test_otc_sell_normalizes_to_same_side(self):
        twse = "2024/04/01 普賣 台積電 100 870.00 87,000 124 261 86,615"
        otc = "2024/04/01 櫃賣 台積電 100 870.00 87,000 124 261 86,615"
        a = parse_tw_trade_line(twse)
        b = parse_tw_trade_line(otc)
        assert a is not None and b is not None
        assert a.side == b.side == Side.CASH_SELL


# --- happy-path: margin trades -------------------------------------------


class TestMarginBuy:
    """資買 (margin buy) — 11 columns; financing splits the cost."""

    def test_parses_full_margin_buy_shape(self):
        # date side name qty price gross fee 起息日   融資金額 資自備款 客戶應付
        line = "2024/05/10 資買 國巨 1,000 600.00 600,000 855 2024/05/14 240,000 360,000 360,855"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.date == date(2024, 5, 10)
        assert out.side == Side.MARGIN_BUY
        assert out.qty == 1000
        assert out.price == Decimal("600.00")
        assert out.fee == Decimal("855")
        # Margin breakdown: loan + self_funded ≈ gross.
        assert out.margin_loan == Decimal("240000")
        assert out.self_funded == Decimal("360000")
        # 起息日 — financing accrual start.
        assert out.interest_start == date(2024, 5, 14)
        # Client pays only self_funded + fee, not the full gross.
        assert out.net_twd == Decimal("-360855")

    def test_carries_zero_tax_and_collateral(self):
        """Margin BUY has no tax (taxed only on sells) and no
        collateral (collateral is a sell-side concept). Carry as 0
        so all four families share the same dataclass shape."""
        line = "2024/05/10 資買 國巨 100 600.00 60,000 86 2024/05/14 24,000 36,000 36,086"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.tax == Decimal("0")
        assert out.collateral == Decimal("0")


class TestMarginSell:
    """資賣 (margin sell) — 12 columns; collateral splits the proceeds."""

    def test_parses_full_margin_sell_shape(self):
        # date side name qty price gross fee tax 起息日   融資金額 擔保價款 客戶應收
        line = "2024/06/20 資賣 國巨 1,000 650.00 650,000 925 1,950 2024/06/24 240,000 410,000 407,125"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.side == Side.MARGIN_SELL
        assert out.qty == 1000
        assert out.tax == Decimal("1950")
        assert out.fee == Decimal("925")
        assert out.margin_loan == Decimal("240000")
        assert out.collateral == Decimal("410000")
        assert out.interest_start == date(2024, 6, 24)
        # Client receives proceeds net of loan repayment and fees.
        assert out.net_twd == Decimal("407125")

    def test_carries_zero_self_funded(self):
        """Margin SELL has no self_funded leg (the financing's already
        in place from the buy). Carry as 0 for shape symmetry."""
        line = "2024/06/20 資賣 國巨 100 650.00 65,000 92 195 2024/06/24 24,000 41,000 40,713"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.self_funded == Decimal("0")


# --- non-trade lines: return None ----------------------------------------


class TestNonTradeLines:
    def test_empty_string_returns_none(self):
        assert parse_tw_trade_line("") is None

    def test_whitespace_returns_none(self):
        assert parse_tw_trade_line("   \n  ") is None

    def test_column_header_returns_none(self):
        """The PDF prints a header row before the trade rows. It must
        not match any of the four trade regexes."""
        assert parse_tw_trade_line("交易日 類別 名稱 股數 單價 成交金額 手續費 客戶應付") is None

    def test_page_footer_returns_none(self):
        assert parse_tw_trade_line("第 3 頁，共 5 頁") is None

    def test_holdings_row_returns_none(self):
        """A row from the holdings section (different shape — no
        side keyword) must NOT accidentally match a trade regex."""
        assert parse_tw_trade_line("2330 台積電 1,000 850.00 850,000") is None

    def test_unknown_side_keyword_returns_none(self):
        """Side strings outside {普買, 櫃買, 普賣, 櫃賣, 資買, 資賣}
        return None — not a parse error, just 'this isn't one of the
        rows we know how to read'."""
        assert parse_tw_trade_line("2024/03/15 申購 台積電 1,000 850.00 850,000 1,213 851,213") is None


# --- edge cases ----------------------------------------------------------


class TestNumericPrecision:
    def test_quantity_with_thousands_separator(self):
        """1,000 must become int 1000, not crash on the comma."""
        line = "2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.qty == 1000

    def test_price_with_decimal(self):
        """Prices have 2-decimal precision in the PDF; preserve it."""
        line = "2024/03/15 普買 元大台灣50 100 142.55 14,255 21 14,276"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.price == Decimal("142.55")

    def test_odd_lot_quantity(self):
        """零股 (odd-lot, < 1000 shares) sometimes prints unpadded.
        Must parse identically to round-lot rows."""
        line = "2024/03/15 普買 台積電 100 850.00 85,000 121 85,121"
        out = parse_tw_trade_line(line)
        assert out is not None
        assert out.qty == 100


class TestDataclass:
    """ParsedTwTrade is a frozen dataclass so callers can use it as a
    dict key / in sets without worrying about mutation."""

    def test_is_frozen(self):
        line = "2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213"
        out = parse_tw_trade_line(line)
        assert out is not None
        try:
            out.qty = 999  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedTwTrade must be frozen")

    def test_equality_by_value(self):
        line = "2024/03/15 普買 台積電 1,000 850.00 850,000 1,213 851,213"
        a = parse_tw_trade_line(line)
        b = parse_tw_trade_line(line)
        assert a == b


# --- ParsedTwTrade direct construction (for orchestrator tests) ----------


class TestParsedTwTradeDirectConstruction:
    """Smoke-test that ParsedTwTrade is importable and constructible
    with the documented field set. Orchestrator tests (Cycle 32) will
    hand-build instances rather than parsing PDF lines."""

    def test_minimum_fields(self):
        t = ParsedTwTrade(
            date=date(2024, 1, 1),
            name="X",
            side=Side.CASH_BUY,
            qty=100,
            price=Decimal("10"),
            gross=Decimal("1000"),
            fee=Decimal("1"),
            net_twd=Decimal("-1001"),
        )
        # Defaults applied correctly.
        assert t.tax == Decimal("0")
        assert t.margin_loan == Decimal("0")
        assert t.self_funded == Decimal("0")
        assert t.collateral == Decimal("0")
        assert t.interest_start is None
