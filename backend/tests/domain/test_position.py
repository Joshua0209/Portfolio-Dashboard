from datetime import date
from decimal import Decimal

import pytest

from invest.domain.money import Money
from invest.domain.position import Lot, Position, RealizedPair
from invest.domain.trade import Side, Trade, Venue


def _trade(side: Side, qty: int, price: str, day: int = 1) -> Trade:
    return Trade(
        date=date(2026, 5, day),
        code="2330",
        side=side,
        qty=qty,
        price=Money(Decimal(price), "TWD"),
        venue=Venue.TW,
    )


@pytest.fixture
def pos() -> Position:
    return Position(code="2330", currency="TWD")


class TestEmpty:
    def test_empty_position_has_zero_qty(self, pos):
        assert pos.total_qty == 0

    def test_empty_position_cost_basis_is_zero_in_currency(self, pos):
        assert pos.cost_basis == Money(Decimal("0"), "TWD")

    def test_empty_position_realized_pnl_is_zero(self, pos):
        assert pos.realized_pnl == Money(Decimal("0"), "TWD")

    def test_empty_position_has_no_lots(self, pos):
        assert pos.open_lots == []

    def test_empty_position_has_no_realized_pairs(self, pos):
        assert pos.realized_pairs == []


class TestBuy:
    def test_single_buy_creates_one_lot(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 1000, "920"))
        assert pos.total_qty == 1000
        assert len(pos.open_lots) == 1
        assert pos.open_lots[0] == Lot(
            date=date(2026, 5, 1),
            qty=1000,
            cost_per_share=Money(Decimal("920"), "TWD"),
        )

    def test_two_buys_create_two_lots_in_order(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 1000, "920", day=1))
        pos.apply(_trade(Side.CASH_BUY, 500, "950", day=10))
        assert pos.total_qty == 1500
        assert len(pos.open_lots) == 2
        assert pos.open_lots[0].date == date(2026, 5, 1)
        assert pos.open_lots[1].date == date(2026, 5, 10)

    def test_cost_basis_sums_lot_costs(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 1000, "920", day=1))
        pos.apply(_trade(Side.CASH_BUY, 500, "950", day=10))
        # 1000 * 920 + 500 * 950 = 920000 + 475000 = 1_395_000
        assert pos.cost_basis == Money(Decimal("1395000"), "TWD")

    def test_buy_in_wrong_currency_raises(self, pos):
        bad = Trade(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("100"), "USD"),
            venue=Venue.TW,
        )
        with pytest.raises(ValueError, match="currency mismatch"):
            pos.apply(bad)


class TestSell:
    def test_full_sell_empties_position_and_records_realized_pair(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 1000, "920", day=1))
        pos.apply(_trade(Side.CASH_SELL, 1000, "950", day=10))

        assert pos.total_qty == 0
        assert pos.open_lots == []
        assert len(pos.realized_pairs) == 1

        pair = pos.realized_pairs[0]
        assert pair.close_qty == 1000
        assert pair.close_date == date(2026, 5, 10)
        assert pair.close_price == Money(Decimal("950"), "TWD")
        # P&L = (950 - 920) * 1000 = 30000
        assert pair.realized_pnl == Money(Decimal("30000"), "TWD")

    def test_partial_sell_preserves_remainder_of_oldest_lot(self, pos):
        # CRITICAL: naive `open_lots.pop(0)` after a partial close would
        # destroy the remainder. This test pins the partial-close path.
        pos.apply(_trade(Side.CASH_BUY, 1000, "920", day=1))
        pos.apply(_trade(Side.CASH_SELL, 300, "950", day=10))

        assert pos.total_qty == 700
        assert len(pos.open_lots) == 1
        assert pos.open_lots[0].qty == 700
        assert pos.open_lots[0].cost_per_share == Money(Decimal("920"), "TWD")
        assert pos.open_lots[0].date == date(2026, 5, 1)  # not changed

        assert len(pos.realized_pairs) == 1
        assert pos.realized_pairs[0].close_qty == 300
        # P&L = (950 - 920) * 300 = 9000
        assert pos.realized_pairs[0].realized_pnl == Money(
            Decimal("9000"), "TWD"
        )

    def test_sell_consumes_oldest_lot_first_fifo(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 1000, "900", day=1))   # cheap
        pos.apply(_trade(Side.CASH_BUY, 1000, "1000", day=5))  # expensive
        pos.apply(_trade(Side.CASH_SELL, 1000, "950", day=10))

        assert pos.total_qty == 1000
        # The expensive lot should remain.
        assert pos.open_lots[0].cost_per_share == Money(
            Decimal("1000"), "TWD"
        )
        # P&L = (950 - 900) * 1000 = 50000 (gain on the cheap lot)
        assert pos.realized_pnl == Money(Decimal("50000"), "TWD")

    def test_sell_spans_multiple_lots_creates_multiple_realized_pairs(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 500, "900", day=1))
        pos.apply(_trade(Side.CASH_BUY, 500, "1000", day=5))
        pos.apply(_trade(Side.CASH_SELL, 800, "950", day=10))

        assert pos.total_qty == 200  # 1000 - 800 left
        # Two realized pairs: 500 from first lot, 300 from second.
        assert len(pos.realized_pairs) == 2
        assert pos.realized_pairs[0].close_qty == 500
        assert pos.realized_pairs[1].close_qty == 300

        # P&L: (950-900)*500 + (950-1000)*300 = 25000 - 15000 = 10000
        assert pos.realized_pnl == Money(Decimal("10000"), "TWD")

        # The remaining 200 of the second lot should still be open.
        assert pos.open_lots[0].qty == 200
        assert pos.open_lots[0].cost_per_share == Money(
            Decimal("1000"), "TWD"
        )

    def test_sell_exceeding_open_qty_raises(self, pos):
        pos.apply(_trade(Side.CASH_BUY, 500, "900"))
        with pytest.raises(ValueError, match="exceeds open"):
            pos.apply(_trade(Side.CASH_SELL, 1000, "950"))


class TestUnsupportedSides:
    def test_short_sell_not_implemented(self, pos):
        with pytest.raises(NotImplementedError, match="short"):
            pos.apply(_trade(Side.SHORT_SELL, 1000, "920"))

    def test_short_cover_not_implemented(self, pos):
        with pytest.raises(NotImplementedError, match="short"):
            pos.apply(_trade(Side.SHORT_COVER, 1000, "920"))

    def test_margin_buy_not_implemented_in_v1(self, pos):
        # Margin lots track fine for FIFO but cost-basis interpretation
        # depends on margin rules (you paid only your portion). v1
        # raises explicitly to avoid silent miscalculation.
        with pytest.raises(NotImplementedError, match="margin"):
            pos.apply(_trade(Side.MARGIN_BUY, 1000, "920"))


class TestRealizedPair:
    def test_realized_pnl_calculation(self):
        pair = RealizedPair(
            open_lot=Lot(
                date=date(2026, 5, 1),
                qty=1000,  # original lot qty (may differ from close_qty)
                cost_per_share=Money(Decimal("900"), "TWD"),
            ),
            close_date=date(2026, 5, 10),
            close_qty=300,
            close_price=Money(Decimal("950"), "TWD"),
        )
        # (950 - 900) * 300 = 15000
        assert pair.realized_pnl == Money(Decimal("15000"), "TWD")

    def test_realized_pnl_loss(self):
        pair = RealizedPair(
            open_lot=Lot(
                date=date(2026, 5, 1),
                qty=1000,
                cost_per_share=Money(Decimal("1000"), "TWD"),
            ),
            close_date=date(2026, 5, 10),
            close_qty=500,
            close_price=Money(Decimal("950"), "TWD"),
        )
        # (950 - 1000) * 500 = -25000
        assert pair.realized_pnl == Money(Decimal("-25000"), "TWD")
