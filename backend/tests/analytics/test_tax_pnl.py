from datetime import date
from decimal import Decimal

import pytest

from invest.analytics.tax_pnl import (
    build_positions,
    realized_pnl_per_position,
    unrealized_pnl_per_position,
)
from invest.domain.money import Money
from invest.domain.position import Position
from invest.domain.trade import Side, Trade, Venue


def _trade(code: str, side: Side, qty: int, price: str, day: int = 1, currency: str = "TWD") -> Trade:
    return Trade(
        date=date(2026, 5, day),
        code=code,
        side=side,
        qty=qty,
        price=Money(Decimal(price), currency),
        venue=Venue.TW,
    )


class TestBuildPositions:
    def test_empty_trades_empty_positions(self):
        assert build_positions([]) == {}

    def test_single_buy_creates_one_position(self):
        positions = build_positions([_trade("2330", Side.CASH_BUY, 1000, "920")])
        assert "2330" in positions
        assert positions["2330"].total_qty == 1000

    def test_multi_ticker_separate_positions(self):
        positions = build_positions([
            _trade("2330", Side.CASH_BUY, 1000, "920"),
            _trade("2454", Side.CASH_BUY, 500, "1180"),
        ])
        assert positions["2330"].total_qty == 1000
        assert positions["2454"].total_qty == 500

    def test_skips_unsupported_sides_silently(self):
        # Margin and short are out of scope for v1 — tax_pnl skips them
        # rather than raising, so users with margin trades still get
        # cash-trade P&L.
        positions = build_positions([
            _trade("2330", Side.CASH_BUY, 1000, "920"),
            _trade("2454", Side.MARGIN_BUY, 500, "1180"),
            _trade("3008", Side.SHORT_SELL, 100, "3000"),
        ])
        assert "2330" in positions
        assert "2454" not in positions
        assert "3008" not in positions


class TestRealizedPnl:
    def test_no_sells_no_realized(self):
        trades = [_trade("2330", Side.CASH_BUY, 1000, "920")]
        result = realized_pnl_per_position(trades)
        assert result == {"2330": Money(Decimal("0"), "TWD")}

    def test_full_round_trip(self):
        # Buy 1000 @ 920, sell 1000 @ 950 → P&L = 30 * 1000 = 30000
        trades = [
            _trade("2330", Side.CASH_BUY, 1000, "920", day=1),
            _trade("2330", Side.CASH_SELL, 1000, "950", day=10),
        ]
        result = realized_pnl_per_position(trades)
        assert result["2330"] == Money(Decimal("30000"), "TWD")

    def test_partial_sell(self):
        # Buy 1000 @ 920, sell 300 @ 950 → P&L = 30 * 300 = 9000
        trades = [
            _trade("2330", Side.CASH_BUY, 1000, "920", day=1),
            _trade("2330", Side.CASH_SELL, 300, "950", day=10),
        ]
        result = realized_pnl_per_position(trades)
        assert result["2330"] == Money(Decimal("9000"), "TWD")

    def test_multi_ticker_separate_pnl(self):
        trades = [
            _trade("2330", Side.CASH_BUY, 1000, "920", day=1),
            _trade("2330", Side.CASH_SELL, 1000, "950", day=10),
            _trade("2454", Side.CASH_BUY, 500, "1180", day=2),
            _trade("2454", Side.CASH_SELL, 500, "1100", day=12),  # loss
        ]
        result = realized_pnl_per_position(trades)
        assert result["2330"] == Money(Decimal("30000"), "TWD")
        # (1100 - 1180) * 500 = -40000
        assert result["2454"] == Money(Decimal("-40000"), "TWD")


class TestUnrealizedPnl:
    def test_no_positions_empty(self):
        result = unrealized_pnl_per_position({}, {})
        assert result == {}

    def test_open_position_with_current_price(self):
        # Bought 1000 @ 920; current price 950 → unrealized = 30 * 1000 = 30000
        positions = build_positions([_trade("2330", Side.CASH_BUY, 1000, "920")])
        prices = {"2330": Money(Decimal("950"), "TWD")}
        result = unrealized_pnl_per_position(positions, prices)
        assert result["2330"] == Money(Decimal("30000"), "TWD")

    def test_position_without_price_skipped(self):
        positions = build_positions([_trade("2330", Side.CASH_BUY, 1000, "920")])
        result = unrealized_pnl_per_position(positions, {})
        assert "2330" not in result

    def test_currency_mismatch_raises(self):
        positions = build_positions([_trade("2330", Side.CASH_BUY, 1000, "920", currency="TWD")])
        prices = {"2330": Money(Decimal("30"), "USD")}
        with pytest.raises(ValueError, match="currency mismatch"):
            unrealized_pnl_per_position(positions, prices)

    def test_multi_lot_sums_correctly(self):
        # Two buys at different prices, current price between them.
        # Lot 1: 1000 @ 900, current 950 → +50000
        # Lot 2: 500 @ 1000, current 950 → -25000
        # Total unrealized: +25000
        positions = build_positions([
            _trade("2330", Side.CASH_BUY, 1000, "900", day=1),
            _trade("2330", Side.CASH_BUY, 500, "1000", day=5),
        ])
        prices = {"2330": Money(Decimal("950"), "TWD")}
        result = unrealized_pnl_per_position(positions, prices)
        assert result["2330"] == Money(Decimal("25000"), "TWD")
