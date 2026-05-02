from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal

import pytest

from invest.domain.money import Money
from invest.domain.trade import Side, Trade, Venue


class TestVenue:
    def test_known_values(self):
        assert Venue.TW.value == "TW"
        assert Venue.US.value == "US"
        assert Venue.HK.value == "HK"
        assert Venue.JP.value == "JP"

    def test_is_str_compatible(self):
        # StrEnum members compare equal to their string value
        assert Venue.TW == "TW"


class TestSideDirection:
    def test_cash_buy_is_buy(self):
        assert Side.CASH_BUY.is_buy
        assert not Side.CASH_BUY.is_sell

    def test_cash_sell_is_sell(self):
        assert Side.CASH_SELL.is_sell
        assert not Side.CASH_SELL.is_buy

    def test_margin_buy_is_buy(self):
        assert Side.MARGIN_BUY.is_buy

    def test_margin_sell_is_sell(self):
        assert Side.MARGIN_SELL.is_sell

    def test_short_sell_is_sell_because_it_opens_a_short(self):
        # 券賣 — opening a short position by selling shares not owned.
        # Direction-of-flow is sell, not buy.
        assert Side.SHORT_SELL.is_sell
        assert not Side.SHORT_SELL.is_buy

    def test_short_cover_is_buy_because_it_closes_a_short(self):
        # 券買 — closing a short by buying shares back.
        # Direction-of-flow is buy.
        assert Side.SHORT_COVER.is_buy
        assert not Side.SHORT_COVER.is_sell


class TestTradeConstruction:
    def _minimal_kwargs(self, **overrides):
        defaults = dict(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920.00"), "TWD"),
            venue=Venue.TW,
        )
        defaults.update(overrides)
        return defaults

    def test_minimal_construction(self):
        t = Trade(**self._minimal_kwargs())
        assert t.code == "2330"
        assert t.side is Side.CASH_BUY
        assert t.qty == 1000
        assert t.price == Money(Decimal("920.00"), "TWD")
        assert t.venue is Venue.TW
        assert t.fee is None
        assert t.tax is None
        assert t.rebate is None

    def test_construction_with_costs(self):
        t = Trade(
            **self._minimal_kwargs(
                fee=Money(Decimal("131.10"), "TWD"),
                tax=Money(Decimal("0"), "TWD"),
                rebate=Money(Decimal("13.11"), "TWD"),
            )
        )
        assert t.fee == Money(Decimal("131.10"), "TWD")
        assert t.tax == Money(Decimal("0"), "TWD")
        assert t.rebate == Money(Decimal("13.11"), "TWD")

    def test_is_frozen(self):
        t = Trade(**self._minimal_kwargs())
        with pytest.raises(FrozenInstanceError):
            t.qty = 2000  # type: ignore

    def test_equality(self):
        a = Trade(**self._minimal_kwargs())
        b = Trade(**self._minimal_kwargs())
        assert a == b

    def test_inequality_when_qty_differs(self):
        a = Trade(**self._minimal_kwargs())
        b = Trade(**self._minimal_kwargs(qty=2000))
        assert a != b

    def test_hashable(self):
        a = Trade(**self._minimal_kwargs())
        b = Trade(**self._minimal_kwargs())
        s = {a, b}
        assert len(s) == 1


class TestTradeGrossValue:
    """Convenience: trade.gross_value() = qty * price (always positive,
    direction is in `side`)."""

    def test_gross_value_for_buy(self):
        t = Trade(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920.00"), "TWD"),
            venue=Venue.TW,
        )
        assert t.gross_value() == Money(Decimal("920000.00"), "TWD")

    def test_gross_value_for_sell_is_still_positive(self):
        t = Trade(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_SELL,
            qty=500,
            price=Money(Decimal("950.00"), "TWD"),
            venue=Venue.TW,
        )
        assert t.gross_value() == Money(Decimal("475000.00"), "TWD")
