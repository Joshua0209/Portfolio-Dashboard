from datetime import date
from decimal import Decimal

import pytest

from invest.domain.money import Money
from invest.domain.trade import Side, Trade as TradeVO, Venue
from invest.persistence.mappers.trade_mapper import TradeMapper
from invest.persistence.models.trade import Trade as TradeRow


class TestToDomain:
    def test_basic_tw_buy(self):
        row = TradeRow(
            id=1,
            date=date(2026, 5, 1),
            code="2330",
            side=int(Side.CASH_BUY),
            qty=1000,
            price=Decimal("920.00"),
            currency="TWD",
            fee=Decimal("131.10"),
            tax=Decimal("0"),
            rebate=Decimal("13.11"),
            source="pdf",
            venue="TW",
        )
        vo = TradeMapper.to_domain(row)

        assert vo == TradeVO(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920.00"), "TWD"),
            venue=Venue.TW,
            fee=Money(Decimal("131.10"), "TWD"),
            tax=Money(Decimal("0"), "TWD"),
            rebate=Money(Decimal("13.11"), "TWD"),
        )

    def test_casts_int_side_to_enum(self):
        row = _row(side=int(Side.MARGIN_BUY))
        vo = TradeMapper.to_domain(row)
        assert vo.side is Side.MARGIN_BUY

    def test_casts_str_venue_to_enum(self):
        row = _row(venue="US", currency="USD")
        vo = TradeMapper.to_domain(row)
        assert vo.venue is Venue.US

    def test_zero_costs_become_money_zero_not_none(self):
        # 0-cost trade still has a defined currency context.
        row = _row(
            fee=Decimal("0"),
            tax=Decimal("0"),
            rebate=Decimal("0"),
        )
        vo = TradeMapper.to_domain(row)
        assert vo.fee == Money(Decimal("0"), "TWD")
        assert vo.tax == Money(Decimal("0"), "TWD")
        assert vo.rebate == Money(Decimal("0"), "TWD")

    def test_foreign_trade_uses_local_currency_for_costs(self):
        row = _row(
            code="AAPL",
            currency="USD",
            venue="US",
            price=Decimal("180.50"),
            fee=Decimal("1.25"),
        )
        vo = TradeMapper.to_domain(row)
        assert vo.price == Money(Decimal("180.50"), "USD")
        assert vo.fee == Money(Decimal("1.25"), "USD")


class TestToRow:
    def test_basic_conversion_with_explicit_source(self):
        vo = TradeVO(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920.00"), "TWD"),
            venue=Venue.TW,
            fee=Money(Decimal("131.10"), "TWD"),
            tax=Money(Decimal("0"), "TWD"),
            rebate=Money(Decimal("13.11"), "TWD"),
        )
        row = TradeMapper.to_row(vo, source="shioaji")

        assert row.date == date(2026, 5, 1)
        assert row.code == "2330"
        assert row.side == int(Side.CASH_BUY)
        assert row.qty == 1000
        assert row.price == Decimal("920.00")
        assert row.currency == "TWD"
        assert row.fee == Decimal("131.10")
        assert row.tax == Decimal("0")
        assert row.rebate == Decimal("13.11")
        assert row.source == "shioaji"
        assert row.venue == "TW"
        assert row.id is None  # synthetic PK assigned at insert

    def test_none_costs_become_zero_in_row(self):
        # VO allows None for costs (trade_seeder before fee calc);
        # row schema requires Decimal — mapper writes 0.
        vo = TradeVO(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920.00"), "TWD"),
            venue=Venue.TW,
            fee=None,
            tax=None,
            rebate=None,
        )
        row = TradeMapper.to_row(vo, source="manual")
        assert row.fee == Decimal("0")
        assert row.tax == Decimal("0")
        assert row.rebate == Decimal("0")

    def test_to_row_rejects_cost_in_different_currency(self):
        # If a Trade VO is constructed with fee in a different currency
        # from price, that's a data-integrity bug (the row schema only
        # has one `currency` column). Mapper must catch it.
        vo = TradeVO(
            date=date(2026, 5, 1),
            code="2330",
            side=Side.CASH_BUY,
            qty=1000,
            price=Money(Decimal("920"), "TWD"),
            venue=Venue.TW,
            fee=Money(Decimal("1"), "USD"),  # mismatched
        )
        with pytest.raises(ValueError, match="currency mismatch"):
            TradeMapper.to_row(vo, source="manual")


class TestRoundTrip:
    def test_row_to_domain_to_row_preserves_business_fields(self):
        original = TradeRow(
            date=date(2026, 5, 15),
            code="2454",
            side=int(Side.MARGIN_BUY),
            qty=500,
            price=Decimal("1180.50"),
            currency="TWD",
            fee=Decimal("84.21"),
            tax=Decimal("0"),
            rebate=Decimal("12.00"),
            source="pdf",
            venue="TW",
        )
        round_tripped = TradeMapper.to_row(
            TradeMapper.to_domain(original),
            source="pdf",
        )
        assert round_tripped.date == original.date
        assert round_tripped.code == original.code
        assert round_tripped.side == original.side
        assert round_tripped.qty == original.qty
        assert round_tripped.price == original.price
        assert round_tripped.currency == original.currency
        assert round_tripped.fee == original.fee
        assert round_tripped.tax == original.tax
        assert round_tripped.rebate == original.rebate
        assert round_tripped.source == original.source
        assert round_tripped.venue == original.venue


def _row(**overrides) -> TradeRow:
    defaults = dict(
        date=date(2026, 5, 1),
        code="2330",
        side=int(Side.CASH_BUY),
        qty=1000,
        price=Decimal("920.00"),
        currency="TWD",
        fee=Decimal("0"),
        tax=Decimal("0"),
        rebate=Decimal("0"),
        source="pdf",
        venue="TW",
    )
    defaults.update(overrides)
    return TradeRow(**defaults)
