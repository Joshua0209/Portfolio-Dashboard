from decimal import Decimal

import pytest

from invest.domain.money import Money


class TestConstruction:
    def test_holds_amount_and_currency(self):
        m = Money(Decimal("100.50"), "TWD")
        assert m.amount == Decimal("100.50")
        assert m.currency == "TWD"

    def test_currency_is_normalized_to_uppercase(self):
        m = Money(Decimal("10"), "twd")
        assert m.currency == "TWD"

    def test_amount_must_be_decimal(self):
        with pytest.raises(TypeError):
            Money(100.5, "TWD")  # float not allowed

    def test_currency_must_be_three_letters(self):
        with pytest.raises(ValueError):
            Money(Decimal("10"), "TW")
        with pytest.raises(ValueError):
            Money(Decimal("10"), "TWDX")

    def test_is_immutable(self):
        m = Money(Decimal("100"), "TWD")
        with pytest.raises((AttributeError, Exception)):
            m.amount = Decimal("200")  # type: ignore


class TestEquality:
    def test_equal_when_same_amount_and_currency(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("100"), "TWD")
        assert a == b

    def test_not_equal_when_different_currency(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("100"), "USD")
        assert a != b

    def test_hashable(self):
        a = Money(Decimal("100"), "TWD")
        s = {a, Money(Decimal("100"), "TWD"), Money(Decimal("100"), "USD")}
        assert len(s) == 2


class TestAddSubtract:
    def test_add_same_currency(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("50"), "TWD")
        assert a + b == Money(Decimal("150"), "TWD")

    def test_add_different_currency_raises(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("50"), "USD")
        with pytest.raises(ValueError, match="currency mismatch"):
            _ = a + b

    def test_subtract_same_currency(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("30"), "TWD")
        assert a - b == Money(Decimal("70"), "TWD")

    def test_subtract_different_currency_raises(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("30"), "USD")
        with pytest.raises(ValueError, match="currency mismatch"):
            _ = a - b


class TestScalarMultiply:
    def test_multiply_by_int(self):
        m = Money(Decimal("100"), "TWD")
        assert m * 3 == Money(Decimal("300"), "TWD")

    def test_multiply_by_decimal(self):
        m = Money(Decimal("100"), "TWD")
        assert m * Decimal("1.5") == Money(Decimal("150.0"), "TWD")

    def test_multiply_by_float_raises(self):
        m = Money(Decimal("100"), "TWD")
        with pytest.raises(TypeError):
            _ = m * 1.5  # float not allowed

    def test_right_multiply_works(self):
        m = Money(Decimal("100"), "TWD")
        assert 3 * m == Money(Decimal("300"), "TWD")


class TestNegate:
    def test_neg_flips_sign(self):
        m = Money(Decimal("100"), "TWD")
        assert -m == Money(Decimal("-100"), "TWD")

    def test_neg_of_neg_is_original(self):
        m = Money(Decimal("100"), "TWD")
        assert -(-m) == m


class TestConvert:
    def test_convert_to_same_currency_returns_equivalent(self):
        m = Money(Decimal("100"), "TWD")
        assert m.convert_to("TWD", Decimal("1")) == m

    def test_convert_applies_rate(self):
        usd = Money(Decimal("100"), "USD")
        # rate = TWD per 1 USD
        twd = usd.convert_to("TWD", Decimal("32.5"))
        assert twd == Money(Decimal("3250.0"), "TWD")

    def test_convert_with_negative_amount(self):
        usd = Money(Decimal("-50"), "USD")
        twd = usd.convert_to("TWD", Decimal("32"))
        assert twd == Money(Decimal("-1600"), "TWD")


class TestComparison:
    def test_less_than_same_currency(self):
        assert Money(Decimal("100"), "TWD") < Money(Decimal("200"), "TWD")

    def test_compare_different_currency_raises(self):
        a = Money(Decimal("100"), "TWD")
        b = Money(Decimal("100"), "USD")
        with pytest.raises(ValueError, match="currency mismatch"):
            _ = a < b
