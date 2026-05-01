from decimal import Decimal

import pytest

from invest.analytics.attribution import fx_attribution
from invest.domain.money import Money


class TestFxAttribution:
    def test_no_change_zero_decomposition(self):
        result = fx_attribution(
            start_value_local=Money(Decimal("100"), "USD"),
            end_value_local=Money(Decimal("100"), "USD"),
            start_fx=Decimal("32"),
            end_fx=Decimal("32"),
        )
        assert result["price"] == Decimal("0")
        assert result["fx"] == Decimal("0")
        assert result["cross"] == Decimal("0")
        assert result["total"] == Decimal("0")

    def test_pure_price_appreciation_no_fx_change(self):
        # +10% local, 0% FX -> total = 10%, all in price
        result = fx_attribution(
            start_value_local=Money(Decimal("100"), "USD"),
            end_value_local=Money(Decimal("110"), "USD"),
            start_fx=Decimal("32"),
            end_fx=Decimal("32"),
        )
        assert result["price"] == Decimal("0.10")
        assert result["fx"] == Decimal("0")
        assert result["cross"] == Decimal("0")
        assert result["total"] == Decimal("0.10")

    def test_pure_fx_gain_no_price_change(self):
        # 0% local, +5% FX (32 -> 33.6) -> total = 5%, all in FX
        result = fx_attribution(
            start_value_local=Money(Decimal("100"), "USD"),
            end_value_local=Money(Decimal("100"), "USD"),
            start_fx=Decimal("32"),
            end_fx=Decimal("33.6"),
        )
        assert result["price"] == Decimal("0")
        assert result["fx"] == Decimal("0.05")
        assert result["cross"] == Decimal("0")
        assert result["total"] == Decimal("0.05")

    def test_both_components_compose_with_cross_term(self):
        # +10% local, +5% FX
        # r_total = 1.10 * 1.05 - 1 = 0.155
        # r_price = 0.10, r_fx = 0.05, cross = 0.005
        # 0.10 + 0.05 + 0.005 = 0.155 ✓
        result = fx_attribution(
            start_value_local=Money(Decimal("100"), "USD"),
            end_value_local=Money(Decimal("110"), "USD"),
            start_fx=Decimal("32"),
            end_fx=Decimal("33.6"),
        )
        assert abs(result["price"] - Decimal("0.10")) < Decimal("1E-10")
        assert abs(result["fx"] - Decimal("0.05")) < Decimal("1E-10")
        assert abs(result["cross"] - Decimal("0.005")) < Decimal("1E-10")
        assert abs(result["total"] - Decimal("0.155")) < Decimal("1E-10")

    def test_components_sum_to_total(self):
        # Identity: price + fx + cross == total, always.
        cases = [
            ("100", "120", "32", "30"),    # gain + FX loss
            ("200", "150", "32", "35"),    # loss + FX gain
            ("100", "100", "32", "32"),    # flat
            ("100", "150", "32", "32"),    # pure gain
        ]
        for s_l, e_l, s_fx, e_fx in cases:
            r = fx_attribution(
                start_value_local=Money(Decimal(s_l), "USD"),
                end_value_local=Money(Decimal(e_l), "USD"),
                start_fx=Decimal(s_fx),
                end_fx=Decimal(e_fx),
            )
            sum_components = r["price"] + r["fx"] + r["cross"]
            assert abs(sum_components - r["total"]) < Decimal("1E-10")

    def test_zero_start_value_raises(self):
        with pytest.raises(ValueError, match="zero"):
            fx_attribution(
                start_value_local=Money(Decimal("0"), "USD"),
                end_value_local=Money(Decimal("100"), "USD"),
                start_fx=Decimal("32"),
                end_fx=Decimal("32"),
            )

    def test_zero_start_fx_raises(self):
        with pytest.raises(ValueError, match="zero"):
            fx_attribution(
                start_value_local=Money(Decimal("100"), "USD"),
                end_value_local=Money(Decimal("110"), "USD"),
                start_fx=Decimal("0"),
                end_fx=Decimal("32"),
            )

    def test_currency_mismatch_raises(self):
        with pytest.raises(ValueError, match="currency mismatch"):
            fx_attribution(
                start_value_local=Money(Decimal("100"), "USD"),
                end_value_local=Money(Decimal("110"), "TWD"),
                start_fx=Decimal("32"),
                end_fx=Decimal("32"),
            )
