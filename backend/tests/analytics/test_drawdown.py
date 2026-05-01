from decimal import Decimal

import pytest

from invest.analytics.drawdown import max_drawdown, underwater_curve


def _D(*vals) -> list:
    return [Decimal(str(v)) for v in vals]


class TestMaxDrawdown:
    def test_empty_is_zero(self):
        assert max_drawdown([]) == Decimal("0")

    def test_single_point_is_zero(self):
        assert max_drawdown(_D(1000)) == Decimal("0")

    def test_monotonic_increase_is_zero(self):
        assert max_drawdown(_D(1000, 1100, 1200, 1300)) == Decimal("0")

    def test_single_drop(self):
        # Peak 1000 -> trough 900 = -10%
        assert max_drawdown(_D(1000, 900)) == Decimal("-0.1")

    def test_recover_then_drop_lower(self):
        # Two drawdown candidates: 1000 -> 950 (-5%), then 1100 -> 880 (-20%)
        # Bigger one wins.
        result = max_drawdown(_D(1000, 950, 1100, 880))
        assert result == Decimal("-0.2")

    def test_recover_back_to_peak(self):
        # Drop and full recovery — max-DD is the depth of the drop.
        result = max_drawdown(_D(1000, 800, 1000))
        assert result == Decimal("-0.2")

    def test_negative_or_zero_invariant(self):
        # Drawdown is by definition ≤ 0.
        for series in [
            _D(1000, 900, 800),
            _D(500, 1000, 750, 800),
            _D(100, 200, 50, 300, 25),
        ]:
            assert max_drawdown(series) <= Decimal("0")


class TestUnderwaterCurve:
    def test_empty(self):
        assert underwater_curve([]) == []

    def test_length_matches_input(self):
        result = underwater_curve(_D(100, 110, 105, 120))
        assert len(result) == 4

    def test_monotonic_increase_all_zero(self):
        result = underwater_curve(_D(1000, 1100, 1200))
        assert result == _D(0, 0, 0)

    def test_drop_then_recover(self):
        # 1000 (peak), 900 (-10%), 1000 (back to peak, 0%)
        result = underwater_curve(_D(1000, 900, 1000))
        assert result[0] == Decimal("0")
        assert result[1] == Decimal("-0.1")
        assert result[2] == Decimal("0")

    def test_underwater_after_new_peak(self):
        # 1000 (peak), 900 (-10%), 1100 (new peak), 990 (-10%)
        result = underwater_curve(_D(1000, 900, 1100, 990))
        assert result[3] == Decimal("-0.1")

    def test_zero_or_negative_peak_returns_zero(self):
        # If peak is 0 (or never positive), drawdown is undefined → 0
        result = underwater_curve(_D(0, 0, 0))
        assert result == _D(0, 0, 0)
