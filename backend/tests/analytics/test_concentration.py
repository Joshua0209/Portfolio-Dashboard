from decimal import Decimal

import pytest

from invest.analytics.concentration import hhi, top_n_share


def _D(*vals) -> list:
    return [Decimal(str(v)) for v in vals]


class TestHHI:
    def test_empty_is_zero(self):
        assert hhi([]) == Decimal("0")

    def test_zero_total_is_zero(self):
        assert hhi(_D(0, 0, 0)) == Decimal("0")

    def test_single_position_is_one(self):
        # 100% concentrated → HHI = 1.0
        assert hhi(_D(1000)) == Decimal("1")

    def test_equal_split_across_n_is_one_over_n(self):
        # 4 equal positions → HHI = 4 * (0.25)^2 = 0.25
        assert hhi(_D(100, 100, 100, 100)) == Decimal("0.25")

    def test_two_unequal(self):
        # 75/25 split → HHI = 0.75^2 + 0.25^2 = 0.5625 + 0.0625 = 0.625
        assert hhi(_D(75, 25)) == Decimal("0.625")

    def test_invariant_bounds(self):
        # Lower bound is 1/N (perfect diversification), upper is 1.
        for weights, n in [
            (_D(1, 1, 1, 1, 1), 5),
            (_D(100, 50, 25, 10, 5), 5),
            (_D(99.5, 0.1, 0.1, 0.1, 0.2), 5),
        ]:
            h = hhi(weights)
            assert h >= Decimal("1") / Decimal(n) - Decimal("1E-10")
            assert h <= Decimal("1")


class TestTopNShare:
    def test_empty_is_zero(self):
        assert top_n_share([], 5) == Decimal("0")

    def test_n_larger_than_count_returns_one(self):
        # Sum of all positions / total = 1
        assert top_n_share(_D(100, 50, 25), 10) == Decimal("1")

    def test_top_one(self):
        # Largest position is 100 out of total 175
        result = top_n_share(_D(100, 50, 25), 1)
        assert result == Decimal("100") / Decimal("175")

    def test_top_two(self):
        # Top 2: 100 + 50 = 150 out of 175
        result = top_n_share(_D(100, 50, 25), 2)
        assert result == Decimal("150") / Decimal("175")

    def test_unsorted_input_handled(self):
        # Should pick the largest regardless of input order
        result = top_n_share(_D(25, 100, 50), 1)
        assert result == Decimal("100") / Decimal("175")

    def test_n_zero_returns_zero(self):
        assert top_n_share(_D(100, 50, 25), 0) == Decimal("0")
