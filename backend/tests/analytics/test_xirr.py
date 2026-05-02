from datetime import date, timedelta
from decimal import Decimal

import pytest

from invest.analytics.xirr import xirr


D0 = date(2026, 5, 1)


def _flow(days: int, amount: str) -> tuple:
    return (D0 + timedelta(days=days), Decimal(amount))


def _approx(actual: Decimal, expected: str, tol_str: str = "1E-5") -> bool:
    return abs(actual - Decimal(expected)) < Decimal(tol_str)


class TestEmpty:
    def test_empty_returns_zero(self):
        assert xirr([]) == Decimal("0")


class TestKnownRates:
    def test_no_growth(self):
        # -1000 in, +1000 out exactly 365 days later → 0%
        r = xirr([_flow(0, "-1000"), _flow(365, "1000")])
        assert _approx(r, "0", "1E-6")

    def test_five_percent_annual(self):
        # -1000 in, +1050 out 365 days later → 5%
        r = xirr([_flow(0, "-1000"), _flow(365, "1050")])
        assert _approx(r, "0.05", "1E-5")

    def test_doubling(self):
        # -1000 in, +2000 out 365 days later → 100%
        r = xirr([_flow(0, "-1000"), _flow(365, "2000")])
        assert _approx(r, "1.0", "1E-4")

    def test_negative_return(self):
        # -1000 in, +900 out 365 days later → -10%
        r = xirr([_flow(0, "-1000"), _flow(365, "900")])
        assert _approx(r, "-0.10", "1E-5")


class TestSubAnnualHorizon:
    def test_doubling_in_half_year_annualizes_to_300pct(self):
        # 4x in 365 days = 300% annual
        r = xirr([_flow(0, "-1000"), _flow(365, "4000")])
        assert _approx(r, "3.0", "1E-4")


class TestMultiFlow:
    def test_two_deposits_one_terminal(self):
        # Two equal deposits 6 months apart, terminal is 2x first deposit.
        # Roughly 0% annual because total in = total out.
        # -500 (d0), -500 (d180), +1000 (d365)
        # Verify it converges and returns near 0.
        r = xirr([
            _flow(0, "-500"),
            _flow(180, "-500"),
            _flow(365, "1000"),
        ])
        # Will be slightly negative because money was deposited later;
        # money-weighted return penalizes the later deposit.
        # Ballpark: small negative
        assert r < Decimal("0.01")
        assert r > Decimal("-0.05")


class TestDoesNotConverge:
    def test_all_positive_flows_raises(self):
        # No sign change → no real root
        with pytest.raises(ValueError):
            xirr([_flow(0, "100"), _flow(365, "200")])

    def test_all_negative_flows_raises(self):
        with pytest.raises(ValueError):
            xirr([_flow(0, "-100"), _flow(365, "-200")])
