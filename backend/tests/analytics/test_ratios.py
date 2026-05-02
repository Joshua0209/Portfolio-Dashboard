from decimal import Decimal

import pytest

from invest.analytics.ratios import calmar, sharpe, sortino


def _D(*vals) -> list:
    return [Decimal(str(v)) for v in vals]


class TestSharpe:
    def test_empty_or_singleton_is_zero(self):
        assert sharpe([]) == Decimal("0")
        assert sharpe(_D(0.05)) == Decimal("0")  # need >=2 for variance

    def test_constant_returns_zero_volatility_zero_sharpe(self):
        # No volatility → undefined → 0 by convention
        assert sharpe(_D(0.05, 0.05, 0.05, 0.05)) == Decimal("0")

    def test_known_value(self):
        # Monthly returns: 0.10, 0.05, 0.15
        # mean = 0.10, sample stdev = 0.05
        # period sharpe = 0.10 / 0.05 = 2.0
        # annualized = 2.0 * sqrt(12) ≈ 6.928
        result = sharpe(_D(0.10, 0.05, 0.15))
        expected = Decimal("2.0") * Decimal("12").sqrt()
        assert abs(result - expected) < Decimal("1E-10")

    def test_higher_volatility_lowers_sharpe(self):
        # Same mean, higher spread → lower Sharpe
        a = sharpe(_D(0.10, 0.05, 0.15))
        b = sharpe(_D(0.10, -0.10, 0.30))  # same mean (10), wider spread
        assert b < a

    def test_negative_returns_negative_sharpe(self):
        # Mean < risk_free=0 should give negative Sharpe
        assert sharpe(_D(-0.05, -0.10, -0.03)) < Decimal("0")

    def test_risk_free_subtracted(self):
        # Risk-free 12% annual = 1% monthly. Returns averaging 1% should give Sharpe = 0.
        result = sharpe(_D(0.005, 0.015, 0.01), risk_free=Decimal("0.12"))
        assert abs(result) < Decimal("1E-10")


class TestSortino:
    def test_empty_or_singleton_is_zero(self):
        assert sortino([]) == Decimal("0")
        assert sortino(_D(0.05)) == Decimal("0")

    def test_no_downside_returns_zero(self):
        # All positive returns — no negatives — convention: 0
        # (some libraries return infinity; we choose 0 to keep banner-safe)
        assert sortino(_D(0.05, 0.10, 0.03)) == Decimal("0")

    def test_with_downside_uses_only_negative_deviation(self):
        # Sortino should be HIGHER than Sharpe for the same series
        # because Sortino ignores upside volatility.
        returns = _D(0.10, -0.05, 0.15, -0.02, 0.08)
        assert sortino(returns) > sharpe(returns)

    def test_risk_free_subtracted(self):
        # A Sortino with a risk_free equal to mean return should give a
        # result < 0 (mean - rf < 0) even when there is downside.
        # With rf=0 (default) the same series gives a positive Sortino.
        returns = _D(0.02, -0.03, 0.04, -0.01, 0.02)
        rf_zero = sortino(returns, risk_free=Decimal("0"))
        # A high annual risk-free (e.g. 36% → 3% per period) should push
        # the ratio down significantly.
        rf_high = sortino(returns, risk_free=Decimal("0.36"))
        assert rf_high < rf_zero


class TestCalmar:
    def test_empty_is_zero(self):
        assert calmar([]) == Decimal("0")

    def test_no_drawdown_is_zero(self):
        # Monotonic up — no DD — convention: 0 (avoid infinity)
        assert calmar(_D(0.05, 0.05, 0.05, 0.05)) == Decimal("0")

    def test_known_value(self):
        # Returns: +20%, -50%, +50%
        # Equity curve: 1.0 → 1.2 → 0.6 → 0.9
        # Max DD: peak 1.2, trough 0.6 → -0.5
        # Cumulative return: 0.9 - 1.0 = -0.1
        # Annualized over 3 periods at 12/year: (1 + (-0.1)) ^ (12/3) - 1
        #   = 0.9^4 - 1 = 0.6561 - 1 = -0.3439
        # Calmar = -0.3439 / 0.5 = -0.6878
        result = calmar(_D(0.20, -0.50, 0.50))
        # Wide tolerance because annualized exponent goes via float.
        assert abs(result - Decimal("-0.6878")) < Decimal("1E-3")

    def test_loss_yields_negative_calmar(self):
        result = calmar(_D(-0.05, -0.10, -0.05))
        assert result < Decimal("0")
