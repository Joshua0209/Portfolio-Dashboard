"""Risk-adjusted return ratios + supporting volatility primitives.

  stdev          : sample stdev (Bessel's correction)
  downside_stdev : sample stdev of below-target observations, centered
                   at target. Divides by (n − 1) using the FULL series
                   count, matching the Sortino convention.
  sharpe         : (mean − rf_per_period) / stdev_all  × √(periods/year)
  sortino        : (mean − rf_per_period) / downside_stdev × √(periods/year)
  calmar         : annualized_return / |max_drawdown|

Convention: when the denominator would be zero (constant returns, no
downside, no drawdown) the function returns 0 instead of raising or
returning infinity. Keeps the dashboard banner-safe.
"""
from decimal import Decimal
from typing import List

from invest.analytics.drawdown import max_drawdown

_ZERO = Decimal("0")
_ONE = Decimal("1")


def _mean(xs: List[Decimal]) -> Decimal:
    return sum(xs, _ZERO) / Decimal(len(xs))


def _sample_stdev(xs: List[Decimal], mean: Decimal) -> Decimal:
    n = len(xs)
    if n < 2:
        return _ZERO
    variance = sum(((x - mean) ** 2 for x in xs), _ZERO) / Decimal(n - 1)
    return variance.sqrt()


def stdev(values: List[Decimal]) -> Decimal:
    """Sample standard deviation. Returns 0 for fewer than 2 inputs."""
    if len(values) < 2:
        return _ZERO
    return _sample_stdev(values, _mean(values))


def downside_stdev(values: List[Decimal], target: Decimal = _ZERO) -> Decimal:
    """Sample stdev of below-target observations, centered at target.

    Divides by (len(values) − 1) — the FULL series degrees of freedom,
    not just the count of below-target values. This matches the
    convention used inside `sortino` and is the modern downside-risk
    definition (Sortino & Price 1994). Differs from the legacy
    monthly.downside_stdev which used population stdev of negatives.
    """
    if len(values) < 2:
        return _ZERO
    below_squared = [(v - target) ** 2 for v in values if v < target]
    if not below_squared:
        return _ZERO
    variance = sum(below_squared, _ZERO) / Decimal(len(values) - 1)
    return variance.sqrt()


def sharpe(
    returns: List[Decimal],
    risk_free: Decimal = _ZERO,
    periods_per_year: int = 12,
) -> Decimal:
    if len(returns) < 2:
        return _ZERO
    mean = _mean(returns)
    sd = _sample_stdev(returns, mean)
    if sd == _ZERO:
        return _ZERO
    rf_per_period = risk_free / Decimal(periods_per_year)
    annualization = Decimal(periods_per_year).sqrt()
    return ((mean - rf_per_period) / sd) * annualization


def sortino(
    returns: List[Decimal],
    risk_free: Decimal = _ZERO,
    periods_per_year: int = 12,
) -> Decimal:
    if len(returns) < 2:
        return _ZERO
    dsd = downside_stdev(returns, target=_ZERO)
    if dsd == _ZERO:
        return _ZERO
    mean = _mean(returns)
    rf_per_period = risk_free / Decimal(periods_per_year)
    annualization = Decimal(periods_per_year).sqrt()
    return ((mean - rf_per_period) / dsd) * annualization


def calmar(
    returns: List[Decimal],
    periods_per_year: int = 12,
) -> Decimal:
    if not returns:
        return _ZERO
    equity: List[Decimal] = [_ONE]
    for r in returns:
        equity.append(equity[-1] * (_ONE + r))
    cumulative = equity[-1] - _ONE
    n = len(returns)
    one_plus_cum = float(_ONE + cumulative)
    if one_plus_cum <= 0.0:
        annualized = -_ONE
    else:
        annualized_f = one_plus_cum ** (periods_per_year / n) - 1.0
        annualized = Decimal(str(annualized_f))
    mdd = max_drawdown(equity)
    if mdd == _ZERO:
        return _ZERO
    return annualized / abs(mdd)
