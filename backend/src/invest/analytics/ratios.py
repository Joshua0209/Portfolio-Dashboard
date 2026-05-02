"""Risk-adjusted return ratios: Sharpe, Sortino, Calmar.
Each takes a list of period returns (Decimal) and produces an
annualized Decimal scalar.
  Sharpe : (mean - rf_per_period) / stdev_all  * sqrt(periods/year)
  Sortino: (mean - rf_per_period) / stdev_negs * sqrt(periods/year)
  Calmar : annualized_return / |max_drawdown|
Convention: when the denominator would be zero (constant returns,
no downside, no drawdown) the function returns 0 instead of raising
or returning infinity. This keeps the dashboard banner-safe.
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
def sharpe(
    returns: List[Decimal],
    risk_free: Decimal = _ZERO,
    periods_per_year: int = 12,
) -> Decimal:
    if len(returns) < 2:
        return _ZERO
    mean = _mean(returns)
    stdev = _sample_stdev(returns, mean)
    if stdev == _ZERO:
        return _ZERO
    rf_per_period = risk_free / Decimal(periods_per_year)
    annualization = Decimal(periods_per_year).sqrt()
    return ((mean - rf_per_period) / stdev) * annualization
def sortino(
    returns: List[Decimal],
    risk_free: Decimal = _ZERO,
    periods_per_year: int = 12,
) -> Decimal:
    if len(returns) < 2:
        return _ZERO
    mean = _mean(returns)
    negatives = [r for r in returns if r < _ZERO]
    if not negatives:
        return _ZERO
    n = len(returns)
    downside_var = sum((r ** 2 for r in negatives), _ZERO) / Decimal(n - 1)
    downside_stdev = downside_var.sqrt()
    if downside_stdev == _ZERO:
        return _ZERO
    rf_per_period = risk_free / Decimal(periods_per_year)
    annualization = Decimal(periods_per_year).sqrt()
    return ((mean - rf_per_period) / downside_stdev) * annualization
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
