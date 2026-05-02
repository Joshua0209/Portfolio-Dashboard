"""Time-weighted return (TWR) calculations.
Modified Dietz monthly TWR with three weighting variants, plus a
chain compounder for multi-period totals.
All math is in Decimal — float drift would corrupt the long-tail
returns that downstream metrics (Sharpe, Sortino) depend on.
"""
from datetime import date as _date
from decimal import Decimal
from typing import List
from invest.domain.cashflow import Cashflow
from invest.domain.money import Money
_VALID_METHODS = frozenset({"day_weighted", "mid_month", "eom"})
_HALF = Decimal("0.5")
_ZERO = Decimal("0")
_ONE = Decimal("1")
def _weight(
    method: str,
    period_start: _date,
    period_end: _date,
    flow_date: _date,
) -> Decimal:
    if method == "mid_month":
        return _HALF
    if method == "eom":
        return _ZERO
    period_days = Decimal((period_end - period_start).days)
    if period_days == 0:
        return _HALF
    elapsed = Decimal((flow_date - period_start).days)
    raw = (period_days - elapsed) / period_days
    # Clamp to [0, 1]: a flow_date outside [period_start, period_end]
    # would otherwise produce a weight < 0 (after period_end) or > 1
    # (before period_start), both of which silently corrupt the
    # denominator.  Clamping is the more defensive choice here —
    # invalid dates still contribute to the numerator (they affect
    # net flows) but carry a bounded weight.
    if raw < _ZERO:
        return _ZERO
    if raw > _ONE:
        return _ONE
    return raw
def modified_dietz(
    start_equity: Money,
    end_equity: Money,
    cashflows: List[Cashflow],
    period_start: _date,
    period_end: _date,
    method: str = "day_weighted",
) -> Decimal:
    """Modified Dietz return for a single period.
    r = (V_end - V_start - F) / (V_start + sum(W_i * F_i))
    F is the sum of EXTERNAL signed cashflows in the period; internal
    flows (dividends, interest, rebates) are excluded since they are
    already reflected in the equity curve.
    """
    if method not in _VALID_METHODS:
        raise ValueError(
            f"unknown method: {method!r}; must be one of "
            f"{sorted(_VALID_METHODS)}"
        )
    external = [cf for cf in cashflows if cf.is_external]
    flow_total = sum(
        (cf.amount.amount for cf in external),
        _ZERO,
    )
    weighted_total = sum(
        (
            _weight(method, period_start, period_end, cf.date)
            * cf.amount.amount
            for cf in external
        ),
        _ZERO,
    )
    numerator = end_equity.amount - start_equity.amount - flow_total
    denominator = start_equity.amount + weighted_total
    if denominator == _ZERO:
        return _ZERO
    return numerator / denominator
def twr_chain(returns: List[Decimal]) -> Decimal:
    """Cumulative TWR: product(1 + r_i) - 1.
    Empty list returns 0 (no periods, no return).
    """
    if not returns:
        return _ZERO
    cumulative = _ONE
    for r in returns:
        cumulative *= _ONE + r
    return cumulative - _ONE
