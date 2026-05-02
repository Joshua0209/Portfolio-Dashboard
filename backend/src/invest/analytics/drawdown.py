"""Drawdown calculations: max-DD scalar + underwater curve series.
Drawdown is always <= 0 by definition (you cannot be 'above' your own
peak). Both functions are pure: take a sequence of equity values,
return Decimal scalar/list. No I/O.
"""
from decimal import Decimal
from typing import List
_ZERO = Decimal("0")
def max_drawdown(equity: List[Decimal]) -> Decimal:
    """Largest peak-to-trough decline over the series, as a fraction
    of the peak. Result <= 0."""
    if not equity:
        return _ZERO
    peak = equity[0]
    worst = _ZERO
    for v in equity:
        if v > peak:
            peak = v
        if peak > _ZERO:
            dd = (v - peak) / peak
            if dd < worst:
                worst = dd
    return worst
def underwater_curve(equity: List[Decimal]) -> List[Decimal]:
    """For each value, the percentage below the running peak.
    Length matches input."""
    out: List[Decimal] = []
    peak = _ZERO
    for v in equity:
        if v > peak:
            peak = v
        if peak > _ZERO:
            out.append((v - peak) / peak)
        else:
            out.append(_ZERO)
    return out
