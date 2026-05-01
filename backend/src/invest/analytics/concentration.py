"""Concentration metrics: HHI and top-N share.
Both functions are weight-agnostic — pass a list of any positive
Decimals (market values, sector totals, currency exposures) and get
back a fraction.
"""
from decimal import Decimal
from typing import List
_ZERO = Decimal("0")
_ONE = Decimal("1")
def hhi(weights: List[Decimal]) -> Decimal:
    """Herfindahl-Hirschman Index: sum(w_i^2) where w_i is each
    weight as a fraction of the total.
    Bounds: 1/N (equal split across N positions) to 1.0 (single
    position holds everything). Returns 0 for empty/zero-total input.
    """
    total = sum(weights, _ZERO)
    if total <= _ZERO:
        return _ZERO
    return sum(((w / total) ** 2 for w in weights), _ZERO)
def top_n_share(weights: List[Decimal], n: int) -> Decimal:
    """Fraction of total held by the n largest entries.
    Sorts internally so input order does not matter. n > len(weights)
    returns 1.0; n == 0 returns 0.
    """
    if n <= 0 or not weights:
        return _ZERO
    total = sum(weights, _ZERO)
    if total <= _ZERO:
        return _ZERO
    sorted_desc = sorted(weights, reverse=True)
    top_n = sum(sorted_desc[:n], _ZERO)
    return top_n / total
