"""FX + price decomposition for foreign-currency positions.
Pure functions over (start_value, end_value, start_fx, end_fx) inputs.
No I/O.
"""
from decimal import Decimal
from typing import Dict
from invest.domain.money import Money
_ZERO = Decimal("0")
def fx_attribution(
    start_value_local: Money,
    end_value_local: Money,
    start_fx: Decimal,
    end_fx: Decimal,
) -> Dict[str, Decimal]:
    """Decompose total TWD return on a foreign-currency position into
    price, FX, and cross-term components.
      r_total = (1 + r_local) * (1 + r_fx) - 1
              = r_local + r_fx + (r_local * r_fx)
    Returns a dict with keys 'price', 'fx', 'cross', 'total'.
    """
    if start_value_local.currency != end_value_local.currency:
        raise ValueError(
            f"currency mismatch: start={start_value_local.currency} "
            f"end={end_value_local.currency}"
        )
    if start_value_local.amount == _ZERO:
        raise ValueError("start_value_local cannot be zero")
    if start_fx == _ZERO:
        raise ValueError("start_fx cannot be zero")
    r_local = (end_value_local.amount - start_value_local.amount) / start_value_local.amount
    r_fx = (end_fx - start_fx) / start_fx
    cross = r_local * r_fx
    total = r_local + r_fx + cross
    return {
        "price": r_local,
        "fx": r_fx,
        "cross": cross,
        "total": total,
    }
