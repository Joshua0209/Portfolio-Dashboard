"""FX + price decomposition.

Two scopes:

  fx_attribution(start_local, end_local, start_fx, end_fx)
      Per-position three-way decomposition of foreign-currency total
      return into price, FX, and cross components.

  usd_exposure_walk(months)
      Whole-portfolio sequential FX P&L walk over a month sequence.
      Treats (bank USD + foreign equity) as the USD-exposed base each
      month and credits the period's TWD delta to FX. Powers the
      monthly bar chart on /fx.

Both pure / no I/O.
"""
from decimal import Decimal
from typing import Any, Dict, List
from invest.domain.money import Money
_ZERO = Decimal("0")
_ONE = Decimal("1")
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


def usd_exposure_walk(months: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Whole-portfolio FX P&L walk across a month sequence.

    For each month i ≥ 1, compute:
        usd_held_twd_{i-1} = bank_usd_in_twd_{i-1} + foreign_market_value_twd_{i-1}
        usd_amount         = usd_held_twd_{i-1} / fx_{i-1}
        fx_pnl_twd         = usd_amount × (fx_i − fx_{i-1})

    Cumulates monthly. Returns:
        {"contribution_twd": <total>, "monthly": [<per-month rows>]}

    Float math (legacy convention — `months` is the legacy list[dict]
    shape from PortfolioStore). When the dual-SoT migration lands, an
    equivalent Decimal version can sit alongside.
    """
    if len(months) < 2:
        return {"contribution_twd": 0, "monthly": []}

    monthly_rows: List[Dict[str, Any]] = []
    cumulative = 0.0
    for i in range(1, len(months)):
        prev = months[i - 1]
        curr = months[i]
        prev_fx = prev.get("fx_usd_twd") or 1
        curr_fx = curr.get("fx_usd_twd") or prev_fx
        usd_held_twd = (prev.get("bank_usd_in_twd", 0) or 0) + (prev.get("foreign_market_value_twd", 0) or 0)
        usd_amount = (usd_held_twd / prev_fx) if prev_fx else 0
        delta_twd = usd_amount * (curr_fx - prev_fx)
        cumulative += delta_twd
        monthly_rows.append({
            "month": curr["month"],
            "fx_usd_twd": curr_fx,
            "usd_amount": usd_amount,
            "fx_pnl_twd": delta_twd,
            "cumulative_fx_pnl_twd": cumulative,
        })
    return {"contribution_twd": cumulative, "monthly": monthly_rows}
