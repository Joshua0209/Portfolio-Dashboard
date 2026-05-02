"""Money-weighted (annualized internal rate of return) calculation.
Newton-Raphson solver for: sum(amount_i / (1+r)^(t_i / 365)) = 0
where t_i is days from the earliest flow.
Float-domain inner loop because Decimal does not natively support
non-integer exponents (would need ln/exp composition); the iteration
tolerance dominates precision needs anyway. Result is converted back
to Decimal at the boundary so downstream analytics math (Sharpe etc.)
stays in Decimal.
"""
from datetime import date as _date
from decimal import Decimal
from typing import List, Tuple
_DEFAULT_GUESS = Decimal("0.1")
_DEFAULT_TOL = Decimal("1E-7")
_DEFAULT_MAX_ITER = 100
def xirr(
    flows: List[Tuple[_date, Decimal]],
    guess: Decimal = _DEFAULT_GUESS,
    *,
    max_iter: int = _DEFAULT_MAX_ITER,
    tol: Decimal = _DEFAULT_TOL,
) -> Decimal:
    if not flows:
        return Decimal("0")
    sorted_flows = sorted(flows, key=lambda x: x[0])
    t0 = sorted_flows[0][0]
    days = [(d - t0).days for d, _ in sorted_flows]
    amounts = [float(a) for _, a in sorted_flows]
    if all(a >= 0 for a in amounts) or all(a <= 0 for a in amounts):
        raise ValueError(
            "XIRR requires at least one positive and one negative flow"
        )
    r = float(guess)
    tol_f = float(tol)
    for _ in range(max_iter):
        if r <= -1.0:
            raise ValueError("XIRR diverged: rate -> -100% or below")
        npv = 0.0
        d_npv = 0.0
        one_plus_r = 1.0 + r
        for t, a in zip(days, amounts):
            exp = t / 365.0
            f = one_plus_r ** exp
            npv += a / f
            d_npv -= exp * a / (f * one_plus_r)
        if d_npv == 0.0:
            raise ValueError("XIRR derivative reached zero; cannot iterate")
        delta = npv / d_npv
        r_new = r - delta
        if abs(r_new - r) < tol_f:
            return Decimal(str(r_new))
        r = r_new
    raise ValueError(f"XIRR did not converge within {max_iter} iterations")
