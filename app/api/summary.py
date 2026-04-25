"""Top-level dashboard summary: KPIs, equity curve, allocation snapshot."""
from __future__ import annotations

from flask import Blueprint

from ._helpers import envelope, store

bp = Blueprint("summary", __name__, url_prefix="/api/summary")


@bp.get("")
def summary():
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

    last = months[-1]
    first = months[0]
    kpis = dict(s.kpis)

    equity_curve = [
        {
            "month": m["month"],
            "equity_twd": m.get("equity_twd", 0),
            "tw_mv": m.get("tw_market_value_twd", 0),
            "foreign_mv": m.get("foreign_market_value_twd", 0),
            "bank_twd": m.get("bank_twd", 0),
            "bank_usd_in_twd": m.get("bank_usd_in_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
            "cum_twr": m.get("cum_twr", 0),
            "period_return": m.get("period_return", 0),
        }
        for m in months
    ]

    cum_twr = last.get("cum_twr", 0) or 0
    xirr = last.get("xirr")
    profit_twd = kpis.get("profit_twd", 0)
    invested_twd = kpis.get("counterfactual_twd", 0)

    allocation = {
        "tw": last.get("tw_market_value_twd", 0),
        "foreign": last.get("foreign_market_value_twd", 0),
        "bank_twd": last.get("bank_twd", 0),
        "bank_usd": last.get("bank_usd_in_twd", 0),
    }

    return envelope({
        "kpis": kpis,
        "twr": cum_twr,
        "xirr": xirr,
        "profit_twd": profit_twd,
        "invested_twd": invested_twd,
        "equity_curve": equity_curve,
        "allocation": allocation,
        "first_month": first["month"],
        "last_month": last["month"],
        "months_covered": len(months),
    })
