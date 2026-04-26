"""FX exposure and P&L attribution."""
from __future__ import annotations

from collections import defaultdict

from flask import Blueprint

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("fx", __name__, url_prefix="/api/fx")


@bp.get("")
def fx():
    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "rate_curve": [],
            "current_rate": None,
            "first_rate": None,
            "by_ccy_monthly": [],
            "fx_pnl": {"contribution_twd": 0, "monthly": []},
            "foreign_share": 0,
            "foreign_value_twd": 0,
        })

    fx_pnl = analytics.fx_pnl(months)

    rate_curve = [
        {"month": m["month"], "fx_usd_twd": m.get("fx_usd_twd")}
        for m in months
    ]

    by_ccy_exposure = []
    for m in months:
        ccy_mv: dict[str, float] = defaultdict(float)
        ccy_mv["TWD"] += m.get("tw_market_value_twd", 0) or 0
        ccy_mv["TWD"] += m.get("bank_twd", 0) or 0
        # bank_usd_in_twd is denominated in TWD but represents USD holdings
        ccy_mv["USD"] += m.get("bank_usd_in_twd", 0) or 0
        for h in m.get("foreign", {}).get("holdings", []) or []:
            ccy = h.get("ccy", "USD")
            local_mv = h.get("mkt_value", 0) or 0
            fx_rate = m.get("fx_usd_twd", 1) if ccy == "USD" else 1.0
            ccy_mv[ccy] += local_mv * fx_rate
        by_ccy_exposure.append({"month": m["month"], **ccy_mv})

    last = months[-1]
    last_total = (last.get("equity_twd", 0) or 0) - (last.get("bank_twd", 0) or 0)
    foreign_mv = last.get("foreign_market_value_twd", 0) or 0
    bank_usd_in_twd = last.get("bank_usd_in_twd", 0) or 0
    foreign_share = (foreign_mv + bank_usd_in_twd) / last_total if last_total else 0

    return envelope({
        "rate_curve": rate_curve,
        "current_rate": last.get("fx_usd_twd"),
        "first_rate": months[0].get("fx_usd_twd"),
        "by_ccy_monthly": by_ccy_exposure,
        "fx_pnl": fx_pnl,
        "foreign_share": foreign_share,
        "foreign_value_twd": foreign_mv + bank_usd_in_twd,
    })
