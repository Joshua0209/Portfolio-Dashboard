"""Cashflow audit trail: real vs counterfactual."""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("cashflows", __name__, url_prefix="/api/cashflows")


@bp.get("/monthly")
def monthly():
    s = store()
    return envelope(analytics.monthly_flows(s.months, s.venue_flows_twd))


@bp.get("/cumulative")
def cumulative():
    s = store()
    flows = s.cumulative_flows or {}
    kpis = s.kpis or {}
    months = s.months

    counterfactual_curve = []
    real_curve = []
    cum_external = 0.0
    for m in months:
        cum_external += m.get("external_flow_twd", 0) or 0
        counterfactual_curve.append({
            "month": m["month"],
            "value": cum_external,
        })
        real_curve.append({
            "month": m["month"],
            "value": m.get("equity_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
        })

    return envelope({
        "cumulative_flows": flows,
        "real_now_twd": kpis.get("real_now_twd", 0),
        "counterfactual_twd": kpis.get("counterfactual_twd", 0),
        "profit_twd": kpis.get("profit_twd", 0),
        "real_curve": real_curve,
        "counterfactual_curve": counterfactual_curve,
    })


@bp.get("/bank")
def bank_transactions():
    """Bank account ledger — capital source view."""
    s = store()
    rows = []
    for m in s.months:
        bank = m.get("bank", {})
        for tx in bank.get("tx_twd", []) or []:
            rows.append({
                "month": m["month"],
                "ccy": "TWD",
                "fx": bank.get("fx"),
                **tx,
            })
        for tx in bank.get("tx_foreign", []) or []:
            rows.append({
                "month": m["month"],
                "fx": bank.get("fx"),
                **tx,
            })
    return envelope(rows)
