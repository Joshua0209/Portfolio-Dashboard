"""Cashflow audit trail: real vs counterfactual."""
from __future__ import annotations

from flask import Blueprint, request

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("cashflows", __name__, url_prefix="/api/cashflows")


@bp.get("/monthly")
def monthly():
    """Monthly waterfall (always returned). When ?resolution=daily is set
    the response also carries a `daily` array — daily and monthly always
    coexist on this endpoint per the design spec ("keep monthly chart").
    """
    s = store()
    body = analytics.monthly_flows(s.months, s.venue_flows_twd)
    if (request.args.get("resolution") or "").lower() == "daily":
        daily = analytics.daily_external_flows(s.months)
        # Cumulative running sum so the waterfall renders.
        running = 0.0
        daily_cum = []
        for r in daily:
            running += float(r["flow_twd"])
            daily_cum.append({**r, "cumulative_twd": running})
        if isinstance(body, dict):
            body["daily"] = daily_cum
            body["resolution"] = "daily"
        else:
            body = {"resolution": "daily", "monthly": body, "daily": daily_cum}
    return envelope(body)


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
    """Bank account ledger — capital source view.

    Each row carries:
      * signed_amount  — direction-aware (+ in / − out)
      * amount_twd     — TWD-equivalent of signed_amount (uses month FX)
      * account        — "TWD" or "FOREIGN"
    """
    s = store()
    rows = []
    for m in s.months:
        bank = m.get("bank", {}) or {}
        fx = bank.get("fx") or {}
        usd_rate = fx.get("USD") or m.get("fx_usd_twd") or 0.0

        for tx in bank.get("tx_twd", []) or []:
            signed = tx.get("signed_amount", 0) or 0
            rows.append({
                **tx,
                "month": m["month"],
                "ccy": "TWD",
                "account": "TWD",
                "fx": fx,
                "amount_twd": signed,
                "signed_amount": signed,
            })
        for tx in bank.get("tx_foreign", []) or []:
            ccy = tx.get("ccy") or "USD"
            signed = tx.get("signed_amount", 0) or 0
            rate = fx.get(ccy) or (usd_rate if ccy == "USD" else 0.0)
            rows.append({
                **tx,
                "month": m["month"],
                "account": "FOREIGN",
                "fx": fx,
                "amount_twd": signed * rate,
                "signed_amount": signed,
            })
    return envelope(rows)
