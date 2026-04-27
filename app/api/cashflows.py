"""Cashflow audit trail: real vs counterfactual."""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from ._helpers import (
    bank_cash_twd,
    daily_store,
    envelope,
    store,
    today_repriced_totals,
    want_daily,
)

bp = Blueprint("cashflows", __name__, url_prefix="/api/cashflows")


@bp.get("/monthly")
def monthly():
    """Monthly waterfall — always returned in its original list shape."""
    s = store()
    return envelope(analytics.monthly_flows(s.months, s.venue_flows_twd))


def _daily_real_vs_counterfactual(months):
    """Per-day (real_curve, counterfactual_curve) pair.

    real_curve[d]      = portfolio_daily.equity_twd[d] + bank_cash(month_of_d)
    counterfactual[d]  = cumulative external flow up to and including d

    Bank cash is monthly-only; we forward-fill from the prior month-end
    so the line is continuous on weekdays after backfill. Returns
    (None, None) when the daily store is empty.
    """
    if not months:
        return None, None
    daily = daily_store()
    equity_series = daily.get_equity_curve()
    if not equity_series:
        return None, None

    bank_by_month = {m["month"]: bank_cash_twd(m) for m in months}
    sorted_months = sorted(bank_by_month.keys())

    def bank_cash_for(date_iso: str) -> float:
        ym = date_iso[:7]
        if ym in bank_by_month:
            return bank_by_month[ym]
        # Forward-fill from the most recent month-end on or before the date.
        prior = [m for m in sorted_months if m <= ym]
        if not prior:
            return 0.0
        return bank_by_month[prior[-1]]

    real_curve = [
        {
            "date": p["date"],
            "value": float(p["equity_twd"]) + bank_cash_for(p["date"]),
        }
        for p in equity_series
    ]

    # Counterfactual = sum of every external flow whose date ≤ priced
    # day d. The two-pointer merge below handles the common case where
    # the first salary deposit precedes the first priced trading day:
    # those pre-window flows must roll into the very first counterfactual
    # value so the curve doesn't start at 0.
    flow_series = sorted(
        analytics.daily_external_flows(months),
        key=lambda f: f["date"],
    )
    counterfactual_curve = []
    cum_external = 0.0
    j = 0
    for p in equity_series:
        d = p["date"]
        while j < len(flow_series) and flow_series[j]["date"] <= d:
            cum_external += float(flow_series[j]["flow_twd"])
            j += 1
        counterfactual_curve.append({"date": d, "value": cum_external})
    return real_curve, counterfactual_curve


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
            # Match the Real-now KPI: stocks MV + bank cash (TWD + USD).
            # Stocks-only here would visually contradict the kpi-real card.
            "value": (m.get("equity_twd", 0) or 0) + bank_cash_twd(m),
            "external_flow": m.get("external_flow_twd", 0),
        })

    # Reprice the hero against today's close so the Cashflows page
    # matches the Overview page when the daily layer has data.
    real_now = kpis.get("real_now_twd", 0)
    profit_now = kpis.get("profit_twd", 0)
    invested = kpis.get("counterfactual_twd", 0)
    today_mv, _, _ = today_repriced_totals(months)
    if today_mv is not None and months:
        real_now = today_mv + bank_cash_twd(months[-1])
        profit_now = real_now - invested

    body = {
        "cumulative_flows": flows,
        "real_now_twd": real_now,
        "counterfactual_twd": invested,
        "profit_twd": profit_now,
        "real_curve": real_curve,
        "counterfactual_curve": counterfactual_curve,
    }

    if want_daily():
        daily_real, daily_cf = _daily_real_vs_counterfactual(months)
        if daily_real is not None:
            body["resolution"] = "daily"
            body["real_curve_daily"] = daily_real
            body["counterfactual_curve_daily"] = daily_cf

    return envelope(body)


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
