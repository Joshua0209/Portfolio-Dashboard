"""GET /api/cashflows/{monthly,cumulative,bank}."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import bank_cash_twd, envelope, today_repriced_totals
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/cashflows/monthly")
def monthly(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
    return envelope(analytics.monthly_flows(s.months, s.venue_flows_twd))


def _daily_real_vs_counterfactual(months, daily: DailyStore):
    if not months:
        return None, None
    equity_series = daily.get_equity_curve()
    if not equity_series:
        return None, None

    bank_by_month = {m["month"]: bank_cash_twd(m) for m in months}
    sorted_months = sorted(bank_by_month.keys())

    def bank_cash_for(date_iso: str) -> float:
        ym = date_iso[:7]
        if ym in bank_by_month:
            return bank_by_month[ym]
        prior = [m for m in sorted_months if m <= ym]
        if not prior:
            return 0.0
        return bank_by_month[prior[-1]]

    real_curve = [
        {"date": p["date"], "value": float(p["equity_twd"]) + bank_cash_for(p["date"])}
        for p in equity_series
    ]

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


@router.get("/api/cashflows/cumulative")
def cumulative(
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    flows = s.cumulative_flows or {}
    kpis = s.kpis or {}
    months = s.months

    counterfactual_curve = []
    real_curve = []
    cum_external = 0.0
    for m in months:
        cum_external += m.get("external_flow_twd", 0) or 0
        counterfactual_curve.append({"month": m["month"], "value": cum_external})
        real_curve.append({
            "month": m["month"],
            "value": (m.get("equity_twd", 0) or 0) + bank_cash_twd(m),
            "external_flow": m.get("external_flow_twd", 0),
        })

    real_now = kpis.get("real_now_twd", 0)
    profit_now = kpis.get("profit_twd", 0)
    invested = kpis.get("counterfactual_twd", 0)
    today_mv, _, _ = today_repriced_totals(months, s, daily)
    if today_mv is not None and months:
        real_now = today_mv + bank_cash_twd(months[-1])
        profit_now = real_now - invested

    body: dict[str, Any] = {
        "cumulative_flows": flows,
        "real_now_twd": real_now,
        "counterfactual_twd": invested,
        "profit_twd": profit_now,
        "real_curve": real_curve,
        "counterfactual_curve": counterfactual_curve,
    }

    if (resolution or "").lower() == "daily":
        daily_real, daily_cf = _daily_real_vs_counterfactual(months, daily)
        if daily_real is not None:
            body["resolution"] = "daily"
            body["real_curve_daily"] = daily_real
            body["counterfactual_curve_daily"] = daily_cf

    return envelope(body)


@router.get("/api/cashflows/bank")
def bank_transactions(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
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
