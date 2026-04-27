"""Top-level dashboard summary: KPIs, equity curve, allocation snapshot."""
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

bp = Blueprint("summary", __name__, url_prefix="/api/summary")


def _monthly_summary():
    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "kpis": {},
            "twr": 0,
            "xirr": None,
            "profit_twd": 0,
            "invested_twd": 0,
            "equity_curve": [],
            "allocation": {"tw": 0, "foreign": 0, "bank_twd": 0, "bank_usd": 0},
            "first_month": None,
            "last_month": None,
            "months_covered": 0,
        })

    last = months[-1]
    first = months[0]
    kpis = dict(s.kpis)

    # Match /api/performance's default day_weighted method so the two
    # surfaces never disagree on cum_twr / period_return.
    pr_rows = analytics.period_returns(months, method="day_weighted")
    pr_returns = [r["period_return"] for r in pr_rows]
    cum_returns = analytics.cumulative_curve(pr_returns)

    equity_curve = [
        {
            "month": m["month"],
            "equity_twd": m.get("equity_twd", 0),
            "tw_mv": m.get("tw_market_value_twd", 0),
            "foreign_mv": m.get("foreign_market_value_twd", 0),
            "bank_twd": m.get("bank_twd", 0),
            "bank_usd_in_twd": m.get("bank_usd_in_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
            "cum_twr": cum_returns[i] if i < len(cum_returns) else 0,
            "period_return": pr_returns[i] if i < len(pr_returns) else 0,
        }
        for i, m in enumerate(months)
    ]

    cum_twr = cum_returns[-1] if cum_returns else 0
    xirr = last.get("xirr")
    profit_twd = kpis.get("profit_twd", 0)
    invested_twd = kpis.get("counterfactual_twd", 0)

    allocation = {
        "tw": last.get("tw_market_value_twd", 0),
        "foreign": last.get("foreign_market_value_twd", 0),
        "bank_twd": last.get("bank_twd", 0),
        "bank_usd": last.get("bank_usd_in_twd", 0),
    }

    # Bank cash stays monthly — there's no daily bank cash source.
    today_mv, today_upnl, n_repriced = today_repriced_totals(months)
    if today_mv is not None:
        real_now_today = today_mv + bank_cash_twd(last)
        kpis = {
            **kpis,
            "real_now_twd": real_now_today,
            "profit_twd": real_now_today - invested_twd,
            "unrealized_pnl_twd": today_upnl,
            "repriced_holdings_count": n_repriced,
        }
        profit_twd = kpis["profit_twd"]

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


def _daily_summary():
    """Same envelope shape as monthly, but equity_curve has one row per
    trading day pulled from portfolio_daily. KPIs and allocation still
    come from the monthly source-of-truth (PDF) — only the time series
    swaps to daily resolution.

    `cum_twr` on each daily row is monthly-anchored: linear interpolation
    between month-end cum_twr values from the monthly chain, so the
    chart line agrees with the Overview KPI at month-ends and stays flat
    after the latest month-end. Same treatment Performance and
    Benchmarks already use — keeps the headline TWR and the chart line
    consistent on the same page.
    """
    monthly_body = _monthly_summary()
    points = daily_store().get_equity_curve()
    if not points:
        body = monthly_body
        body["data"]["resolution"] = "monthly"
        return body
    monthly_data = monthly_body["data"]

    from .performance import _monthly_anchored_cum
    months = store().months
    monthly_cum = [r.get("cum_twr", 0) for r in monthly_data["equity_curve"]]
    daily_dates = [p["date"] for p in points]
    anchored_cum = _monthly_anchored_cum(daily_dates, months, monthly_cum)

    daily_curve = [
        {
            "date": p["date"],
            "equity_twd": p["equity_twd"],
            "n_positions": p["n_positions"],
            "fx_usd_twd": p["fx_usd_twd"],
            "has_overlay": bool(p["has_overlay"]),
            "cum_twr": anchored_cum[i],
        }
        for i, p in enumerate(points)
    ]
    return envelope({
        **{k: v for k, v in monthly_data.items() if k != "equity_curve"},
        "equity_curve": daily_curve,
        "resolution": "daily",
    })


@bp.get("")
def summary():
    if want_daily():
        return _daily_summary()
    return _monthly_summary()
