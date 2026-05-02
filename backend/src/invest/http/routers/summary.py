"""GET /api/summary — top-level dashboard payload (KPIs + equity curve + allocation).

Phase 6.5 wiring: ported from legacy app/api/summary.py with re-pointed
imports. Reads PortfolioStore for the monthly base, DailyStore for the
daily-resolution branch and today-repriced KPIs. Returns the same
envelope shape the legacy frontend already consumes.

Branch on ?resolution=daily:
  monthly  KPIs + equity_curve from months[]
  daily    Same KPIs, equity_curve swapped for per-day rows from
           portfolio_daily, with cum_twr linearly interpolated through
           monthly anchors so the chart line lands exactly on the KPI
           value at month-ends.

Empty-state branch (months == []) returns the legacy empty envelope so
the frontend's chart-empty branches still fire on a fresh checkout.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import bank_cash_twd, envelope, today_repriced_totals
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


def _monthly_summary(s: PortfolioStore, daily: DailyStore) -> dict[str, Any]:
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

    today_mv, today_upnl, n_repriced = today_repriced_totals(months, s, daily)
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


def _daily_summary(s: PortfolioStore, daily: DailyStore) -> dict[str, Any]:
    """Daily-resolution branch: same envelope shape, equity_curve swapped
    for per-day rows. KPIs and allocation stay monthly (no daily source).

    cum_twr on each daily row uses monthly_anchored_cum so the line
    agrees with the headline TWR at month-ends and stays flat through
    the partial current month.
    """
    monthly_body = _monthly_summary(s, daily)
    points = daily.get_equity_curve()
    if not points:
        body = monthly_body
        body["data"]["resolution"] = "monthly"
        return body
    monthly_data = monthly_body["data"]

    months = s.months
    monthly_cum = [r.get("cum_twr", 0) for r in monthly_data["equity_curve"]]
    daily_dates = [p["date"] for p in points]
    anchored_cum = analytics.monthly_anchored_cum(daily_dates, months, monthly_cum)

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


@router.get("/api/summary")
def summary(
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    if (resolution or "").lower() == "daily":
        return _daily_summary(s, daily)
    return _monthly_summary(s, daily)
