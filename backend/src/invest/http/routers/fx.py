"""GET /api/fx — FX exposure + P&L attribution."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends, Query

from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/fx")
def fx(
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    use_daily = (resolution or "").lower() == "daily"
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

    daily_rate_curve = None
    daily_fx_pnl = None
    if use_daily:
        fx_series = daily.get_fx_series(ccy="USD")
        if fx_series:
            daily_rate_curve = [
                {"date": r["date"], "fx_usd_twd": r["rate_to_twd"]}
                for r in fx_series
            ]
            usd_exposure = daily.get_usd_exposure_series()
            daily_fx_pnl = analytics.daily_fx_pnl(usd_exposure, fx_series)

    rate_curve = (
        daily_rate_curve
        if daily_rate_curve is not None
        else [
            {"month": m["month"], "fx_usd_twd": m.get("fx_usd_twd")}
            for m in months
        ]
    )

    by_ccy_exposure = []
    for m in months:
        ccy_mv: dict[str, float] = defaultdict(float)
        ccy_mv["TWD"] += m.get("tw_market_value_twd", 0) or 0
        ccy_mv["TWD"] += m.get("bank_twd", 0) or 0
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

    body: dict[str, Any] = {
        "resolution": "daily" if daily_rate_curve is not None else "monthly",
        "rate_curve": rate_curve,
        "current_rate": (
            daily_rate_curve[-1]["fx_usd_twd"]
            if daily_rate_curve else last.get("fx_usd_twd")
        ),
        "first_rate": (
            daily_rate_curve[0]["fx_usd_twd"]
            if daily_rate_curve else months[0].get("fx_usd_twd")
        ),
        "by_ccy_monthly": by_ccy_exposure,
        "fx_pnl": fx_pnl,
        "foreign_share": foreign_share,
        "foreign_value_twd": foreign_mv + bank_usd_in_twd,
    }
    if daily_fx_pnl is not None:
        body["fx_pnl_daily"] = daily_fx_pnl
    return envelope(body)
