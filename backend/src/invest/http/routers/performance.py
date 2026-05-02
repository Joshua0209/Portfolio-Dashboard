"""GET /api/performance/{timeseries,rolling,attribution}.

Phase 6.5 wiring: full port of legacy app/api/performance.py.

  /timeseries   per-month TWR + drawdown; ?resolution=daily swaps to
                per-day rows from portfolio_daily; ?method= picks
                day_weighted | mid_month | eom flow-timing rule
  /rolling      3/6/12-month rolling TWR + 6-month rolling Sharpe;
                ?resolution=daily swaps to 30/60/90-day rolling
  /attribution  per-month TW vs Foreign vs FX P&L decomposition

Empty-state envelope preserved.
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query

from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import bank_cash_twd, envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


Method = Literal["day_weighted", "mid_month", "eom"]


def _recomputed(months: list[dict], method: str) -> tuple[list[float], list[float], list[dict]]:
    rows = analytics.period_returns(months, method=method)  # type: ignore[arg-type]
    pr = [r["period_return"] for r in rows]
    cum = analytics.cumulative_curve(pr)
    return pr, cum, rows


def _daily_timeseries(s: PortfolioStore, daily: DailyStore, method: str) -> dict | None:
    months = s.months
    equity_series = daily.get_equity_curve()
    if not equity_series or not months:
        return None

    monthly_returns, monthly_cum, _meta = _recomputed(months, method)
    month_labels = [m["month"] for m in months]

    positions_only_series = [
        {**r, "equity_twd": float(r["equity_twd"]) - float(r.get("cash_twd") or 0)}
        for r in equity_series
    ]
    flow_series = analytics.daily_investment_flows(s.months)
    anchor = analytics.anchor_for_daily(months, monthly_cum, equity_series[0]["date"])
    rows = analytics.daily_twr(
        positions_only_series, flow_series, anchor_cum_return=anchor
    )

    daily_dates = [r["date"] for r in rows]
    anchored_cum = analytics.monthly_anchored_cum(daily_dates, months, monthly_cum)
    actual_daily_cum = [r["cum_twr"] for r in rows]
    dd_curve = analytics.drawdown_curve(actual_daily_cum)

    bank_for = analytics.bank_cash_forward_fill(months)
    out_rows = [
        {
            "date": rows[i]["date"],
            "period_return": rows[i]["period_return"],
            "cum_twr": anchored_cum[i],
            "equity_twd": rows[i]["equity_twd"] + bank_for(rows[i]["date"]),
            "flow_twd": rows[i]["flow_twd"],
            "drawdown": dd_curve[i]["drawdown"],
            "wealth_index": dd_curve[i]["wealth"],
        }
        for i in range(len(rows))
    ]

    twr_total = monthly_cum[-1] if monthly_cum else 0
    cagr = analytics.cagr_from_cum(twr_total, len(monthly_returns))
    max_dd = analytics.max_drawdown(monthly_cum)

    return {
        "resolution": "daily",
        "monthly": out_rows,
        "method": method,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": months[-1].get("xirr"),
        "max_drawdown": max_dd,
        "monthly_volatility": analytics.stdev(monthly_returns),
        "annualized_volatility": analytics.stdev(monthly_returns) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(monthly_returns),
        "sortino_annualized": analytics.sortino(monthly_returns),
        "calmar": analytics.calmar(monthly_returns),
        "best_month": max(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "worst_month": min(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "positive_months": sum(1 for r in out_rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in out_rows if r["period_return"] < 0),
        "hit_rate": (
            sum(1 for r in out_rows if r["period_return"] > 0)
            / max(1, sum(1 for r in out_rows if r["period_return"] != 0))
        ),
        "drawdown_episodes": analytics.drawdown_episodes(monthly_cum, month_labels),
    }


@router.get("/api/performance/timeseries")
def timeseries(
    method: Method = Query(default="day_weighted"),
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    if (resolution or "").lower() == "daily":
        d = _daily_timeseries(s, daily, method)
        if d is not None:
            return envelope(d)

    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "monthly": [],
            "method": method,
            "twr_total": 0,
            "cagr": 0,
            "xirr": None,
            "max_drawdown": 0,
            "monthly_volatility": 0,
            "annualized_volatility": 0,
            "sharpe_annualized": 0,
            "sortino_annualized": 0,
            "calmar": 0,
            "best_month": None,
            "worst_month": None,
            "positive_months": 0,
            "negative_months": 0,
            "hit_rate": 0,
            "drawdown_episodes": [],
        })

    pr, cum, meta = _recomputed(months, method)
    dd = analytics.drawdown_curve(cum)
    month_labels = [m["month"] for m in months]

    rows = []
    for i, m in enumerate(months):
        rows.append({
            "month": m["month"],
            "period_return": pr[i],
            "cum_twr": cum[i],
            "v_start": meta[i]["v_start"],
            "equity_twd": (m.get("equity_twd", 0) or 0) + bank_cash_twd(m),
            "external_flow": m.get("external_flow_twd", 0),
            "weighted_flow": meta[i]["weighted_flow"],
            "days_in_month": meta[i]["days_in_month"],
            "drawdown": dd[i]["drawdown"],
            "wealth_index": dd[i]["wealth"],
        })

    twr_total = cum[-1] if cum else 0
    cagr = analytics.cagr_from_cum(twr_total, len(pr))
    episodes = analytics.drawdown_episodes(cum, month_labels)
    xirr = months[-1].get("xirr")

    return envelope({
        "monthly": rows,
        "method": method,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": xirr,
        "max_drawdown": analytics.max_drawdown(cum),
        "monthly_volatility": analytics.stdev(pr),
        "annualized_volatility": analytics.stdev(pr) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(pr),
        "sortino_annualized": analytics.sortino(pr),
        "calmar": analytics.calmar(pr),
        "best_month": max(rows, key=lambda r: r["period_return"]) if rows else None,
        "worst_month": min(rows, key=lambda r: r["period_return"]) if rows else None,
        "positive_months": sum(1 for r in rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in rows if r["period_return"] < 0),
        "hit_rate": (
            sum(1 for r in rows if r["period_return"] > 0) /
            max(1, sum(1 for r in rows if r["period_return"] != 0))
        ),
        "drawdown_episodes": episodes,
    })


@router.get("/api/performance/rolling")
def rolling(
    method: Method = Query(default="day_weighted"),
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    if (resolution or "").lower() == "daily":
        equity_series = daily.get_equity_curve()
        if equity_series:
            positions_only_series = [
                {**r, "equity_twd": float(r["equity_twd"]) - float(r.get("cash_twd") or 0)}
                for r in equity_series
            ]
            flow_series = analytics.daily_investment_flows(s.months)
            rows = analytics.daily_twr(positions_only_series, flow_series)
            pr = [r["period_return"] for r in rows]
            dates = [r["date"] for r in rows]
            return envelope({
                "resolution": "daily",
                "rolling_30d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 30))
                ],
                "rolling_60d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 60))
                ],
                "rolling_90d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 90))
                ],
                "rolling_sharpe_60d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_sharpe(pr, 60))
                ],
            })

    months = s.months
    pr, _, _ = _recomputed(months, method)
    return envelope({
        "rolling_3m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(pr, 3))
        ],
        "rolling_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(pr, 6))
        ],
        "rolling_12m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(pr, 12))
        ],
        "rolling_sharpe_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_sharpe(pr, 6))
        ],
    })


@router.get("/api/performance/attribution")
def attribution(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
    """Per-segment contribution to total return (TW vs Foreign vs FX)."""
    months = s.months
    if not months:
        return envelope([])

    venue_flows = {v["month"]: v for v in s.venue_flows_twd}

    out = []
    cum_tw = cum_fr = cum_fx = 0.0
    for i, m in enumerate(months):
        prev = months[i - 1] if i > 0 else None
        tw_mv = m.get("tw_market_value_twd", 0) or 0
        fr_mv = m.get("foreign_market_value_twd", 0) or 0
        prev_tw = (prev.get("tw_market_value_twd", 0) or 0) if prev else 0
        prev_fr = (prev.get("foreign_market_value_twd", 0) or 0) if prev else 0

        vf = venue_flows.get(m["month"], {})
        tw_net_flow = (vf.get("tw_buy_twd", 0) or 0) - (vf.get("tw_sell_twd", 0) or 0)
        fr_net_flow = (vf.get("foreign_buy_twd", 0) or 0) - (vf.get("foreign_sell_twd", 0) or 0)

        tw_pnl = tw_mv - prev_tw - tw_net_flow
        fr_pnl_total = fr_mv - prev_fr - fr_net_flow

        prev_fx = (prev.get("fx_usd_twd") or 1) if prev else 1
        curr_fx = m.get("fx_usd_twd") or prev_fx
        prev_usd = (prev_fr / prev_fx) if (prev and prev_fx) else 0
        fx_component = prev_usd * (curr_fx - prev_fx)
        fr_price_pnl = fr_pnl_total - fx_component

        cum_tw += tw_pnl
        cum_fr += fr_price_pnl
        cum_fx += fx_component

        out.append({
            "month": m["month"],
            "tw_pnl": tw_pnl,
            "tw_net_flow": tw_net_flow,
            "tw_mv_end": tw_mv,
            "foreign_pnl_price": fr_price_pnl,
            "foreign_pnl_fx": fx_component,
            "foreign_pnl_total": fr_pnl_total,
            "foreign_net_flow": fr_net_flow,
            "foreign_mv_end": fr_mv,
            "cum_tw_pnl": cum_tw,
            "cum_foreign_price_pnl": cum_fr,
            "cum_fx_pnl": cum_fx,
        })

    return envelope({
        "monthly": out,
        "totals": {
            "tw_pnl_twd": cum_tw,
            "foreign_price_pnl_twd": cum_fr,
            "fx_pnl_twd": cum_fx,
            "total_pnl_twd": cum_tw + cum_fr + cum_fx,
        },
    })
