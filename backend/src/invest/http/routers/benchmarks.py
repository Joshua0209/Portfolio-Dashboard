"""GET /api/benchmarks/{strategies,compare}."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from invest import benchmarks
from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/benchmarks/strategies")
def list_strategies() -> dict[str, Any]:
    return envelope([
        {
            "key": s.key,
            "name": s.name,
            "market": s.market,
            "weights": s.weights,
            "description": s.description,
        }
        for s in benchmarks.STRATEGIES
    ])


@router.get("/api/benchmarks/compare")
def compare(
    keys: str = Query(default="tw_passive,us_passive"),
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    months = s.months
    if not months:
        return envelope({"empty": True})

    strat_keys = [k.strip() for k in (keys or "").split(",") if k.strip()]

    month_list = [m["month"] for m in months]
    pr_rows = analytics.period_returns(months, method="day_weighted")
    portfolio_returns = [r["period_return"] for r in pr_rows]
    portfolio_cum = analytics.cumulative_curve(portfolio_returns)

    portfolio_curve = [
        {"month": m, "period_return": pr, "cum_return": cr}
        for m, pr, cr in zip(month_list, portfolio_returns, portfolio_cum)
    ]

    strategies_out = []
    for key in strat_keys:
        strat = benchmarks.get_strategy(key)
        if not strat:
            continue
        rows = benchmarks.strategy_monthly_returns(strat, month_list)
        valid_returns = [r["period_return"] for r in rows if r["period_return"] is not None]

        cum_total = rows[-1]["cum_return"] if rows else 0.0
        max_dd = analytics.max_drawdown([r.get("cum_return") or 0 for r in rows])
        vol = analytics.stdev(valid_returns) * (12 ** 0.5) if valid_returns else 0.0
        sharpe_v = analytics.sharpe(valid_returns) if valid_returns else 0.0
        sortino_v = analytics.sortino(valid_returns) if valid_returns else 0.0

        strategies_out.append({
            "key": strat.key,
            "name": strat.name,
            "market": strat.market,
            "description": strat.description,
            "weights": strat.weights,
            "curve": rows,
            "stats": {
                "twr_total": cum_total,
                "annualized_volatility": vol,
                "max_drawdown": max_dd,
                "sharpe": sharpe_v,
                "sortino": sortino_v,
            },
        })

    portfolio_stats = {
        "twr_total": portfolio_cum[-1] if portfolio_cum else 0,
        "annualized_volatility": analytics.stdev(portfolio_returns) * (12 ** 0.5),
        "max_drawdown": analytics.max_drawdown(portfolio_cum),
        "sharpe": analytics.sharpe(portfolio_returns),
        "sortino": analytics.sortino(portfolio_returns),
    }

    body: dict[str, Any] = {
        "months": month_list,
        "portfolio": {
            "name": "Your portfolio",
            "curve": portfolio_curve,
            "stats": portfolio_stats,
        },
        "strategies": strategies_out,
    }

    if (resolution or "").lower() == "daily":
        equity_series = daily.get_equity_curve()
        if equity_series:
            daily_dates = [r["date"] for r in equity_series]
            anchored_cum = analytics.monthly_anchored_cum(daily_dates, months, portfolio_cum)
            body["portfolio_daily_curve"] = [
                {"date": d, "cum_return": c}
                for d, c in zip(daily_dates, anchored_cum)
            ]
            body["resolution"] = "daily"

            for s_out in body["strategies"]:
                strat = benchmarks.get_strategy(s_out["key"])
                if not strat:
                    continue
                rows_d = benchmarks.strategy_daily_returns(
                    strat, daily_dates, daily,
                )
                s_out["curve_daily"] = rows_d

    return envelope(body)
