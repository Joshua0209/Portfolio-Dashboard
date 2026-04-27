"""Benchmark comparison: portfolio TWR vs strategy TWR side-by-side."""
from __future__ import annotations

from flask import Blueprint, current_app, request

from .. import analytics, benchmarks
from ._helpers import envelope, store

bp = Blueprint("benchmarks", __name__, url_prefix="/api/benchmarks")


def _daily_store():
    return current_app.extensions["daily_store"]


@bp.get("/strategies")
def list_strategies():
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


@bp.get("/compare")
def compare():
    """Compare portfolio TWR to one or more strategy TWRs across the
    portfolio's full history.

    Query params:
      keys=<comma list>   strategy keys to include (default: tw_passive,us_passive)
    """
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

    requested = request.args.get("keys") or "tw_passive,us_passive"
    strat_keys = [k.strip() for k in requested.split(",") if k.strip()]

    month_list = [m["month"] for m in months]
    portfolio_returns = [m.get("period_return", 0) or 0 for m in months]
    portfolio_cum = [m.get("cum_twr", 0) or 0 for m in months]

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
        # Filter for stat calcs: only months where we actually have a return
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

    body = {
        "months": month_list,
        "portfolio": {
            "name": "Your portfolio",
            "curve": portfolio_curve,
            "stats": portfolio_stats,
        },
        "strategies": strategies_out,
    }

    # Daily portfolio overlay — strategy curves stay monthly. Mixing
    # resolutions on one chart is fine: monthly strategy renders as
    # step-points; daily portfolio as a continuous line.
    if (request.args.get("resolution") or "").lower() == "daily":
        equity_series = _daily_store().get_equity_curve()
        if equity_series:
            flow_series = analytics.daily_external_flows(s.months)
            daily_rows = analytics.daily_twr(equity_series, flow_series)
            body["portfolio_daily_curve"] = [
                {"date": r["date"], "cum_return": r["cum_twr"]}
                for r in daily_rows
            ]
            body["resolution"] = "daily"

    return envelope(body)
