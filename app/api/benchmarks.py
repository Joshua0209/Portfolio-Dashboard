"""Benchmark comparison: portfolio TWR vs strategy TWR side-by-side."""
from __future__ import annotations

from flask import Blueprint, request

from .. import analytics, benchmarks
from ._helpers import daily_store, envelope, store, want_daily

bp = Blueprint("benchmarks", __name__, url_prefix="/api/benchmarks")


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
    # Recompute via day_weighted so this matches /api/summary's TWR. The
    # parser-stored period_return/cum_twr fields use the legacy mid_month
    # method (~+193% over the demo period vs +152% under day_weighted) and
    # would silently disagree with the Overview KPI.
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

    # Daily portfolio overlay + daily strategy curves. Both render at
    # daily precision when the daily store has rows.
    if want_daily():
        equity_series = daily_store().get_equity_curve()
        if equity_series:
            # Anchor the daily portfolio overlay to the monthly chain's
            # month-end values via linear interpolation, matching the
            # Performance page's chart treatment. The line lands exactly
            # on portfolio.stats.twr_total at the last month-end, so a
            # head-to-head against the strategy curves stays apples-to-
            # apples (strategies still run their own daily Mod Dietz, but
            # the comparison floor — your TWR — agrees with Overview).
            from app.api.performance import _monthly_anchored_cum
            daily_dates = [r["date"] for r in equity_series]
            anchored_cum = _monthly_anchored_cum(daily_dates, months, portfolio_cum)
            body["portfolio_daily_curve"] = [
                {"date": d, "cum_return": c}
                for d, c in zip(daily_dates, anchored_cum)
            ]
            body["resolution"] = "daily"

            # Daily strategy curves over the same date window.
            for s_out in body["strategies"]:
                strat = benchmarks.get_strategy(s_out["key"])
                if not strat:
                    continue
                rows_d = benchmarks.strategy_daily_returns(
                    strat, daily_dates, daily_store()
                )
                s_out["curve_daily"] = rows_d

    return envelope(body)


def _anchor_for_daily(
    months: list[dict], monthly_cum: list[float], first_daily_date: str
) -> float:
    """Last monthly cum_twr strictly before first_daily_date, else 0.

    See performance._anchor_for_daily — duplicated here to avoid a cross-
    blueprint import. Both call sites need the same anchor semantics so
    the Performance and Benchmark daily curves agree at every point.
    """
    first_ym = first_daily_date[:7]
    anchor = 0.0
    for i, m in enumerate(months):
        if m["month"] >= first_ym:
            break
        if i < len(monthly_cum):
            anchor = monthly_cum[i]
    return anchor
