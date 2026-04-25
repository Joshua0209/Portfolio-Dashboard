"""Performance: TWR, XIRR, drawdown, rolling, volatility, monthly returns."""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("performance", __name__, url_prefix="/api/performance")


@bp.get("/timeseries")
def timeseries():
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

    period_returns = [m.get("period_return", 0) or 0 for m in months]
    cum = [m.get("cum_twr", 0) or 0 for m in months]
    dd = analytics.drawdown_curve(cum)

    rows = []
    for i, m in enumerate(months):
        rows.append({
            "month": m["month"],
            "period_return": period_returns[i],
            "cum_twr": cum[i],
            "v_start": m.get("v_start", 0),
            "equity_twd": m.get("equity_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
            "drawdown": dd[i]["drawdown"],
            "wealth_index": dd[i]["wealth"],
        })

    last = months[-1]
    return envelope({
        "monthly": rows,
        "twr_total": last.get("cum_twr", 0),
        "xirr": last.get("xirr"),
        "max_drawdown": analytics.max_drawdown(cum),
        "monthly_volatility": analytics.stdev(period_returns),
        "annualized_volatility": analytics.stdev(period_returns) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(period_returns),
        "best_month": max(rows, key=lambda r: r["period_return"]) if rows else None,
        "worst_month": min(rows, key=lambda r: r["period_return"]) if rows else None,
        "positive_months": sum(1 for r in rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in rows if r["period_return"] < 0),
    })


@bp.get("/rolling")
def rolling():
    s = store()
    months = s.months
    period_returns = [m.get("period_return", 0) or 0 for m in months]
    return envelope({
        "rolling_3m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 3))
        ],
        "rolling_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 6))
        ],
        "rolling_12m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 12))
        ],
    })


@bp.get("/attribution")
def attribution():
    """Per-segment contribution to total return (TW vs Foreign)."""
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

    # Approximate segment return: change in market value − net flow into segment,
    # divided by start value of segment. Falls back gracefully.
    out = []
    for i, m in enumerate(months):
        prev = months[i - 1] if i > 0 else None
        tw_mv = m.get("tw_market_value_twd", 0)
        fr_mv = m.get("foreign_market_value_twd", 0)
        prev_tw = prev.get("tw_market_value_twd", 0) if prev else 0
        prev_fr = prev.get("foreign_market_value_twd", 0) if prev else 0

        flows = m.get("investment_flows_twd", {}) or {}
        tw_flow = (flows.get("tw_buy", 0) or 0) - (flows.get("tw_sell", 0) or 0)
        fr_flow = (flows.get("foreign_buy", 0) or 0) - (flows.get("foreign_sell", 0) or 0)

        tw_pnl = tw_mv - prev_tw - tw_flow
        fr_pnl = fr_mv - prev_fr - fr_flow

        out.append({
            "month": m["month"],
            "tw_pnl": tw_pnl,
            "foreign_pnl": fr_pnl,
            "tw_mv_end": tw_mv,
            "foreign_mv_end": fr_mv,
        })
    return envelope(out)
