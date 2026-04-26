"""Performance: TWR, XIRR, drawdown, rolling, Sharpe, Sortino, Calmar, attribution."""
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
    month_labels = [m["month"] for m in months]

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
    twr_total = last.get("cum_twr", 0) or 0
    n = len(period_returns)
    cagr = analytics.cagr_from_cum(twr_total, n)
    episodes = analytics.drawdown_episodes(cum, month_labels)
    return envelope({
        "monthly": rows,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": last.get("xirr"),
        "max_drawdown": analytics.max_drawdown(cum),
        "monthly_volatility": analytics.stdev(period_returns),
        "annualized_volatility": analytics.stdev(period_returns) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(period_returns),
        "sortino_annualized": analytics.sortino(period_returns),
        "calmar": analytics.calmar(period_returns),
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
        "rolling_sharpe_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_sharpe(period_returns, 6))
        ],
    })


@bp.get("/attribution")
def attribution():
    """Per-segment contribution to total return (TW vs Foreign vs FX).

    Uses venue-level trade flows from broker statements as the ground truth
    for what was deployed into each segment, then computes
        segment P&L = ΔMV − net flow into the segment.

    Foreign P&L is decomposed further: FX contribution = previous USD
    holding × ΔFX rate; price contribution = remainder. The previous
    version pulled flows from a key set that didn't exist, so it always
    reported ΔMV (wrong whenever there were buys/sells).
    """
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

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
