"""Performance: TWR, XIRR, drawdown, rolling, Sharpe, Sortino, Calmar, attribution."""
from __future__ import annotations

from flask import Blueprint, current_app, request

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("performance", __name__, url_prefix="/api/performance")


def _daily_store():
    return current_app.extensions["daily_store"]


def _daily_timeseries() -> dict:
    """Daily Modified Dietz timeseries — falls back to monthly envelope
    silently when the daily layer is empty (cold start / pre-backfill).
    Same key set as the monthly response so frontend can read either.
    """
    s = store()
    equity_series = _daily_store().get_equity_curve()
    if not equity_series:
        return None  # caller falls back to monthly

    flow_series = analytics.daily_external_flows(s.months)
    rows = analytics.daily_twr(equity_series, flow_series)
    period_returns = [r["period_return"] for r in rows]
    cum = [r["cum_twr"] for r in rows]
    dates = [r["date"] for r in rows]
    twr_total = cum[-1] if cum else 0
    dd_curve = analytics.drawdown_curve(cum)

    # Re-shape rows to match the monthly response keys frontend expects.
    out_rows = [
        {
            "date": rows[i]["date"],
            "period_return": rows[i]["period_return"],
            "cum_twr": rows[i]["cum_twr"],
            "equity_twd": rows[i]["equity_twd"],
            "flow_twd": rows[i]["flow_twd"],
            "drawdown": dd_curve[i]["drawdown"],
            "wealth_index": dd_curve[i]["wealth"],
        }
        for i in range(len(rows))
    ]

    return {
        "resolution": "daily",
        "monthly": out_rows,  # key kept as 'monthly' for frontend compat
        "method": "daily_modified_dietz",
        "twr_total": twr_total,
        "max_drawdown": analytics.max_drawdown(cum),
        "monthly_volatility": analytics.stdev(period_returns),
        # daily volatility annualizes by sqrt(252), not sqrt(12)
        "annualized_volatility": analytics.stdev(period_returns) * (252 ** 0.5),
        "sharpe_annualized": analytics.sharpe(period_returns) * ((252 / 12) ** 0.5),
        "sortino_annualized": analytics.sortino(period_returns) * ((252 / 12) ** 0.5),
        "best_month": max(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "worst_month": min(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "positive_months": sum(1 for r in out_rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in out_rows if r["period_return"] < 0),
        "hit_rate": (
            sum(1 for r in out_rows if r["period_return"] > 0)
            / max(1, sum(1 for r in out_rows if r["period_return"] != 0))
        ),
        "drawdown_episodes": analytics.drawdown_episodes(cum, dates),
    }


_VALID_METHODS = {"day_weighted", "mid_month", "eom"}


def _method_param() -> str:
    """Read ?method= from request, defaulting to day_weighted."""
    m = (request.args.get("method") or "day_weighted").lower()
    return m if m in _VALID_METHODS else "day_weighted"


def _recomputed(months: list[dict], method: str) -> tuple[list[float], list[float], list[dict]]:
    """Return (period_returns, cum_twr, per_month_meta) under chosen method."""
    rows = analytics.period_returns(months, method=method)  # type: ignore[arg-type]
    pr = [r["period_return"] for r in rows]
    cum = analytics.cumulative_curve(pr)
    return pr, cum, rows


@bp.get("/timeseries")
def timeseries():
    if (request.args.get("resolution") or "").lower() == "daily":
        daily = _daily_timeseries()
        if daily is not None:
            return envelope(daily)
        # Fall through to monthly when daily layer is empty.

    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "monthly": [],
            "twr_total": 0,
            "xirr": None,
            "max_drawdown": 0,
            "monthly_volatility": 0,
            "annualized_volatility": 0,
            "sharpe_annualized": 0,
            "best_month": None,
            "worst_month": None,
            "positive_months": 0,
            "negative_months": 0,
        })

    method = _method_param()
    period_returns, cum, meta = _recomputed(months, method)
    dd = analytics.drawdown_curve(cum)
    month_labels = [m["month"] for m in months]

    rows = []
    for i, m in enumerate(months):
        rows.append({
            "month": m["month"],
            "period_return": period_returns[i],
            "cum_twr": cum[i],
            "v_start": meta[i]["v_start"],
            "equity_twd": m.get("equity_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
            "weighted_flow": meta[i]["weighted_flow"],
            "days_in_month": meta[i]["days_in_month"],
            "drawdown": dd[i]["drawdown"],
            "wealth_index": dd[i]["wealth"],
        })

    twr_total = cum[-1] if cum else 0
    n = len(period_returns)
    cagr = analytics.cagr_from_cum(twr_total, n)
    episodes = analytics.drawdown_episodes(cum, month_labels)

    # XIRR is independent of period_return method (it works on absolute
    # cashflows, not %-returns). Re-use the parser-stored value if we kept
    # the legacy method, else fall back to None for now.
    xirr = months[-1].get("xirr") if method == "mid_month" else months[-1].get("xirr")

    return envelope({
        "monthly": rows,
        "method": method,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": xirr,
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
    if (request.args.get("resolution") or "").lower() == "daily":
        equity_series = _daily_store().get_equity_curve()
        if equity_series:
            s = store()
            flow_series = analytics.daily_external_flows(s.months)
            rows = analytics.daily_twr(equity_series, flow_series)
            pr = [r["period_return"] for r in rows]
            dates = [r["date"] for r in rows]
            # Daily rolling windows (~30/60/90 trading days ≈ 1/3/6 months).
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
        # Empty daily layer → fall through to monthly.

    s = store()
    months = s.months
    method = _method_param()
    period_returns, _, _ = _recomputed(months, method)
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
