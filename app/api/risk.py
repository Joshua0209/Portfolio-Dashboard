"""Risk metrics: drawdown, volatility, concentration."""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from ..holdings_today import current_holdings
from ._helpers import daily_store, envelope, store, want_daily

bp = Blueprint("risk", __name__, url_prefix="/api/risk")


def _daily_drawdown_curve():
    """Drawdown curve keyed by `date` instead of `month`. Returns
    (None, None) when the daily layer is empty so the caller can fall
    back to monthly.
    """
    rows = daily_store().get_drawdown_series()
    if not rows:
        return None, None
    curve = [
        {
            "date": r["date"],
            "wealth": (r["equity_twd"] / r["peak_twd"]) if r["peak_twd"] else 1.0,
            "drawdown": r["drawdown_pct"],
        }
        for r in rows
    ]
    current_dd = curve[-1]["drawdown"] if curve else 0
    return curve, current_dd


@bp.get("")
def risk():
    use_daily = want_daily()
    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "monthly_volatility": 0,
            "annualized_volatility": 0,
            "max_drawdown": 0,
            "current_drawdown": 0,
            "drawdown_curve": [],
            "sharpe_annualized": 0,
            "hhi": 0,
            "diversification_score": 0,
            "top_5_share": 0,
            "top_10_share": 0,
            "position_count": 0,
            "leverage_value_twd": 0,
            "leverage_pct": 0,
            "weight_distribution": [],
        })

    period_returns = [m.get("period_return", 0) or 0 for m in months]
    cum = [m.get("cum_twr", 0) or 0 for m in months]
    dd_curve = analytics.drawdown_curve(cum)
    month_labels = [m["month"] for m in months]

    holdings = current_holdings(s, daily_store())
    total = sum(h["mkt_value_twd"] for h in holdings) or 1
    weights = [h["mkt_value_twd"] / total for h in holdings]

    leverage_value = sum(h["mkt_value_twd"] for h in holdings if h.get("type") == "融資")
    leverage_pct = leverage_value / total if total else 0

    # Per-month leverage timeline: market value of 融資 positions / equity.
    # Lets you spot when borrowed exposure crept up.
    leverage_timeline = []
    for m in months:
        tw_holdings = (m.get("tw") or {}).get("holdings", []) or []
        margin_mv = sum((h.get("mkt_value", 0) or 0) for h in tw_holdings if h.get("type") == "融資")
        equity = m.get("equity_twd", 0) or 0
        leverage_timeline.append({
            "month": m["month"],
            "margin_mv_twd": margin_mv,
            "equity_twd": equity,
            "leverage_pct": (margin_mv / equity) if equity else 0,
        })

    top_5_share = analytics.top_n_share(weights, 5)
    top_10_share = analytics.top_n_share(weights, 10)
    hhi_value = analytics.hhi(weights)

    # Daily branch: replace drawdown_curve + current_drawdown + max_drawdown
    # with values sourced from portfolio_daily. Volatility / Sharpe / Sortino
    # / HHI / leverage stay monthly — those metrics need monthly returns or
    # snapshot weights and don't get more accurate at daily resolution.
    daily_dd_curve, daily_current_dd = (None, None)
    daily_max_dd = None
    daily_episodes = None
    if use_daily:
        daily_dd_curve, daily_current_dd = _daily_drawdown_curve()
        if daily_dd_curve:
            daily_max_dd = min((p["drawdown"] for p in daily_dd_curve), default=0)
            daily_dates = [p["date"] for p in daily_dd_curve]
            # Build a synthetic cum-return series from drawdown wealth_index
            # so drawdown_episodes() picks up daily-resolution episodes.
            cum_daily = [p["wealth"] - 1.0 for p in daily_dd_curve]
            daily_episodes = analytics.drawdown_episodes(cum_daily, daily_dates)

    drawdown_curve_out = (
        daily_dd_curve
        if daily_dd_curve is not None
        else [{"month": months[i]["month"], **dd_curve[i]} for i in range(len(months))]
    )

    return envelope({
        "resolution": "daily" if daily_dd_curve is not None else "monthly",
        "monthly_volatility": analytics.stdev(period_returns),
        "annualized_volatility": analytics.stdev(period_returns) * (12 ** 0.5),
        "downside_volatility": analytics.downside_stdev(period_returns) * (12 ** 0.5),
        "max_drawdown": daily_max_dd if daily_max_dd is not None else analytics.max_drawdown(cum),
        "current_drawdown": (
            daily_current_dd if daily_current_dd is not None
            else (dd_curve[-1]["drawdown"] if dd_curve else 0)
        ),
        "drawdown_curve": drawdown_curve_out,
        "drawdown_episodes": daily_episodes if daily_episodes is not None
            else analytics.drawdown_episodes(cum, month_labels),
        "sharpe_annualized": analytics.sharpe(period_returns),
        "sortino_annualized": analytics.sortino(period_returns),
        "calmar": analytics.calmar(period_returns),
        "hhi": hhi_value,
        "effective_n": analytics.effective_n(weights),
        "diversification_score": 1 - hhi_value,
        "top_5_share": top_5_share,
        "top_10_share": top_10_share,
        "position_count": len(holdings),
        "leverage_value_twd": leverage_value,
        "leverage_pct": leverage_pct,
        "leverage_timeline": leverage_timeline,
        "weight_distribution": [
            {"code": h["code"], "name": h["name"], "weight": h["mkt_value_twd"] / total}
            for h in sorted(holdings, key=lambda h: h["mkt_value_twd"], reverse=True)
        ],
    })
