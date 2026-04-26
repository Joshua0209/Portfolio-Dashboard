"""Risk metrics: drawdown, volatility, concentration."""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from .holdings import _holdings_for_month
from ._helpers import envelope, store

bp = Blueprint("risk", __name__, url_prefix="/api/risk")


@bp.get("")
def risk():
    s = store()
    months = s.months
    if not months:
        return envelope({"empty": True})

    period_returns = [m.get("period_return", 0) or 0 for m in months]
    cum = [m.get("cum_twr", 0) or 0 for m in months]
    dd_curve = analytics.drawdown_curve(cum)
    month_labels = [m["month"] for m in months]

    last = months[-1]
    holdings = _holdings_for_month(last)
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

    return envelope({
        "monthly_volatility": analytics.stdev(period_returns),
        "annualized_volatility": analytics.stdev(period_returns) * (12 ** 0.5),
        "downside_volatility": analytics.downside_stdev(period_returns) * (12 ** 0.5),
        "max_drawdown": analytics.max_drawdown(cum),
        "current_drawdown": dd_curve[-1]["drawdown"] if dd_curve else 0,
        "drawdown_curve": [
            {"month": months[i]["month"], **dd_curve[i]} for i in range(len(months))
        ],
        "drawdown_episodes": analytics.drawdown_episodes(cum, month_labels),
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
