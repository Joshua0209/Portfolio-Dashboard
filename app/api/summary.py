"""Top-level dashboard summary: KPIs, equity curve, allocation snapshot.

Phase 4 adds the optional ?resolution=daily branch. The default
(?resolution=monthly or no param) is byte-identical to the pre-Phase-4
response — the daily branch only kicks in when the frontend's one-shot
/api/health probe finds the daily layer READY and re-fetches with
?resolution=daily appended.
"""
from __future__ import annotations

from flask import Blueprint, current_app, request

from ._helpers import envelope, store

bp = Blueprint("summary", __name__, url_prefix="/api/summary")


def _daily_store():
    return current_app.extensions["daily_store"]


def _monthly_summary():
    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "kpis": {},
            "twr": 0,
            "xirr": None,
            "profit_twd": 0,
            "invested_twd": 0,
            "equity_curve": [],
            "allocation": {"tw": 0, "foreign": 0, "bank_twd": 0, "bank_usd": 0},
            "first_month": None,
            "last_month": None,
            "months_covered": 0,
        })

    last = months[-1]
    first = months[0]
    kpis = dict(s.kpis)

    equity_curve = [
        {
            "month": m["month"],
            "equity_twd": m.get("equity_twd", 0),
            "tw_mv": m.get("tw_market_value_twd", 0),
            "foreign_mv": m.get("foreign_market_value_twd", 0),
            "bank_twd": m.get("bank_twd", 0),
            "bank_usd_in_twd": m.get("bank_usd_in_twd", 0),
            "external_flow": m.get("external_flow_twd", 0),
            "cum_twr": m.get("cum_twr", 0),
            "period_return": m.get("period_return", 0),
        }
        for m in months
    ]

    cum_twr = last.get("cum_twr", 0) or 0
    xirr = last.get("xirr")
    profit_twd = kpis.get("profit_twd", 0)
    invested_twd = kpis.get("counterfactual_twd", 0)

    allocation = {
        "tw": last.get("tw_market_value_twd", 0),
        "foreign": last.get("foreign_market_value_twd", 0),
        "bank_twd": last.get("bank_twd", 0),
        "bank_usd": last.get("bank_usd_in_twd", 0),
    }

    return envelope({
        "kpis": kpis,
        "twr": cum_twr,
        "xirr": xirr,
        "profit_twd": profit_twd,
        "invested_twd": invested_twd,
        "equity_curve": equity_curve,
        "allocation": allocation,
        "first_month": first["month"],
        "last_month": last["month"],
        "months_covered": len(months),
    })


def _daily_summary():
    """Same envelope shape as monthly, but equity_curve has one row per
    trading day pulled from portfolio_daily. KPIs and allocation still
    come from the monthly source-of-truth (PDF) — only the time series
    swaps to daily resolution."""
    monthly_body = _monthly_summary()
    points = _daily_store().get_equity_curve()
    if not points:
        # Daily layer not backfilled yet — fall back gracefully to the
        # monthly response. Frontend re-checks /api/health and will pick
        # this back up once the cache is warm.
        body = monthly_body
        body["data"]["resolution"] = "monthly"
        return body
    monthly_data = monthly_body["data"]
    daily_curve = [
        {
            "date": p["date"],
            "equity_twd": p["equity_twd"],
            "n_positions": p["n_positions"],
            "fx_usd_twd": p["fx_usd_twd"],
            "has_overlay": bool(p["has_overlay"]),
        }
        for p in points
    ]
    return envelope({
        **{k: v for k, v in monthly_data.items() if k != "equity_curve"},
        "equity_curve": daily_curve,
        "resolution": "daily",
    })


@bp.get("")
def summary():
    resolution = (request.args.get("resolution") or "").lower()
    if resolution == "daily":
        return _daily_summary()
    # Default + unknown values fall through to monthly to preserve the
    # backwards-compatible response.
    return _monthly_summary()
