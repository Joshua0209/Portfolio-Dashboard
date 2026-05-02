"""Holdings: current positions, sectors, timeline, historical snapshots.

The "current" question (what's held today, valued today) is answered by
app/holdings_today.py — see its docstring for the resolution rules.
This blueprint just shapes those rows for the API and adds aggregates
(weights, totals, sector breakdown).

Historical snapshots (/snapshot/<month>) reach for `holdings_for_month`
because the literal question is "as of past month-end" — that's PDF-only
by definition.
"""
from __future__ import annotations

from flask import Blueprint

from .. import analytics
from ..holdings_today import current_holdings, holdings_for_month
from ._helpers import daily_store, envelope, store

bp = Blueprint("holdings", __name__, url_prefix="/api/holdings")


@bp.get("/current")
def current():
    s = store()
    if not s.months:
        return envelope({"holdings": [], "total_twd": 0})
    last = s.months[-1]

    rows = current_holdings(s, daily_store())
    if not rows:
        return envelope({
            "as_of": last["month"],
            "fx_usd_twd": last.get("fx_usd_twd"),
            "holdings": [],
            "total_mv_twd": 0,
            "total_cost_twd": 0,
            "total_upnl_twd": 0,
            "total_upnl_pct": 0,
            "repriced_holdings_count": 0,
        })

    rows.sort(key=lambda r: r["mkt_value_twd"], reverse=True)
    total = sum(r["mkt_value_twd"] for r in rows)
    for r in rows:
        r["weight"] = (r["mkt_value_twd"] / total) if total else 0
    total_cost = sum(r["cost_twd"] for r in rows)
    total_upnl = sum(r["unrealized_pnl_twd"] for r in rows)
    n_repriced = sum(1 for r in rows if r.get("repriced_at"))
    as_of = next((r["repriced_at"] for r in rows if r.get("repriced_at")), last["month"])
    return envelope({
        "as_of": as_of,
        "fx_usd_twd": last.get("fx_usd_twd"),
        "holdings": rows,
        "total_mv_twd": total,
        "total_cost_twd": total_cost,
        "total_upnl_twd": total_upnl,
        "total_upnl_pct": (total_upnl / total_cost) if total_cost else 0,
        "repriced_holdings_count": n_repriced,
    })


def _monthly_timeline() -> list[dict]:
    s = store()
    out = []
    for m in s.months:
        tw = m.get("tw", {}).get("holdings", []) or []
        fr = m.get("foreign", {}).get("holdings", []) or []
        out.append({
            "month": m["month"],
            "tw_count": len(tw),
            "foreign_count": len(fr),
            "tw_mv_twd": m.get("tw_market_value_twd", 0),
            "foreign_mv_twd": m.get("foreign_market_value_twd", 0),
        })
    return out


def _daily_timeline() -> list[dict]:
    """One row per priced day from positions_daily, aggregated by venue."""
    return [
        {
            "date": r["date"],
            "tw_count": r["n_tw"],
            "foreign_count": r["n_foreign"],
            "tw_mv_twd": r["tw_twd"],
            "foreign_mv_twd": r["foreign_twd"],
        }
        for r in daily_store().get_allocation_timeseries()
    ]


@bp.get("/timeline")
def timeline():
    """Holdings count + market value per period for trend view.

    ?resolution=daily swaps to per-day rows from positions_daily; empty
    daily store falls back to monthly so the frontend never sees 404.
    """
    from ._helpers import want_daily

    if want_daily():
        daily = _daily_timeline()
        if daily:
            return envelope({"resolution": "daily", "rows": daily})
    return envelope({"resolution": "monthly", "rows": _monthly_timeline()})


@bp.get("/sectors")
def sectors():
    s = store()
    if not s.months:
        return envelope([])
    rows = current_holdings(s, daily_store())
    if not rows:
        return envelope([])
    total = sum(r["mkt_value_twd"] for r in rows) or 1
    for r in rows:
        r["weight"] = r["mkt_value_twd"] / total
    return envelope(analytics.sector_breakdown(rows))


@bp.get("/snapshot/<month>")
def snapshot(month: str):
    s = store()
    found = next((m for m in s.months if m["month"] == month), None)
    if not found:
        return envelope({"holdings": []}, error="month not found"), 404
    rows = holdings_for_month(found)
    rows.sort(key=lambda r: r["mkt_value_twd"], reverse=True)
    total = sum(r["mkt_value_twd"] for r in rows)
    for r in rows:
        r["weight"] = (r["mkt_value_twd"] / total) if total else 0
    return envelope({
        "month": month,
        "fx_usd_twd": found.get("fx_usd_twd"),
        "holdings": rows,
        "total_mv_twd": total,
    })
