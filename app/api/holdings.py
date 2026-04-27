"""Holdings: current positions across TW + foreign, with computed unified shape."""
from __future__ import annotations

from flask import Blueprint, current_app, request

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("holdings", __name__, url_prefix="/api/holdings")


def _daily_store():
    return current_app.extensions["daily_store"]


def _normalize_tw(h: dict, fx: float) -> dict:
    return {
        "venue": "TW",
        "code": h.get("code"),
        "name": h.get("name"),
        "type": h.get("type"),
        "ccy": "TWD",
        "qty": h.get("qty", 0),
        "avg_cost": h.get("avg_cost", 0),
        "cost_local": h.get("cost", 0),
        "cost_twd": h.get("cost", 0),
        "ref_price": h.get("ref_price", 0),
        "mkt_value_local": h.get("mkt_value", 0),
        "mkt_value_twd": h.get("mkt_value", 0),
        "unrealized_pnl_local": h.get("unrealized_pnl", 0),
        "unrealized_pnl_twd": h.get("unrealized_pnl", 0),
        "unrealized_pct": (h.get("unrealized_pnl", 0) / h["cost"]) if h.get("cost") else 0,
    }


def _normalize_foreign(h: dict, fx: float) -> dict:
    ccy = h.get("ccy", "USD")
    rate = fx if ccy == "USD" else 1.0
    cost_local = h.get("cost", 0)
    mkt_local = h.get("mkt_value", 0)
    upnl_local = h.get("unrealized_pnl", 0)
    return {
        "venue": "Foreign",
        "code": h.get("code"),
        "name": h.get("name"),
        "type": h.get("market"),
        "ccy": ccy,
        "qty": h.get("qty", 0),
        "avg_cost": (cost_local / h["qty"]) if h.get("qty") else 0,
        "cost_local": cost_local,
        "cost_twd": cost_local * rate,
        "ref_price": h.get("close", 0),
        "mkt_value_local": mkt_local,
        "mkt_value_twd": mkt_local * rate,
        "unrealized_pnl_local": upnl_local,
        "unrealized_pnl_twd": upnl_local * rate,
        "unrealized_pct": (upnl_local / cost_local) if cost_local else 0,
    }


def _holdings_for_month(month: dict) -> list[dict]:
    fx = month.get("fx_usd_twd", 1.0) or 1.0
    rows: list[dict] = []
    for h in month.get("tw", {}).get("holdings", []):
        rows.append(_normalize_tw(h, fx))
    for h in month.get("foreign", {}).get("holdings", []):
        rows.append(_normalize_foreign(h, fx))
    return rows


@bp.get("/current")
def current():
    s = store()
    if not s.months:
        return envelope({"holdings": [], "total_twd": 0})
    last = s.months[-1]
    rows = _holdings_for_month(last)
    rows.sort(key=lambda r: r["mkt_value_twd"], reverse=True)
    total = sum(r["mkt_value_twd"] for r in rows)
    for r in rows:
        r["weight"] = (r["mkt_value_twd"] / total) if total else 0
    total_cost = sum(r["cost_twd"] for r in rows)
    total_upnl = sum(r["unrealized_pnl_twd"] for r in rows)
    return envelope({
        "as_of": last["month"],
        "fx_usd_twd": last.get("fx_usd_twd"),
        "holdings": rows,
        "total_mv_twd": total,
        "total_cost_twd": total_cost,
        "total_upnl_twd": total_upnl,
        "total_upnl_pct": (total_upnl / total_cost) if total_cost else 0,
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
        for r in _daily_store().get_allocation_timeseries()
    ]


@bp.get("/timeline")
def timeline():
    """Holdings count + market value per period for trend view.

    Always returns {rows, resolution}. ?resolution=daily swaps to per-day
    rows from positions_daily; empty daily store falls back to monthly so
    the frontend never sees a 404.
    """
    if (request.args.get("resolution") or "").lower() == "daily":
        daily = _daily_timeline()
        if daily:
            return envelope({"resolution": "daily", "rows": daily})
    return envelope({"resolution": "monthly", "rows": _monthly_timeline()})


@bp.get("/sectors")
def sectors():
    s = store()
    if not s.months:
        return envelope([])
    last = s.months[-1]
    rows = _holdings_for_month(last)
    total = sum(r["mkt_value_twd"] for r in rows) or 1
    for r in rows:
        r["weight"] = r["mkt_value_twd"] / total
    breakdown = analytics.sector_breakdown(rows)
    return envelope(breakdown)


@bp.get("/snapshot/<month>")
def snapshot(month: str):
    s = store()
    found = next((m for m in s.months if m["month"] == month), None)
    if not found:
        return envelope({"holdings": []}, error="month not found"), 404
    rows = _holdings_for_month(found)
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
