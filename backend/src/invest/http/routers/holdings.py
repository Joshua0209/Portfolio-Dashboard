"""GET /api/holdings/{current,timeline,sectors,snapshot/<month>}.

Phase 6.5 wiring: full port of legacy app/api/holdings.py.

  /current        Today's positions, valued today (current_holdings)
  /timeline       Per-period count + MV (?resolution=daily falls back
                  to monthly when daily store is empty — never 404)
  /sectors        Sector breakdown of current_holdings
  /snapshot/<m>   Historical PDF month-end (literal "as of past month")

Empty-state envelopes preserved verbatim from legacy.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from invest.analytics import monthly as analytics
from invest.analytics.holdings_today import current_holdings, holdings_for_month
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/holdings/current")
def current(
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    if not s.months:
        return envelope({"holdings": [], "total_twd": 0})
    last = s.months[-1]

    rows = current_holdings(s, daily)
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


def _monthly_timeline(s: PortfolioStore) -> list[dict]:
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


def _daily_timeline(daily: DailyStore) -> list[dict]:
    return [
        {
            "date": r["date"],
            "tw_count": r["n_tw"],
            "foreign_count": r["n_foreign"],
            "tw_mv_twd": r["tw_twd"],
            "foreign_mv_twd": r["foreign_twd"],
        }
        for r in daily.get_allocation_timeseries()
    ]


@router.get("/api/holdings/timeline")
def timeline(
    resolution: str = Query("monthly"),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    if (resolution or "").lower() == "daily":
        d = _daily_timeline(daily)
        if d:
            return envelope({"resolution": "daily", "rows": d})
    return envelope({"resolution": "monthly", "rows": _monthly_timeline(s)})


@router.get("/api/holdings/sectors")
def sectors(
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> Any:
    if not s.months:
        return envelope([])
    rows = current_holdings(s, daily)
    if not rows:
        return envelope([])
    total = sum(r["mkt_value_twd"] for r in rows) or 1
    for r in rows:
        r["weight"] = r["mkt_value_twd"] / total
    return envelope(analytics.sector_breakdown(rows))


@router.get("/api/holdings/snapshot/{month}")
def snapshot(
    month: str,
    s: PortfolioStore = Depends(get_portfolio_store),
) -> Any:
    found = next((m for m in s.months if m["month"] == month), None)
    if not found:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "data": {"holdings": []},
                     "error": "month not found"},
        )
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
