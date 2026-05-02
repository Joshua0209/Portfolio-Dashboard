"""GET /api/tax — per-ticker realized + unrealized P&L (FIFO basis)."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from invest.analytics import monthly as analytics
from invest.analytics.holdings_today import current_holdings
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/tax")
def tax(
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    realized_avg = {r["code"]: r for r in analytics.realized_pnl_by_ticker(s.by_ticker)}
    realized_fifo = analytics.realized_pnl_by_ticker_fifo(s.by_ticker)

    current = current_holdings(s, daily) if s.months else []
    current_by_code = {h["code"]: h for h in current if h.get("code")}

    enriched = []
    for r in realized_fifo:
        code = r["code"]
        cur = current_by_code.get(code)
        unrealized = cur.get("unrealized_pnl_twd", 0) if cur else 0
        unrealized_pct = cur.get("unrealized_pct", 0) if cur else 0
        cur_qty = cur.get("qty", 0) if cur else 0
        avg = realized_avg.get(code, {})
        enriched.append({
            **r,
            "realized_pnl_avg_twd": avg.get("realized_pnl_twd", 0),
            "current_qty": cur_qty,
            "unrealized_pnl_twd": unrealized,
            "unrealized_pct": unrealized_pct,
            "total_pnl_twd": r["realized_pnl_twd"] + unrealized + r.get("dividends_twd", 0),
        })

    fifo_codes = {r["code"] for r in realized_fifo}
    for code, cur in current_by_code.items():
        if code in fifo_codes:
            continue
        upnl = cur.get("unrealized_pnl_twd", 0) or 0
        if not cur.get("qty") and not upnl:
            continue
        enriched.append({
            "code": code,
            "name": cur.get("name"),
            "venue": cur.get("venue"),
            "realized_pnl_twd": 0.0,
            "realized_pnl_avg_twd": 0.0,
            "sell_proceeds_twd": 0.0,
            "cost_of_sold_twd": 0.0,
            "sell_qty": 0,
            "open_qty": cur.get("qty", 0),
            "current_qty": cur.get("qty", 0),
            "open_cost_twd": cur.get("cost_twd", 0),
            "wins": 0, "losses": 0,
            "win_rate": None, "profit_factor": None, "avg_holding_days": None,
            "dividends_twd": 0.0,
            "fully_closed": False,
            "unrealized_pnl_twd": upnl,
            "unrealized_pct": cur.get("unrealized_pct", 0),
            "total_pnl_twd": upnl,
        })
    enriched.sort(key=lambda r: r["total_pnl_twd"], reverse=True)

    realized_total = sum(r["realized_pnl_twd"] for r in enriched)
    div_total = sum(r.get("dividends_twd", 0) for r in enriched)
    fees_total = sum((s.by_ticker.get(r["code"], {}) or {}).get("fees_twd", 0) for r in enriched)
    tax_total = sum((s.by_ticker.get(r["code"], {}) or {}).get("tax_twd", 0) for r in enriched)
    unrealized_total = sum(h.get("unrealized_pnl_twd", 0) or 0 for h in current)

    rebate_total = 0.0
    for m in s.months:
        for r in (m.get("tw") or {}).get("rebates", []) or []:
            rebate_total += r.get("amount_twd", 0) or 0

    closed = [r for r in enriched if r.get("fully_closed")]
    winners = [r for r in closed if r["realized_pnl_twd"] > 0]
    losers = [r for r in closed if r["realized_pnl_twd"] < 0]
    holding_days = [r["avg_holding_days"] for r in enriched if r.get("avg_holding_days") is not None]
    avg_hold = sum(holding_days) / len(holding_days) if holding_days else None

    return envelope({
        "by_ticker": enriched,
        "totals": {
            "realized_pnl_twd": realized_total,
            "dividends_twd": div_total,
            "unrealized_pnl_twd": unrealized_total,
            "total_pnl_twd": realized_total + div_total + unrealized_total,
            "fees_twd": fees_total,
            "tax_twd": tax_total,
            "rebate_twd": rebate_total,
            "net_cost_twd": fees_total + tax_total - rebate_total,
            "closed_positions": len(closed),
            "winners_count": len(winners),
            "losers_count": len(losers),
            "win_rate": (len(winners) / (len(winners) + len(losers))) if (winners or losers) else 0,
            "avg_holding_days": avg_hold,
        },
    })
