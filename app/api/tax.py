"""Tax / cost-basis report."""
from __future__ import annotations

from collections import defaultdict

from flask import Blueprint

from .. import analytics
from .holdings import _holdings_for_month
from ._helpers import envelope, store

bp = Blueprint("tax", __name__, url_prefix="/api/tax")


@bp.get("")
def tax():
    s = store()
    realized = analytics.realized_pnl_by_ticker(s.by_ticker)

    last = s.months[-1] if s.months else {}
    current = _holdings_for_month(last) if last else []
    current_by_code = {h["code"]: h for h in current if h.get("code")}

    enriched = []
    for r in realized:
        code = r["code"]
        cur = current_by_code.get(code)
        unrealized = cur.get("unrealized_pnl_twd", 0) if cur else 0
        unrealized_pct = cur.get("unrealized_pct", 0) if cur else 0
        cur_qty = cur.get("qty", 0) if cur else 0
        enriched.append({
            **r,
            "current_qty": cur_qty,
            "unrealized_pnl_twd": unrealized,
            "unrealized_pct": unrealized_pct,
            "total_pnl_twd": r["realized_pnl_twd"] + unrealized,
        })
    enriched.sort(key=lambda r: r["total_pnl_twd"], reverse=True)

    realized_total = sum(r["realized_pnl_twd"] for r in realized)
    fees_total = sum(r["fees_twd"] for r in realized)
    tax_total = sum(r["tax_twd"] for r in realized)
    unrealized_total = sum(r["unrealized_pnl_twd"] for r in enriched)

    winners = [r for r in enriched if r["realized_pnl_twd"] > 0]
    losers = [r for r in enriched if r["realized_pnl_twd"] < 0]

    return envelope({
        "by_ticker": enriched,
        "totals": {
            "realized_pnl_twd": realized_total,
            "unrealized_pnl_twd": unrealized_total,
            "total_pnl_twd": realized_total + unrealized_total,
            "fees_twd": fees_total,
            "tax_twd": tax_total,
            "winners_count": len(winners),
            "losers_count": len(losers),
            "win_rate": len(winners) / (len(winners) + len(losers)) if (winners or losers) else 0,
        },
    })
