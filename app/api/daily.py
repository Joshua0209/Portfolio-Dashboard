"""Daily-resolution endpoints: /api/daily/equity, /api/daily/prices/<symbol>.

The daily store is read-only on the request path. When the backfill
hasn't run yet, the `require_ready_or_warming` decorator returns 202
(INITIALIZING) or 503 (FAILED) instead of letting the empty store leak
through.
"""
from __future__ import annotations

from flask import Blueprint, request

from ._helpers import (
    daily_store,
    envelope,
    require_ready_or_warming,
    store as portfolio_store,
)

bp = Blueprint("daily", __name__, url_prefix="/api/daily")


def _normalize_trade_date(d: str) -> str:
    """portfolio.json trade dates use 'YYYY/MM/DD'; normalize to ISO so
    they line up with prices.date keys on the chart."""
    return d.replace("/", "-") if "/" in d else d


@bp.get("/equity")
@require_ready_or_warming
def equity_curve():
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    points = daily_store().get_equity_curve(start=start, end=end)
    return envelope({
        "points": points,
        "empty": len(points) == 0,
        "start": start,
        "end": end,
    })


@bp.get("/prices/<symbol>")
@require_ready_or_warming
def prices(symbol: str):
    """Daily price history for one symbol + trade markers from portfolio.json.

    Trades are surfaced verbatim from `summary.all_trades` (filtered to
    this symbol, dates ISO-normalized). Source-of-truth stays the PDF —
    if a marker looks misplaced, check portfolio.json, not the SQLite
    cache.
    """
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    points = daily_store().get_ticker_history(symbol, start=start, end=end)

    pdf = portfolio_store()
    trades_raw = pdf.all_trades or []
    trades: list[dict] = []
    for t in trades_raw:
        if t.get("code") != symbol:
            continue
        d = _normalize_trade_date(t.get("date", ""))
        if start and d < start:
            continue
        if end and d > end:
            continue
        trades.append({
            "date": d,
            "side": t.get("side"),
            "qty": t.get("qty"),
            "price": t.get("price"),
            "venue": t.get("venue"),
            "ccy": t.get("ccy"),
        })

    return envelope({
        "symbol": symbol,
        "points": points,
        "trades": trades,
        "empty": len(points) == 0,
        "start": start,
        "end": end,
    })
