"""Daily-resolution endpoints: /api/daily/equity, /api/daily/prices/<symbol>.

The daily store is read-only on the request path. When the backfill
hasn't run yet, the `require_ready_or_warming` decorator returns 202
(INITIALIZING) or 503 (FAILED) instead of letting the empty store leak
through.
"""
from __future__ import annotations

import re

from flask import Blueprint, abort, request

from ._helpers import (
    daily_store,
    envelope,
    require_ready_or_warming,
    store as portfolio_store,
)

bp = Blueprint("daily", __name__, url_prefix="/api/daily")

# Strict ISO YYYY-MM-DD; rejects bad month/day so a malformed query like
# ?start=2026-99-99 returns HTTP 400 instead of bubbling a ValueError
# from date.fromisoformat() deeper in the store layer.
_ISO_DATE_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


def _validate_iso_date(s: str | None, field: str) -> str | None:
    if s is not None and not _ISO_DATE_RE.match(s):
        abort(400, description=f"{field} must be YYYY-MM-DD")
    return s


def _normalize_trade_date(d: str) -> str:
    """portfolio.json trade dates use 'YYYY/MM/DD'; normalize to ISO so
    they line up with prices.date keys on the chart."""
    return d.replace("/", "-") if "/" in d else d


@bp.get("/equity")
@require_ready_or_warming
def equity_curve():
    start = _validate_iso_date(request.args.get("start") or None, "start")
    end = _validate_iso_date(request.args.get("end") or None, "end")
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
    start = _validate_iso_date(request.args.get("start") or None, "start")
    end = _validate_iso_date(request.args.get("end") or None, "end")
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
