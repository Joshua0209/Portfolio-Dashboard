"""Transactions: filterable trade history."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from flask import Blueprint, request

from ._helpers import envelope, store

bp = Blueprint("transactions", __name__, url_prefix="/api/transactions")


def _parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y/%m/%d")
    except (TypeError, ValueError):
        return datetime(1970, 1, 1)


@bp.get("")
def list_transactions():
    s = store()
    trades = list(s.all_trades)

    venue = request.args.get("venue")
    side = request.args.get("side")
    code = request.args.get("code")
    month = request.args.get("month")
    q = (request.args.get("q") or "").strip().lower()

    if venue:
        trades = [t for t in trades if t.get("venue") == venue]
    if side:
        trades = [t for t in trades if t.get("side") == side]
    if code:
        trades = [t for t in trades if (t.get("code") or "") == code]
    if month:
        trades = [t for t in trades if t.get("month") == month]
    if q:
        trades = [
            t for t in trades
            if q in (t.get("name") or "").lower()
            or q in (t.get("code") or "").lower()
        ]

    trades.sort(key=lambda t: _parse_date(t.get("date", "")), reverse=True)
    return envelope(trades, count=len(trades))


@bp.get("/aggregates")
def aggregates():
    s = store()
    trades = s.all_trades

    by_month_venue: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "fees": 0.0, "tax": 0.0, "n": 0}
    )
    by_venue: dict[str, dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "fees": 0.0, "tax": 0.0, "n": 0}
    )
    fee_total = 0.0
    tax_total = 0.0
    buy_total = 0.0
    sell_total = 0.0
    n = 0

    for t in trades:
        venue = t.get("venue", "?")
        month = t.get("month", "?")
        gross = t.get("gross_twd", 0) or 0
        fee = t.get("fee_twd", 0) or 0
        tax = t.get("tax_twd", 0) or 0
        side = t.get("side", "")

        is_buy = side in ("普買", "資買", "融資買進", "買進")
        is_sell = side in ("普賣", "資賣", "融資賣出", "賣出")

        bucket_key = (month, venue)
        bucket = by_month_venue[bucket_key]
        venue_bucket = by_venue[venue]

        if is_buy:
            bucket["buy"] += gross
            venue_bucket["buy"] += gross
            buy_total += gross
        elif is_sell:
            bucket["sell"] += gross
            venue_bucket["sell"] += gross
            sell_total += gross

        bucket["fees"] += fee
        bucket["tax"] += tax
        bucket["n"] += 1
        venue_bucket["fees"] += fee
        venue_bucket["tax"] += tax
        venue_bucket["n"] += 1

        fee_total += fee
        tax_total += tax
        n += 1

    monthly = []
    months_seen = sorted({k[0] for k in by_month_venue.keys()})
    venues_seen = sorted({k[1] for k in by_month_venue.keys()})
    for m in months_seen:
        row = {"month": m}
        for v in venues_seen:
            b = by_month_venue.get((m, v), {})
            row[f"{v}_buy"] = b.get("buy", 0)
            row[f"{v}_sell"] = b.get("sell", 0)
            row[f"{v}_fees"] = b.get("fees", 0)
            row[f"{v}_tax"] = b.get("tax", 0)
            row[f"{v}_n"] = b.get("n", 0)
        monthly.append(row)

    # Per-exchange foreign-trade volume (NASDAQ vs NYSE etc.).
    by_exchange: dict[str, dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "fees": 0.0, "n": 0}
    )
    for t in trades:
        if t.get("venue") != "Foreign":
            continue
        ex = t.get("exchange") or "Other"
        b = by_exchange[ex]
        gross = t.get("gross_twd", 0) or 0
        if t.get("side") == "買進":
            b["buy"] += gross
        elif t.get("side") == "賣出":
            b["sell"] += gross
        b["fees"] += t.get("fee_twd", 0) or 0
        b["n"] += 1

    notional = buy_total + sell_total
    return envelope({
        "totals": {
            "trades": n,
            "buy_twd": buy_total,
            "sell_twd": sell_total,
            "fees_twd": fee_total,
            "tax_twd": tax_total,
            "fee_drag_pct": (fee_total / notional) if notional else 0,
            "fee_bps": (fee_total / notional * 10000) if notional else 0,
            "tax_bps": (tax_total / notional * 10000) if notional else 0,
            "avg_trade_twd": (notional / n) if n else 0,
            "turnover_twd": notional,
        },
        "by_venue": dict(by_venue),
        "by_exchange": dict(by_exchange),
        "monthly": monthly,
        "venues": venues_seen,
    })
