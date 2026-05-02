"""GET /api/transactions and /api/transactions/aggregates.

Phase 6.5 wiring: full port of legacy app/api/transactions.py. Reads
from PortfolioStore.all_trades (parsed-PDF source) + trades_overlay
(Shioaji session) joined into one chronological list.

Filters: ?venue, ?side, ?code, ?month=YYYY-MM, ?q=substring (legacy
parity).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query

from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


def _parse_date(s: str) -> datetime:
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except (TypeError, ValueError):
            continue
    return datetime(1970, 1, 1)


def _overlay_trades_as_pdf_shape(daily: DailyStore) -> list[dict]:
    """Pull trades_overlay rows reshaped to match PDF trade shape."""
    out: list[dict] = []
    try:
        with daily.connect_ro() as conn:
            rows = conn.execute(
                "SELECT date, code, side, qty, price, fee_twd, tax_twd, "
                "gross_twd, net_twd, ccy, venue, type, source "
                "FROM trades_overlay"
            ).fetchall()
    except Exception:
        # trades_overlay table may not exist yet (fresh DB) — treat as empty.
        return []
    for r in rows:
        out.append({
            "month": r["date"][:7],
            "date": r["date"],
            "code": r["code"],
            "name": r["code"],
            "side": r["side"],
            "qty": r["qty"],
            "price": r["price"],
            "ccy": r["ccy"],
            "venue": r["venue"],
            "type": r["type"],
            "fee_twd": r["fee_twd"],
            "tax_twd": r["tax_twd"],
            "gross_twd": r["gross_twd"],
            "net_twd": r["net_twd"],
            "margin_loan_twd": 0.0,
            "self_funded_twd": 0.0,
            "source": "overlay",
        })
    return out


@router.get("/api/transactions")
def list_transactions(
    venue: str | None = Query(default=None),
    side: str | None = Query(default=None),
    code: str | None = Query(default=None),
    month: str | None = Query(default=None),
    q: str | None = Query(default=None),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    pdf_trades = list(s.all_trades)
    trades = pdf_trades + _overlay_trades_as_pdf_shape(daily)

    if venue:
        trades = [t for t in trades if t.get("venue") == venue]
    if side:
        trades = [t for t in trades if t.get("side") == side]
    if code:
        trades = [t for t in trades if (t.get("code") or "") == code]
    if month:
        trades = [t for t in trades if t.get("month") == month]
    if q:
        ql = q.strip().lower()
        trades = [
            t for t in trades
            if ql in (t.get("name") or "").lower()
            or ql in (t.get("code") or "").lower()
        ]

    trades.sort(key=lambda t: _parse_date(t.get("date", "")), reverse=True)
    return envelope(trades, count=len(trades))


@router.get("/api/transactions/aggregates")
def aggregates(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
    trades = s.all_trades

    by_month_venue: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "fees": 0.0, "tax": 0.0, "rebate": 0.0, "n": 0}
    )
    by_venue: dict[str, dict] = defaultdict(
        lambda: {"buy": 0.0, "sell": 0.0, "fees": 0.0, "tax": 0.0, "rebate": 0.0, "n": 0}
    )
    fee_total = 0.0
    tax_total = 0.0
    buy_total = 0.0
    sell_total = 0.0
    n = 0

    rebates_by_month: dict[str, float] = defaultdict(float)
    rebate_total = 0.0
    for m in s.months:
        ym = m.get("month") or "?"
        for r in (m.get("tw") or {}).get("rebates", []) or []:
            amt = r.get("amount_twd", 0) or 0
            rebates_by_month[ym] += amt
            rebate_total += amt
            by_month_venue[(ym, "TW")]["rebate"] += amt
            by_venue["TW"]["rebate"] += amt

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
    months_seen = sorted({k[0] for k in by_month_venue.keys()} | set(rebates_by_month.keys()))
    venues_seen = sorted({k[1] for k in by_month_venue.keys()})
    for m in months_seen:
        row = {"month": m, "rebate": rebates_by_month.get(m, 0)}
        for v in venues_seen:
            b = by_month_venue.get((m, v), {})
            row[f"{v}_buy"] = b.get("buy", 0)
            row[f"{v}_sell"] = b.get("sell", 0)
            row[f"{v}_fees"] = b.get("fees", 0)
            row[f"{v}_tax"] = b.get("tax", 0)
            row[f"{v}_rebate"] = b.get("rebate", 0)
            row[f"{v}_n"] = b.get("n", 0)
        monthly.append(row)

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
    net_cost = fee_total + tax_total - rebate_total
    return envelope({
        "totals": {
            "trades": n,
            "buy_twd": buy_total,
            "sell_twd": sell_total,
            "fees_twd": fee_total,
            "tax_twd": tax_total,
            "rebate_twd": rebate_total,
            "net_cost_twd": net_cost,
            "fee_drag_pct": (net_cost / notional) if notional else 0,
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
