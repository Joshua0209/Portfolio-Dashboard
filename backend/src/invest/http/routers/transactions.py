"""GET /api/transactions and /api/transactions/aggregates.

Trade is the authoritative source for trade history in the new schema,
so these endpoints can return real data right now (unlike summary or
holdings which depend on the analytics aggregator). Field encoding
uses the new schema's shape (numeric Side, Decimal price, ISO date) —
Phase 8 frontend regenerates types from OpenAPI to match.

Filters: ?code, ?venue, ?source, ?month=YYYY-MM, ?q (legacy parity).
Sort: descending by date, then by id (stable within same date).

Aggregates today are limited to trade COUNT and per-(month, venue)
counts. Full TWD totals require currency-converted gross_twd which the
schema doesn't carry — Phase 7 wires the converter.

Note on ?side=: the legacy endpoint accepted a Chinese-string side
filter (e.g. "現買"). The new schema encodes Side as an integer enum.
A ?side= filter is deferred until Phase 8 when the frontend migrates to
the new OpenAPI types — adding it before then would be dead weight.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session, select

from invest.http.deps import get_session
from invest.http.envelope import success
from invest.persistence.models.trade import Trade

router = APIRouter()


def _serialize(t: Trade) -> dict[str, Any]:
    return {
        "id": t.id,
        "date": t.date.isoformat(),
        "month": t.date.strftime("%Y-%m"),
        "code": t.code,
        "side": t.side,
        "qty": t.qty,
        "price": str(t.price),
        "currency": t.currency,
        "fee": str(t.fee),
        "tax": str(t.tax),
        "rebate": str(t.rebate),
        "source": t.source,
        "venue": t.venue,
    }


def _all_trades(session: Session) -> list[Trade]:
    return list(session.exec(select(Trade)).all())


@router.get("/api/transactions")
def list_transactions(
    code: str | None = Query(default=None),
    venue: str | None = Query(default=None),
    source: str | None = Query(default=None),
    month: str | None = Query(default=None),
    q: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    rows = _all_trades(session)
    if code:
        rows = [r for r in rows if r.code == code]
    if venue:
        rows = [r for r in rows if r.venue == venue]
    if source:
        rows = [r for r in rows if r.source == source]
    if month:
        rows = [r for r in rows if r.date.strftime("%Y-%m") == month]
    if q:
        q_lower = q.lower()
        rows = [r for r in rows if q_lower in (r.code or "").lower()]
    rows.sort(key=lambda r: (r.date, r.id or 0), reverse=True)
    return success([_serialize(r) for r in rows])


@router.get("/api/transactions/aggregates")
def aggregates(session: Session = Depends(get_session)) -> dict[str, Any]:
    rows = _all_trades(session)

    by_venue: dict[str, dict[str, int]] = defaultdict(
        lambda: {"n": 0, "buy_n": 0, "sell_n": 0}
    )
    by_month_venue: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        v = r.venue
        m = r.date.strftime("%Y-%m")
        by_venue[v]["n"] += 1
        # Side encoding: CASH_BUY=1, MARGIN_BUY=11, SHORT_COVER=22 are buys.
        if r.side in (1, 11, 22):
            by_venue[v]["buy_n"] += 1
        else:
            by_venue[v]["sell_n"] += 1
        by_month_venue[(m, v)] += 1

    venues_seen = sorted(by_venue.keys())
    months_seen = sorted({m for m, _ in by_month_venue.keys()})
    monthly = []
    for m in months_seen:
        row: dict[str, Any] = {"month": m}
        for v in venues_seen:
            row[f"{v}_n"] = by_month_venue.get((m, v), 0)
        monthly.append(row)

    return success({
        "totals": {"trades": len(rows)},
        "by_venue": dict(by_venue),
        "monthly": monthly,
        "venues": venues_seen,
    })
