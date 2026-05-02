"""GET /api/tickers (list) and /api/tickers/<code> (detail).

Trade gives us DISTINCT(code) cheaply, so the list endpoint can return
real data right now. Detail returns trades for the code; richer fields
(realized P&L summary, position history, dividends) come in Phase 7
when analytics + dividends models port.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import Session, select

from invest.http.deps import get_session
from invest.http.envelope import error, success
from invest.persistence.models.trade import Trade

router = APIRouter()


def _serialize_trade(t: Trade) -> dict[str, Any]:
    return {
        "id": t.id,
        "date": t.date.isoformat(),
        "code": t.code,
        "side": t.side,
        "qty": t.qty,
        "price": str(t.price),
        "currency": t.currency,
        "venue": t.venue,
        "source": t.source,
    }


@router.get("/api/tickers")
def list_tickers(session: Session = Depends(get_session)) -> dict[str, Any]:
    stmt = select(Trade.code, func.count(Trade.id).label("n")).group_by(Trade.code)
    rows = list(session.exec(stmt).all())
    return success([{"code": r[0], "trade_count": int(r[1])} for r in rows])


@router.get("/api/tickers/{code}")
def ticker_detail(code: str, session: Session = Depends(get_session)) -> Any:
    trades = list(session.exec(select(Trade).where(Trade.code == code)).all())
    if not trades:
        return JSONResponse(status_code=404, content=error("not found"))
    trades.sort(key=lambda t: (t.date, t.id or 0))
    return success({
        "code": code,
        "trades": [_serialize_trade(t) for t in trades],
        "trade_count": len(trades),
    })
