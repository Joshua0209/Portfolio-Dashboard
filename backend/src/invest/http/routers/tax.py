"""GET /api/tax - per-ticker realized + unrealized P&L (FIFO basis).

Phase 6 baseline: empty list. Realized P&L computation uses
analytics.realized_pnl_by_ticker_fifo + holdings_today.current_holdings
in legacy. Both are Phase 7 ports (analytics module hasn't been split
yet per PLAN section 6).
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/tax")
def tax(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success([])
