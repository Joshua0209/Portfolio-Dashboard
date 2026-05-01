"""GET /api/dividends - dividend events + rebates.

Phase 6 baseline: empty-state envelope. Source-of-truth in legacy is
`summary.dividends` from portfolio.json — bank-derived events that don't
yet have a Trade-equivalent representation. Phase 7 will introduce a
Dividend model + parser hook so these arrive on real data; until then
the response shape mirrors the legacy empty branch so the frontend's
empty-table rendering still works.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/dividends")
def dividends(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "rows": [],
        "by_ticker": {},
        "monthly_by_venue": [],
        "by_ccy": {},
        "totals": {"count": 0, "total_twd": 0},
    })
