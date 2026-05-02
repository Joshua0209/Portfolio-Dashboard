"""GET /api/holdings/{current,timeline,sectors,snapshot/<month>}.

Phase 6 baseline: empty-state envelopes only.
  /current        -> {holdings: [], total_twd: 0}
  /timeline       -> {resolution: "monthly", rows: []}
                     ?resolution=daily falls back to monthly when daily
                     store is empty (legacy invariant — never 404).
  /sectors        -> []
  /snapshot/<m>   -> 404 with {ok: False, error: "month not found"}
                     (no PortfolioMonthly model yet — Phase 7 may add one)

Currently-held positions, historical snapshots, and sector breakdown all
read through the analytics layer and holdings_today helpers in legacy.
Those modules are explicitly Phase 7+ work, so this router pins shape
today and gets data wired later.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import error, success

router = APIRouter()


@router.get("/api/holdings/current")
def current(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({"holdings": [], "total_twd": 0})


@router.get("/api/holdings/timeline")
def timeline(_session: Session = Depends(get_session)) -> dict[str, Any]:
    # Legacy invariant: ?resolution=daily falls back to monthly empty.
    # We honor the param shape but the response is monthly until daily
    # store is populated.
    return success({"resolution": "monthly", "rows": []})


@router.get("/api/holdings/sectors")
def sectors(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success([])


@router.get("/api/holdings/snapshot/{month}")
def snapshot(month: str, _session: Session = Depends(get_session)) -> Any:
    return JSONResponse(status_code=404, content=error("month not found"))
