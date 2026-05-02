"""GET /api/daily/equity and /api/daily/prices/{symbol}.

Both endpoints are gated by the daily-state machine (Cycle 42's
synthesized PortfolioDaily-existence gate). Both validate ?start and
?end against a strict YYYY-MM-DD regex; legacy app/api/daily.py uses
the same regex specifically to reject 2026-99-99 instead of bubbling a
ValueError from date.fromisoformat() through the store layer.

Phase 6 baseline: when ready, return empty points/trades arrays. The
PriceRepo for per-symbol history exists, but wiring the trade-marker
overlay (which mixes Trade rows + portfolio.json all_trades in legacy)
is Phase 7's job.
"""
from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import Session, select

from invest.http.deps import get_session
from invest.http.envelope import error, success
from invest.persistence.models.portfolio_daily import PortfolioDaily

router = APIRouter()

_ISO_DATE_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


def _has_portfolio_daily(session: Session) -> bool:
    last = session.scalar(select(func.max(PortfolioDaily.date)))
    return last is not None


def _initializing() -> JSONResponse:
    return JSONResponse(
        status_code=202,
        content=success({"state": "INITIALIZING", "progress": {}}),
    )


def _bad_date(field: str) -> JSONResponse:
    return JSONResponse(
        status_code=400, content=error(f"{field} must be YYYY-MM-DD"),
    )


def _check_iso(s: str | None, field: str) -> JSONResponse | None:
    if s is not None and not _ISO_DATE_RE.match(s):
        return _bad_date(field)
    return None


@router.get("/api/daily/equity")
def equity(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> Any:
    if not _has_portfolio_daily(session):
        return _initializing()
    bad = _check_iso(start, "start") or _check_iso(end, "end")
    if bad is not None:
        return bad
    return success({
        "points": [],
        "empty": True,
        "start": start,
        "end": end,
    })


@router.get("/api/daily/prices/{symbol}")
def prices(
    symbol: str,
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> Any:
    if not _has_portfolio_daily(session):
        return _initializing()
    bad = _check_iso(start, "start") or _check_iso(end, "end")
    if bad is not None:
        return bad
    return success({
        "symbol": symbol,
        "points": [],
        "trades": [],
        "empty": True,
        "start": start,
        "end": end,
    })
