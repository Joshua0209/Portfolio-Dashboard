"""GET /api/daily/equity and /api/daily/prices/{symbol}.

Phase 6.5 wiring: full port of legacy app/api/daily.py. The legacy
state-gate (require_ready_or_warming → INITIALIZING 202) is preserved
via the PortfolioDaily-existence check. Date params validated via the
strict regex from legacy.
"""
from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import Session, select

from invest.http.deps import get_daily_store, get_portfolio_store, get_session
from invest.http.envelope import error, success
from invest.persistence.daily_store import DailyStore
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.portfolio_store import PortfolioStore


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


def _normalize_trade_date(d: str) -> str:
    return d.replace("/", "-") if "/" in d else d


@router.get("/api/daily/equity")
def equity(
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    session: Session = Depends(get_session),
    daily: DailyStore = Depends(get_daily_store),
) -> Any:
    if not _has_portfolio_daily(session):
        return _initializing()
    bad = _check_iso(start, "start") or _check_iso(end, "end")
    if bad is not None:
        return bad
    points = daily.get_equity_curve(start=start, end=end)
    return success({
        "points": points,
        "empty": len(points) == 0,
        "start": start,
        "end": end,
    })


@router.get("/api/daily/prices/{symbol}")
def prices(
    symbol: str,
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    session: Session = Depends(get_session),
    daily: DailyStore = Depends(get_daily_store),
    s: PortfolioStore = Depends(get_portfolio_store),
) -> Any:
    if not _has_portfolio_daily(session):
        return _initializing()
    bad = _check_iso(start, "start") or _check_iso(end, "end")
    if bad is not None:
        return bad
    points = daily.get_ticker_history(symbol, start=start, end=end)

    trades_raw = s.all_trades or []
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

    return success({
        "symbol": symbol,
        "points": points,
        "trades": trades,
        "empty": len(points) == 0,
        "start": start,
        "end": end,
    })
