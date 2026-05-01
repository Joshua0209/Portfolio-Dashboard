"""GET /api/health - liveness + dataset state.

Same response shape as the legacy /api/health (CLAUDE.md 'API surface'
section) so the existing frontend can switch backends without changes.

State derivation in this Phase 6 baseline:
  months_loaded     count(distinct YYYY-MM) over Trade.date
  as_of             max(Trade.date), null if no rows
  daily_state       READY if PortfolioDaily has rows, else INITIALIZING
                    (FAILED deferred to Phase 7 jobs/state machine)
  daily_last_known  max(PortfolioDaily.date)
  daily_progress    {} placeholder
  daily_error       null placeholder

Phase 7 will wire daily_progress and daily_error from a real backfill
state machine. The keys are reserved here so the response shape is
stable across phases - the frontend already keys on these in the
legacy code.
"""
from __future__ import annotations

from datetime import date as _date
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlmodel import Session, select

from invest.http.deps import get_session
from invest.http.envelope import success
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.trade import Trade


router = APIRouter()


@router.get("/api/health")
def health(session: Session = Depends(get_session)) -> dict[str, Any]:
    return success(_health_payload(session))


def _health_payload(session: Session) -> dict[str, Any]:
    months_loaded = _count_distinct_months(session)
    max_trade_date = session.exec(select(func.max(Trade.date))).one()
    max_pd_date = session.exec(select(func.max(PortfolioDaily.date))).one()

    daily_state = "READY" if max_pd_date is not None else "INITIALIZING"

    return {
        "months_loaded": months_loaded,
        "as_of": _iso(max_trade_date),
        "daily_state": daily_state,
        "daily_last_known": _iso(max_pd_date),
        "daily_progress": {},
        "daily_error": None,
    }


def _count_distinct_months(session: Session) -> int:
    """Distinct YYYY-MM count from Trade.date.

    SQLite's strftime is the most portable way; SQLAlchemy doesn't
    surface a generic year-month extractor.
    """
    stmt = select(func.count(func.distinct(func.strftime("%Y-%m", Trade.date))))
    return int(session.exec(stmt).one() or 0)


def _iso(d: _date | None) -> str | None:
    return d.isoformat() if d else None
