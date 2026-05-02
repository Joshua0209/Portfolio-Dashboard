"""Incremental snapshot — gap-fill from last_known_date to today.

Operator-triggered (POST /api/admin/refresh, scripts/snapshot.py).
Synchronous; no daily-state machine involvement (snapshot runs
against an already-warm layer; the state machine is about cold-
start lifecycle only).

Skip semantics:
  - last_known_date is None    -> "no_prior_data_call_backfill"
  - last_known_date >= today   -> "already_up_to_date"
  - otherwise                  -> fetch + build_daily over
                                  (last_known + 1, today)
"""
from __future__ import annotations

import logging
from datetime import date as _date, timedelta
from typing import Any, Callable, Optional

from sqlmodel import Session, desc, select

from invest.jobs import _positions
from invest.persistence.models.portfolio_daily import PortfolioDaily

log = logging.getLogger(__name__)

FetchOrchestrator = Callable[[Session, _date, _date], None]


def find_last_known_date(session: Session) -> Optional[_date]:
    stmt = (
        select(PortfolioDaily)
        .order_by(desc(PortfolioDaily.date))
        .limit(1)
    )
    row = session.exec(stmt).first()
    return row.date if row else None


def run_incremental(
    session: Session,
    *,
    today: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> dict[str, Any]:
    last = find_last_known_date(session)
    if last is None:
        return {
            "skipped_reason": "no_prior_data_call_backfill",
            "positions_rows": 0,
            "portfolio_rows": 0,
        }
    if last >= today:
        return {
            "skipped_reason": "already_up_to_date",
            "positions_rows": 0,
            "portfolio_rows": 0,
            "last_known_date": last.isoformat(),
        }

    gap_start = last + timedelta(days=1)
    fetch_orchestrator(session, gap_start, today)
    result = _positions.build_daily(session, gap_start, today)
    return {
        "skipped_reason": None,
        "gap_start": gap_start.isoformat(),
        "gap_end": today.isoformat(),
        **result,
    }
