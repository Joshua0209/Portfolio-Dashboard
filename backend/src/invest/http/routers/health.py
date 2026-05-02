"""GET /api/health — liveness + dataset state.

Phase 6.5 wiring: reads from PortfolioStore (months_loaded, as_of) and
DailyStore (daily_last_known via meta). Same envelope shape as the
legacy /api/health so the frontend keys carry forward unchanged.

  months_loaded     len(PortfolioStore.months)
  as_of             PortfolioStore.kpis['as_of'] (parsed-PDF cutoff)
  daily_state       READY if DailyStore.last_known_date is set,
                    else INITIALIZING
  daily_last_known  DailyStore.get_meta('last_known_date')
  daily_progress    {} placeholder (Phase 7 backfill state machine)
  daily_error       null placeholder

Why not SQLModel queries: Phase 6.5 routers all read from PortfolioStore
(JSON aggregate) and the legacy DailyStore (raw SQL on the SQLite cache).
The new SQLModel ORM tables sit alongside the legacy schema in the same
DB but use different column names and aren't used on the request path
yet. Phase 10+ migrates to Trade-table-derived aggregates and the
SQLModel queries become live.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.envelope import success
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


@router.get("/api/health")
def health(
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> dict[str, Any]:
    months_loaded = len(s.months)
    as_of = s.as_of

    try:
        daily_last_known = daily.get_meta("last_known_date")
    except Exception:
        daily_last_known = None

    daily_state = "READY" if daily_last_known else "INITIALIZING"

    return success({
        "months_loaded": months_loaded,
        "as_of": as_of,
        "daily_state": daily_state,
        "daily_last_known": daily_last_known,
        "daily_progress": {},
        "daily_error": None,
    })
