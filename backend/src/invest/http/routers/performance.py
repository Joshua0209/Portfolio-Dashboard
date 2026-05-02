"""GET /api/performance/{timeseries,rolling,attribution}.

Phase 6 baseline: empty-state envelopes + ?method= validation.
The actual TWR / drawdown / Sharpe / Sortino math runs through
analytics.period_returns + analytics.cumulative_curve in legacy.
That module is explicitly Phase 3's port target ("Split analytics.py
(995 lines) into per-metric files" — PLAN section 6) and hasn't
landed yet.

Pinned now:
  - Default method = "day_weighted" (matches /api/summary's choice
    so the two surfaces never disagree on cum_twr)
  - method ∈ {day_weighted, mid_month, eom} — Literal type produces
    a 422 on bogus values without us needing a custom validator.
  - Empty-state monthly[] + zeroed metrics, mirroring the legacy
    `not months` branch.
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


Method = Literal["day_weighted", "mid_month", "eom"]


@router.get("/api/performance/timeseries")
def timeseries(
    method: Method = Query(default="day_weighted"),
    _session: Session = Depends(get_session),
) -> dict[str, Any]:
    return success({
        "empty": True,
        "monthly": [],
        "method": method,
        "twr_total": 0,
        "cagr": 0,
        "xirr": None,
        "max_drawdown": 0,
        "monthly_volatility": 0,
        "annualized_volatility": 0,
        "sharpe_annualized": 0,
        "sortino_annualized": 0,
        "calmar": 0,
        "best_month": None,
        "worst_month": None,
        "positive_months": 0,
        "negative_months": 0,
        "hit_rate": 0,
        "drawdown_episodes": [],
    })


@router.get("/api/performance/rolling")
def rolling(
    method: Method = Query(default="day_weighted"),
    _session: Session = Depends(get_session),
) -> dict[str, Any]:
    return success([])


@router.get("/api/performance/attribution")
def attribution(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "by_venue": {},
        "by_ccy": {},
        "monthly": [],
    })
