"""GET /api/summary - top-level dashboard payload.

Phase 6 baseline: returns the legacy empty-state envelope unconditionally.
KPIs, equity curve, and allocation come online when the analytics layer
ports in Phase 7 (which builds month aggregates from Trade rows). Until
then the response shape is the same `not s.months` branch the legacy
endpoint already returns when portfolio.json is empty — frontend's
chart-empty rendering still fires.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/summary")
def summary(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success(_empty_summary())


def _empty_summary() -> dict[str, Any]:
    return {
        "empty": True,
        "kpis": {},
        "twr": 0,
        "xirr": None,
        "profit_twd": 0,
        "invested_twd": 0,
        "equity_curve": [],
        "allocation": {"tw": 0, "foreign": 0, "bank_twd": 0, "bank_usd": 0},
        "first_month": None,
        "last_month": None,
        "months_covered": 0,
    }
