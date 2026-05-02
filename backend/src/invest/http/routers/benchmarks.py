"""GET /api/benchmarks/{strategies,compare}.

Phase 6 baseline: empty catalogue + empty compare envelope.
Phase 7 ports the full STRATEGIES list and the yfinance-based
strategy_monthly_returns helpers from app/benchmarks.py — both are
analytics-adjacent (they touch yfinance.py + analytics.py) and belong
in the larger Phase 7 ports of those modules.

The /compare endpoint accepts ?keys=tw_passive,us_passive (legacy
default). On empty store legacy returns {"empty": True}; we preserve.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/benchmarks/strategies")
def list_strategies(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success([])


@router.get("/api/benchmarks/compare")
def compare(
    keys: str = Query(default="tw_passive,us_passive"),
    _session: Session = Depends(get_session),
) -> dict[str, Any]:
    return success({"empty": True, "requested_keys": keys.split(",")})
