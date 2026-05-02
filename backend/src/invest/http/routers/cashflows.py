"""GET /api/cashflows/{monthly,cumulative,bank}.

Phase 6 baseline: empty-state envelopes.
  /monthly      list of monthly flow rows (legacy returns a list directly)
  /cumulative   real vs counterfactual curves
  /bank         bank ledger + monthly aggregates

Real values come from analytics.monthly_flows + the bank-derived
cashflow stream in portfolio.json. Both Phase 7+ ports.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/cashflows/monthly")
def monthly(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success([])


@router.get("/api/cashflows/cumulative")
def cumulative(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "real_curve": [],
        "counterfactual_curve": [],
    })


@router.get("/api/cashflows/bank")
def bank(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "ledger": [],
        "monthly": [],
        "totals": {"deposits_twd": 0, "withdrawals_twd": 0, "net_twd": 0},
    })
