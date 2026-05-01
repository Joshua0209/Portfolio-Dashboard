"""GET /api/fx - FX exposure + P&L attribution.

Phase 6 baseline: empty-state envelope mirroring the legacy
`not s.months` branch in app/api/fx.py. Daily-resolution branch is
honored (?resolution=daily silently ignored when daily store is empty,
same as legacy).
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/fx")
def fx(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "empty": True,
        "rate_curve": [],
        "current_rate": None,
        "first_rate": None,
        "by_ccy_monthly": [],
        "fx_pnl": {"contribution_twd": 0, "monthly": []},
        "foreign_share": 0,
        "foreign_value_twd": 0,
    })
