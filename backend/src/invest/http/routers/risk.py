"""GET /api/risk - drawdown, concentration, leverage, ratios.

Phase 6 baseline: empty-state envelope. Risk computation reads from
analytics.drawdown_curve / hhi / top_n_share + holdings_today.current_
holdings, all of which are Phase 7+ port targets.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlmodel import Session

from invest.http.deps import get_session
from invest.http.envelope import success

router = APIRouter()


@router.get("/api/risk")
def risk(_session: Session = Depends(get_session)) -> dict[str, Any]:
    return success({
        "drawdown_curve": [],
        "max_drawdown": 0,
        "hhi": 0,
        "top5_share": 0,
        "top10_share": 0,
        "leverage_exposure": {"margin_loan_twd": 0, "equity_twd": 0, "ratio": 0},
        "ratios": {"sharpe": 0, "sortino": 0, "calmar": 0},
    })
