"""PDF-vs-Shioaji monthly verifier — adapter for the existing
invest.ingestion.trade_verifier.

Operator-triggered (POST /api/admin/reconcile, scripts/verify_month.py).
PDF discovery is the caller's responsibility — this module takes
already-parsed Securities/Foreign statements and runs the diff against
Trade rows for the affected months.

Same posture as snapshot.run_incremental: synchronous, returns a
summary envelope, no state-machine involvement (operator-triggered
work, not cold-start lifecycle).
"""
from __future__ import annotations

import re
from typing import Any

from sqlmodel import Session

from invest.ingestion import trade_verifier
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def run(
    session: Session,
    *,
    month: str,
    securities: list,
    foreign: list,
    apply: bool = False,
) -> dict[str, Any]:
    """Verify parsed PDF statements against Trade rows for `month`.

    Returns {month, matched, pdf_only, shioaji_only, events_inserted,
    applied}. Raises ValueError on malformed `month` (must be YYYY-MM).
    """
    if not _MONTH_RE.match(month):
        raise ValueError(f"month must be YYYY-MM, got {month!r}")

    trade_repo = TradeRepo(session)
    reconcile_repo = ReconcileRepo(session)

    result = trade_verifier.verify_trades_against_statements(
        securities=securities,
        foreign=foreign,
        trade_repo=trade_repo,
        reconcile_repo=reconcile_repo,
        apply=apply,
    )

    return {
        "month": month,
        "matched": result.diff.matched,
        "pdf_only": len(result.diff.pdf_only),
        "shioaji_only": len(result.diff.shioaji_only),
        "events_inserted": result.events_inserted,
        "applied": apply,
    }
