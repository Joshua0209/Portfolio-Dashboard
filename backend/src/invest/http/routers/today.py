"""GET /api/today/* + POST /api/admin/* — operational surface.

Two APIRouter instances live in this one module file because the
underlying surface is operationally entangled — the Developer Tools
accordion on the legacy /today page surfaces the admin POSTs, and
keeping their definitions adjacent in the codebase mirrors that
operational reality.

  read_router   /api/today/* — gated by daily-state machine, except
                                 /freshness (the staleness probe must
                                 always reply or the /today page can't
                                 render its 'no data yet' branch).
  admin_router  /api/admin/* — POSTs gated by Depends(require_admin);
                                 reads (failed-tasks GET, used by the
                                 banner partial) stay open.

Phase 6 baseline — the daily-state machine
  Legacy require_ready_or_warming inspects backfill_state.get() (the
  in-process state machine) AND daily_store.get_today_snapshot(). The
  state machine isn't ported yet (Phase 7's job), so we synthesize:

    PortfolioDaily empty -> INITIALIZING (HTTP 202)
    PortfolioDaily rows  -> READY (HTTP 200, real-but-stub envelope)
    FAILED                -> deferred until Phase 7

  This is forward-compatible with the eventual machine: once the
  machine ports, the gate just consults it. The shape returned for
  INITIALIZING already matches the legacy {state, progress} envelope.

Reconcile event projection
  Reuses ReconcileRepo.find_open() — the read side of the audit hook
  shipped in Cycle 38. /api/today/reconcile is the read counterpart to
  /api/admin/reconcile/{id}/dismiss, both via the same repo.
"""
from __future__ import annotations

import re
from datetime import date as _date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import Session, select

from invest.http.deps import (
    get_daily_store,
    get_portfolio_store,
    get_session,
    require_admin,
)
from invest.http.envelope import error, success
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.jobs import retry_failed, snapshot_workflow
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.reconcile_repo import ReconcileRepo

read_router = APIRouter()
admin_router = APIRouter()

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _max_pd_date(daily) -> _date | None:
    """Latest date in the daily layer, or None when empty.

    Uses the legacy DailyStore schema (data/dashboard.db has equity_twd,
    not the SQLModel `equity` column). Returns date object or None to
    match the legacy `snapshot is not None` semantics.
    """
    snap = daily.get_today_snapshot()
    if not snap:
        return None
    try:
        y, m, d = (int(p) for p in snap["date"].split("-"))
        return _date(y, m, d)
    except Exception:
        return None


def _initializing_response() -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=success({"state": "INITIALIZING", "progress": {}}),
    )


def _staleness_band(stale_days: int | None) -> str:
    if stale_days is None:
        return "red"
    if stale_days < 1:
        return "green"
    if stale_days < 3:
        return "yellow"
    return "red"


def _today_in_tpe() -> str:
    return datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()


@read_router.get("/api/today/snapshot")
def today_snapshot(daily=Depends(get_daily_store)) -> Any:
    snap = daily.get_today_snapshot()
    if not snap:
        return _initializing_response()
    return success({
        "date": snap["date"],
        "equity_twd": float(snap.get("equity_twd") or 0),
        "fx_usd_twd": snap.get("fx_usd_twd"),
        "n_positions": int(snap.get("n_positions") or 0),
        "has_overlay": bool(snap.get("has_overlay")),
        "delta": None,
    })


@read_router.get("/api/today/movers")
def today_movers(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    return success({"gainers": [], "decliners": []})


@read_router.get("/api/today/sparkline")
def today_sparkline(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    points = daily.get_equity_curve()[-30:] if hasattr(daily, "get_equity_curve") else []
    return success({"points": points})


@read_router.get("/api/today/period-returns")
def today_period_returns(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    return success({"mtd": 0, "qtd": 0, "ytd": 0, "inception": 0})


@read_router.get("/api/today/drawdown")
def today_drawdown(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    return success({"curve": [], "max_drawdown": 0, "current_drawdown": 0})


@read_router.get("/api/today/risk-metrics")
def today_risk_metrics(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    return success({
        "annualized_return": 0,
        "annualized_volatility": 0,
        "sharpe": 0,
        "sortino": 0,
        "hit_rate": 0,
    })


@read_router.get("/api/today/calendar")
def today_calendar(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    return success({"days": []})


@read_router.get("/api/today/freshness")
def today_freshness(daily=Depends(get_daily_store)) -> dict[str, Any]:
    last = _max_pd_date(daily)
    today_tpe = _today_in_tpe()
    if last is None:
        return success({
            "data_date": None,
            "today_in_tpe": today_tpe,
            "stale_days": None,
            "band": "red",
        })
    y, m, d = (int(p) for p in today_tpe.split("-"))
    stale = (_date(y, m, d) - last).days
    return success({
        "data_date": last.isoformat(),
        "today_in_tpe": today_tpe,
        "stale_days": stale,
        "band": _staleness_band(stale),
    })


@read_router.get("/api/today/reconcile")
def today_reconcile(session: Session = Depends(get_session)) -> dict[str, Any]:
    repo = ReconcileRepo(session)
    events = repo.find_open()
    out = []
    for e in events:
        detail = e.detail or {}
        out.append({
            "id": e.id,
            "pdf_month": e.pdf_month,
            "event_type": e.event_type,
            "detected_at": e.detected_at.isoformat() if e.detected_at else None,
            "code": detail.get("code"),
            "sdk_leg_count": detail.get("sdk_leg_count"),
            "pdf_trade_count": detail.get("pdf_trade_count"),
        })
    return success({"events": out, "count": len(out)})


@admin_router.get("/api/admin/failed-tasks")
def admin_failed_tasks(session: Session = Depends(get_session)) -> dict[str, Any]:
    repo = FailedTaskRepo(session)
    open_tasks = repo.find_open()
    serialized = [
        {
            "id": t.id,
            "task_type": t.task_type,
            "payload": t.payload,
            "error": t.error,
            "attempts": t.attempts,
            "first_failed_at": t.first_failed_at.isoformat(),
            "last_failed_at": t.last_failed_at.isoformat(),
        }
        for t in open_tasks
    ]
    return success({"tasks": serialized, "count": len(serialized)})


@admin_router.post(
    "/api/admin/refresh", dependencies=[Depends(require_admin)],
)
def admin_refresh(
    daily=Depends(get_daily_store),
    portfolio=Depends(get_portfolio_store),
) -> dict[str, Any]:
    # Phase 11 cutover: route the endpoint through the canonical
    # production path (`invest.jobs.snapshot_workflow.run`), which
    # backs `python scripts/snapshot_daily.py`. The previously wired
    # `snapshot.run_incremental` was a SQLModel-backed scaffold for
    # the future Trade-table aggregator and never executed real
    # fetches. Tests monkeypatch `snapshot_workflow.run` to avoid
    # network calls.
    summary = snapshot_workflow.run(daily, portfolio.raw)
    return success(summary)


@admin_router.post(
    "/api/admin/retry-failed", dependencies=[Depends(require_admin)],
)
def admin_retry_failed(session: Session = Depends(get_session)) -> dict[str, Any]:
    # Resolver is intentionally a "nothing to retry" no-op until the
    # Phase 2 services are wired into a real per-task resolver. This
    # keeps the envelope honest: if the DLQ is empty, both counters
    # are 0; if it has rows, every row will count as still_failing
    # (no false positives for "resolved").
    def not_yet_wired_resolver(task):
        def _retry():
            raise NotImplementedError(
                "admin retry resolver not yet wired up — run "
                "scripts/retry_failed.py with a real PriceClient/"
                "FxClient instead"
            )
        return _retry

    summary = retry_failed.run(session, not_yet_wired_resolver)
    return success(summary)


@admin_router.post(
    "/api/admin/reconcile", dependencies=[Depends(require_admin)],
)
def admin_reconcile(
    month: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> Any:
    if not month or not _MONTH_RE.match(month):
        return JSONResponse(
            status_code=400, content=error("month must be YYYY-MM"),
        )
    return success({
        "month": month,
        "events_created": 0,
        "events_dismissed": 0,
    })


@admin_router.post(
    "/api/admin/reconcile/{event_id}/dismiss",
    dependencies=[Depends(require_admin)],
)
def admin_reconcile_dismiss(
    event_id: int, session: Session = Depends(get_session),
) -> dict[str, Any]:
    repo = ReconcileRepo(session)
    target = repo.find_by_id(event_id)
    if target is None:
        return success({"dismissed": False, "event_id": event_id})
    repo.dismiss(event_id)
    return success({"dismissed": True, "event_id": event_id})
