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

import math
import re
from datetime import date as _date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query, status
from fastapi.responses import JSONResponse
from sqlalchemy import func
from sqlmodel import Session, select

from invest.analytics.drawdown import max_drawdown, underwater_curve
from invest.analytics.ratios import sharpe, sortino
from invest.http.deps import (
    get_daily_store,
    get_portfolio_store,
    get_session,
    require_admin,
)
from invest.http.envelope import error, success
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.jobs import retry_failed, snapshot
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.reconcile_repo import ReconcileRepo

_TRADING_DAYS_PER_YEAR = 252
_ONE_DAY = timedelta(days=1)

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


def _parse_iso(d: str) -> _date:
    y, m, dd = (int(p) for p in d.split("-"))
    return _date(y, m, dd)


def _period_anchors(today: _date) -> dict[str, _date]:
    """Cutoff dates for each window: the period boundary immediately
    *before* the period started. Anchor equity is the last row whose
    date is <= cutoff — i.e., the close before the period began.
    """
    qtr_first_month = ((today.month - 1) // 3) * 3 + 1
    return {
        "MTD": _date(today.year, today.month, 1) - _ONE_DAY,
        "QTD": _date(today.year, qtr_first_month, 1) - _ONE_DAY,
        "YTD": _date(today.year, 1, 1) - _ONE_DAY,
    }


def _compute_period_windows(curve: list[dict[str, Any]]) -> list[dict[str, Any]]:
    last = curve[-1]
    last_eq = float(last.get("equity_twd") or 0)
    last_date = _parse_iso(last["date"])
    cutoffs = _period_anchors(last_date)

    windows: list[dict[str, Any]] = []
    for label, cutoff in cutoffs.items():
        anchor = _last_row_on_or_before(curve, cutoff)
        windows.append(_window_dict(label, anchor, last_eq))

    inception_anchor = curve[0]
    windows.append(_window_dict("Inception", inception_anchor, last_eq))
    return windows


def _last_row_on_or_before(
    curve: list[dict[str, Any]], cutoff: _date
) -> dict[str, Any] | None:
    """Return the last row whose date <= cutoff, or None.

    Requires curve to be sorted ascending by date (as returned by
    DailyStore.get_equity_curve). The early break relies on this order —
    a descending or unsorted curve would produce a silently wrong result.
    """
    target = cutoff.isoformat()
    candidate: dict[str, Any] | None = None
    for row in curve:
        if row["date"] <= target:
            candidate = row
        else:
            break
    return candidate


def _window_dict(
    label: str, anchor: dict[str, Any] | None, last_eq: float
) -> dict[str, Any]:
    if anchor is None:
        return {"label": label, "delta_pct": None, "delta_twd": None, "anchor_date": None}
    anchor_eq = float(anchor.get("equity_twd") or 0)
    if anchor_eq <= 0:
        return {
            "label": label,
            "delta_pct": None,
            "delta_twd": last_eq - anchor_eq,
            "anchor_date": anchor["date"],
        }
    return {
        "label": label,
        "delta_pct": ((last_eq / anchor_eq) - 1.0) * 100.0,
        "delta_twd": last_eq - anchor_eq,
        "anchor_date": anchor["date"],
    }


def _daily_returns(curve: list[dict[str, Any]]) -> list[Decimal]:
    """Day-over-day equity returns. Skips rows where the prior equity
    was non-positive (cannot define a return)."""
    out: list[Decimal] = []
    prev: float | None = None
    for row in curve:
        eq = float(row.get("equity_twd") or 0)
        if prev is not None and prev > 0:
            out.append(Decimal(str((eq / prev) - 1.0)))
        prev = eq
    return out


def _compute_risk_metrics(curve: list[dict[str, Any]]) -> dict[str, Any]:
    returns = _daily_returns(curve)
    n = len(returns)
    if n < 2:
        return {"empty": True, "n_days": n}

    floats = [float(r) for r in returns]
    mean = sum(floats) / n
    variance = sum((r - mean) ** 2 for r in floats) / (n - 1)
    stdev = math.sqrt(variance)
    ann_return = (1.0 + mean) ** _TRADING_DAYS_PER_YEAR - 1.0
    ann_vol = stdev * math.sqrt(_TRADING_DAYS_PER_YEAR)

    rolling_vol = None
    if n >= 30:
        recent = floats[-30:]
        rmean = sum(recent) / 30
        rvar = sum((r - rmean) ** 2 for r in recent) / 29
        rolling_vol = math.sqrt(rvar) * math.sqrt(_TRADING_DAYS_PER_YEAR) * 100.0

    equities = [Decimal(str(row.get("equity_twd") or 0)) for row in curve]
    mdd = float(max_drawdown(equities))
    positive = sum(1 for r in floats if r > 0)
    return {
        "ann_return_pct": ann_return * 100.0,
        "ann_vol_pct": ann_vol * 100.0,
        "rolling_30d_vol_pct": rolling_vol,
        "sharpe": float(sharpe(returns, periods_per_year=_TRADING_DAYS_PER_YEAR)),
        "sortino": float(sortino(returns, periods_per_year=_TRADING_DAYS_PER_YEAR)),
        "max_drawdown_pct": mdd * 100.0,
        "hit_rate_pct": (positive / n) * 100.0,
        "best_day_pct": max(floats) * 100.0,
        "worst_day_pct": min(floats) * 100.0,
        "n_days": n,
    }


def _compute_calendar(curve: list[dict[str, Any]]) -> dict[str, Any]:
    cells: list[dict[str, Any]] = []
    months: dict[tuple[int, int], dict[str, Any]] = {}
    prev_eq: float | None = None
    for row in curve:
        eq = float(row.get("equity_twd") or 0)
        if prev_eq is not None and prev_eq > 0:
            ret_pct = ((eq / prev_eq) - 1.0) * 100.0
            cells.append({"date": row["date"], "return_pct": ret_pct})
            d = _parse_iso(row["date"])
            key = (d.year, d.month)
            if key not in months:
                months[key] = {
                    "year": d.year,
                    "month": d.month,
                    "label": f"{d.year}-{d.month:02d}",
                }
        prev_eq = eq
    ordered_months = [months[k] for k in sorted(months.keys())]
    return {"cells": cells, "months": ordered_months}


def _compute_movers(daily) -> list[dict[str, Any]]:
    """Latest two distinct dates from positions_daily — per-symbol
    market-value % change between them.

    TODO: this function bypasses DailyStore's named-method interface and
    issues raw SQL via connect_ro directly. Migrate to a
    DailyStore.get_movers() method so schema changes don't silently break
    this router branch.
    """
    if not hasattr(daily, "connect_ro"):
        return []
    with daily.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date FROM positions_daily ORDER BY date DESC LIMIT 1"
        ).fetchall()
        if not rows:
            return []
        latest = rows[0]["date"]
        prev_row = conn.execute(
            "SELECT date FROM positions_daily WHERE date < ? ORDER BY date DESC LIMIT 1",
            (latest,),
        ).fetchone()
        if not prev_row:
            return []
        prev = prev_row["date"]
        cur_rows = conn.execute(
            "SELECT symbol, mv_twd FROM positions_daily WHERE date = ?",
            (latest,),
        ).fetchall()
        prev_rows = conn.execute(
            "SELECT symbol, mv_twd FROM positions_daily WHERE date = ?",
            (prev,),
        ).fetchall()
    prev_map = {r["symbol"]: r["mv_twd"] for r in prev_rows}
    movers: list[dict[str, Any]] = []
    for r in cur_rows:
        prev_mv = prev_map.get(r["symbol"])
        if prev_mv is None or prev_mv <= 0:
            continue
        delta_pct = ((r["mv_twd"] / prev_mv) - 1.0) * 100.0
        movers.append({"symbol": r["symbol"], "delta_pct": delta_pct})
    movers.sort(key=lambda m: abs(m["delta_pct"]), reverse=True)
    return movers


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
    return success({"movers": _compute_movers(daily)})


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
    curve = daily.get_equity_curve()
    if len(curve) < 2:
        return success({"empty": True, "windows": []})
    return success({"windows": _compute_period_windows(curve)})


@read_router.get("/api/today/drawdown")
def today_drawdown(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    curve = daily.get_equity_curve()
    if len(curve) < 2:
        return success({"empty": True, "points": []})
    equities = [Decimal(str(row.get("equity_twd") or 0)) for row in curve]
    underwater = underwater_curve(equities)
    points = [
        {"date": row["date"], "drawdown_pct": float(dd) * 100.0}
        for row, dd in zip(curve, underwater)
    ]
    return success({"points": points})


@read_router.get("/api/today/risk-metrics")
def today_risk_metrics(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    curve = daily.get_equity_curve()
    if len(curve) < 2:
        return success({"empty": True})
    return success(_compute_risk_metrics(curve))


@read_router.get("/api/today/calendar")
def today_calendar(daily=Depends(get_daily_store)) -> Any:
    if _max_pd_date(daily) is None:
        return _initializing_response()
    curve = daily.get_equity_curve()
    if len(curve) < 2:
        return success({"empty": True, "cells": [], "months": []})
    return success(_compute_calendar(curve))


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
    # Phase 14.2 cutover: route the endpoint through the canonical
    # production path (`invest.jobs.snapshot.run`), which backs
    # `python scripts/snapshot_daily.py`. ``snapshot.run_incremental``
    # remains as a SQLModel-backed scaffold for the future Trade-table
    # aggregator (Phase 14.3+) but is not yet on the request path.
    # Tests monkeypatch `snapshot.run` to avoid network calls.
    summary = snapshot.run(daily, portfolio.raw)
    return success(summary)


@admin_router.post(
    "/api/admin/retry-failed", dependencies=[Depends(require_admin)],
)
def admin_retry_failed(
    session: Session = Depends(get_session),
    daily=Depends(get_daily_store),
) -> dict[str, Any]:
    """Drain the SQLModel ``failed_tasks`` DLQ via ``retry_failed.run``.

    Resolver dispatches per-task_type to the price/fx fetch helpers
    that produced the original DLQ rows. Mirrors the resolver in
    ``scripts/retry_failed_tasks.py``; both routes share the same
    contract: callable fetches AND persists.
    """
    summary = retry_failed.run(session, _build_admin_resolver(daily))
    return success(summary)


def _build_admin_resolver(store):
    """Resolver factory for the admin DLQ drain — same dispatch shape
    as the CLI's ``build_resolver`` (deliberately inlined to avoid a
    cross-cutting helper module while the migration is in flight).
    """
    from invest.jobs import backfill_runner
    from invest.prices import sources as price_sources

    def resolver(task):
        ttype = task.task_type
        target = (task.payload or {}).get("target")
        if not target:
            raise ValueError(
                f"failed_task id={task.id} has no payload['target']"
            )
        floor = store.get_meta("backfill_floor") or "2025-08-01"
        today = store.get_meta("last_known_date") or floor
        if ttype == "tw_prices":
            def _do() -> None:
                rows = price_sources.get_prices(
                    target, "TWD", floor, today, store=store, today=today,
                )
                backfill_runner._persist_symbol_prices(store, target, rows)
            return _do
        if ttype == "foreign_prices":
            def _do() -> None:
                rows = price_sources.get_prices(
                    target, "USD", floor, today, store=store, today=today,
                )
                backfill_runner._persist_symbol_prices(store, target, rows)
            return _do
        if ttype == "fx_rates":
            # Phase 14.3b: route through fx_provider.fetch_and_store_range,
            # mirroring the price_service routing for tw/foreign upstreams.
            def _do() -> None:
                backfill_runner._fetch_range_via_fx_provider(
                    store, target, floor, today,
                )
            return _do
        if ttype == "benchmark_prices":
            def _do() -> None:
                rows = price_sources.get_yfinance_prices(
                    target, floor, today, store=store, today=today,
                )
                ccy = "TWD" if target.endswith((".TW", ".TWO")) else "USD"
                tagged = [
                    {**r, "symbol": target, "currency": ccy, "source": "yfinance"}
                    for r in rows
                ]
                backfill_runner._persist_symbol_prices(store, target, tagged)
            return _do
        raise ValueError(f"unknown task_type: {ttype}")

    return resolver


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
