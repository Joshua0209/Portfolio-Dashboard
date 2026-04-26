"""/today page + admin endpoints (Phases 10, 12, 13).

Phase 10 ships:
  GET  /api/admin/failed-tasks  → list open DLQ rows
  POST /api/admin/retry-failed  → retry every open DLQ row, summarize result

Phase 12 ships:
  POST /api/admin/reconcile?month=YYYY-MM  → manual reconciliation run
  POST /api/admin/reconcile/<id>/dismiss   → hide one open event
  GET  /api/today/reconcile                → list open events for the global banner

The /today blueprint is mounted now so that Phases 12 (reconciliation)
and 13 (the page itself) can extend the same module instead of moving
endpoints around. Per spec §6.4, all admin actions live behind a
collapsible Developer Tools accordion on /today; the URL namespace
(`/api/admin/*`) is reserved so a future dedicated /admin page can lift
the endpoints without renaming.
"""
from __future__ import annotations

import json
import re

from flask import Blueprint, current_app, request

from .. import backfill_runner
from ._helpers import envelope, store as portfolio_store

bp = Blueprint("today", __name__)

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _store():
    return current_app.extensions["daily_store"]


# --- DLQ read endpoint ---------------------------------------------------


@bp.get("/api/admin/failed-tasks")
def failed_tasks():
    tasks = _store().get_failed_tasks()
    return envelope({"tasks": tasks, "count": len(tasks)})


# --- DLQ retry dispatcher ------------------------------------------------


def _build_retry_resolver():
    """Map a failed_tasks row → callable that re-runs the original fetch.

    Lives inside the request scope so each task_type's retry is wired to
    the live fetch helpers (and any test-time monkeypatching applies).
    The resolver is intentionally permissive: unknown task_types raise
    so a future task_type added without a retry plan fails loudly.
    """
    # Lazy imports so the price_sources module is captured per-request,
    # which means monkeypatch.setattr(app.price_sources, ...) in tests
    # is observed by the retry path.
    from .. import price_sources

    store = _store()

    def resolver(row):
        ttype = row["task_type"]
        target = row["target"]
        if ttype == "tw_prices":
            # Phase 10's retry policy: re-fetch the broad window. The DLQ
            # row doesn't persist start/end (they're derivable), so a
            # bounded retry over the BACKFILL_FLOOR..today envelope is
            # the conservative choice — duplicate UPSERTs are cheap.
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_prices(
                target, "TWD", floor, today, store=store
            )
        if ttype == "foreign_prices":
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_prices(
                target, "USD", floor, today, store=store
            )
        if ttype == "fx_rates":
            floor = store.get_meta("backfill_floor") or "2025-08-01"
            today = store.get_meta("last_known_date") or floor
            return lambda: price_sources.get_fx_rates(target, floor, today)
        raise ValueError(f"unknown task_type: {ttype}")

    return resolver


@bp.post("/api/admin/retry-failed")
def retry_failed():
    summary = backfill_runner.retry_open_tasks(_store(), _build_retry_resolver())
    return envelope(summary)


# --- Phase 12: reconciliation -------------------------------------------


def _build_overlay_client():
    """Return a (start, end) → list[trade] callable, or None when Shioaji
    is unconfigured. Lives in request scope so monkeypatched ShioajiClient
    instances in tests are observed.
    """
    from .. import shioaji_client

    client = shioaji_client.ShioajiClient()
    if not client.configured:
        return None
    return client.list_trades


@bp.post("/api/admin/reconcile")
def reconcile_run():
    """Manually trigger reconciliation for one PDF month.

    Spec §12: never auto-fired. The button on /today and the CLI
    (scripts/reconcile.py) both land here.
    """
    month = request.args.get("month") or ""
    if not _MONTH_RE.match(month):
        return {"ok": False, "error": "month must be YYYY-MM"}, 400

    from .. import reconcile as reconcile_mod

    pdf = portfolio_store().raw
    overlay_fn = _build_overlay_client()
    summary = reconcile_mod.run_for_month(
        _store(), pdf, month, overlay_client=overlay_fn
    )
    return envelope(summary)


@bp.post("/api/admin/reconcile/<int:event_id>/dismiss")
def reconcile_dismiss(event_id: int):
    from .. import reconcile as reconcile_mod
    dismissed = reconcile_mod.dismiss_event(_store(), event_id)
    return envelope({"dismissed": dismissed, "event_id": event_id})


# --- Phase 13: /today data endpoints ------------------------------------


def _zoneinfo_today() -> str:
    """ISO date 'YYYY-MM-DD' for "now" in Asia/Taipei."""
    from datetime import datetime
    from zoneinfo import ZoneInfo

    return datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()


def _weekday_name(iso_date: str) -> str:
    from datetime import date as _date

    y, m, d = (int(p) for p in iso_date.split("-"))
    return _date(y, m, d).strftime("%A")


def _staleness_band(stale_days: int) -> str:
    """Plan §3 Phase 14: green <1d / yellow <3d / red >=3d (or no data)."""
    if stale_days < 1:
        return "green"
    if stale_days < 3:
        return "yellow"
    return "red"


@bp.get("/api/today/snapshot")
def today_snapshot():
    """Latest equity row + delta vs the immediately prior trading day.

    Used by the /today hero. Empty envelope when portfolio_daily is empty.
    """
    ds = _store()
    with ds.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date, equity_twd, fx_usd_twd, n_positions, has_overlay "
            "FROM portfolio_daily ORDER BY date DESC LIMIT 2"
        ).fetchall()
    if not rows:
        return envelope({"empty": True})
    latest = dict(rows[0])
    prev = dict(rows[1]) if len(rows) > 1 else None
    delta_twd = (latest["equity_twd"] - prev["equity_twd"]) if prev else 0.0
    delta_pct = (
        (delta_twd / prev["equity_twd"] * 100.0)
        if prev and prev["equity_twd"]
        else 0.0
    )
    return envelope({
        "data_date": latest["date"],
        "weekday": _weekday_name(latest["date"]),
        "today_in_tpe": _zoneinfo_today(),
        "equity_twd": latest["equity_twd"],
        "delta_twd": delta_twd,
        "delta_pct": delta_pct,
        "n_positions": latest["n_positions"],
        "has_overlay": bool(latest["has_overlay"]),
        "fx_usd_twd": latest["fx_usd_twd"],
        "empty": False,
    })


@bp.get("/api/today/movers")
def today_movers():
    """Top movers from positions_daily — for each symbol, % delta between
    the two most recent dates. Returns up to N gainers/losers in a single
    flat list sorted by |delta_pct| desc (frontend filters into two
    columns)."""
    ds = _store()
    with ds.connect_ro() as conn:
        dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT date FROM positions_daily ORDER BY date DESC LIMIT 2"
        ).fetchall()]
        if len(dates) < 2:
            return envelope({"movers": [], "data_date": dates[0] if dates else None})
        latest, prior = dates[0], dates[1]
        rows = conn.execute(
            "SELECT a.symbol, a.mv_twd AS mv_now, b.mv_twd AS mv_prev "
            "FROM positions_daily a JOIN positions_daily b "
            "  ON a.symbol = b.symbol AND a.date = ? AND b.date = ?",
            (latest, prior),
        ).fetchall()
    movers = []
    for r in rows:
        prev = r["mv_prev"]
        if prev <= 0:
            continue
        delta = r["mv_now"] - prev
        movers.append({
            "symbol": r["symbol"],
            "mv_now": r["mv_now"],
            "delta_twd": delta,
            "delta_pct": delta / prev * 100.0,
        })
    movers.sort(key=lambda m: abs(m["delta_pct"]), reverse=True)
    return envelope({"movers": movers, "data_date": latest})


@bp.get("/api/today/sparkline")
def today_sparkline():
    """Last 30 trading days of equity_twd for the hero sparkline."""
    ds = _store()
    with ds.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date, equity_twd FROM portfolio_daily "
            "ORDER BY date DESC LIMIT 30"
        ).fetchall()
    points = [dict(r) for r in reversed(rows)]
    return envelope({"points": points})


@bp.get("/api/today/period-returns")
def today_period_returns():
    """MTD / QTD / YTD / inception equity returns for the hero strip.

    Pure-derive from portfolio_daily — no extra fetches. For each window,
    return the % change between the first equity row in the window and
    the latest, plus the absolute TWD delta. Empty envelope when there's
    no data."""
    from datetime import date as _date

    ds = _store()
    with ds.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT date, equity_twd FROM portfolio_daily ORDER BY date ASC"
        ).fetchall()]
    if not rows:
        return envelope({"empty": True})

    latest = rows[-1]
    latest_date = latest["date"]
    latest_eq = latest["equity_twd"]
    y, m, _ = (int(p) for p in latest_date.split("-"))
    qstart_month = ((m - 1) // 3) * 3 + 1

    def first_on_or_after(iso_start: str):
        for r in rows:
            if r["date"] >= iso_start:
                return r
        return None

    def window(label: str, iso_start: str):
        anchor = first_on_or_after(iso_start)
        if anchor is None or not anchor["equity_twd"]:
            return {"label": label, "delta_pct": None, "delta_twd": None,
                    "anchor_date": None, "anchor_equity": None}
        d = latest_eq - anchor["equity_twd"]
        p = d / anchor["equity_twd"] * 100.0
        return {"label": label, "delta_pct": p, "delta_twd": d,
                "anchor_date": anchor["date"],
                "anchor_equity": anchor["equity_twd"]}

    return envelope({
        "latest_date": latest_date,
        "latest_equity_twd": latest_eq,
        "windows": [
            window("MTD", f"{y:04d}-{m:02d}-01"),
            window("QTD", f"{y:04d}-{qstart_month:02d}-01"),
            window("YTD", f"{y:04d}-01-01"),
            window("Inception", rows[0]["date"]),
        ],
    })


@bp.get("/api/today/drawdown")
def today_drawdown():
    """Underwater equity curve — % below running all-time-high.

    Returns: {points: [{date, drawdown_pct}], current_dd, max_dd,
              max_dd_date, peak_date}
    Empty envelope when portfolio_daily has no rows.
    """
    ds = _store()
    with ds.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT date, equity_twd FROM portfolio_daily "
            "WHERE equity_twd > 0 ORDER BY date ASC"
        ).fetchall()]
    if not rows:
        return envelope({"empty": True, "points": []})

    points = []
    peak = 0.0
    peak_date = rows[0]["date"]
    max_dd = 0.0
    max_dd_date = rows[0]["date"]
    max_dd_peak = rows[0]["date"]
    for r in rows:
        eq = r["equity_twd"]
        if eq > peak:
            peak = eq
            peak_date = r["date"]
        dd = (eq / peak - 1.0) * 100.0 if peak else 0.0
        points.append({"date": r["date"], "drawdown_pct": dd})
        if dd < max_dd:
            max_dd = dd
            max_dd_date = r["date"]
            max_dd_peak = peak_date
    current_dd = points[-1]["drawdown_pct"] if points else 0.0
    return envelope({
        "empty": False,
        "points": points,
        "current_dd": current_dd,
        "max_dd": max_dd,
        "max_dd_date": max_dd_date,
        "max_dd_peak_date": max_dd_peak,
        "current_peak_date": peak_date,
    })


@bp.get("/api/today/risk-metrics")
def today_risk_metrics():
    """Daily-resolution risk metrics: rolling 30d annualized vol,
    Sharpe (rf=0), Sortino, hit rate, max DD. All derived from
    portfolio_daily; no external calls.

    Returns nulls when there are too few rows for a metric (need ≥21
    daily-return observations for the rolling window).
    """
    import math

    ds = _store()
    with ds.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT date, equity_twd FROM portfolio_daily "
            "WHERE equity_twd > 0 ORDER BY date ASC"
        ).fetchall()]
    if len(rows) < 2:
        return envelope({"empty": True})

    rets: list[float] = []
    for prev, cur in zip(rows, rows[1:]):
        if prev["equity_twd"] > 0:
            rets.append(cur["equity_twd"] / prev["equity_twd"] - 1.0)

    n = len(rets)
    if n == 0:
        return envelope({"empty": True})

    mean = sum(rets) / n
    var = sum((r - mean) ** 2 for r in rets) / n if n else 0.0
    std = math.sqrt(var)
    downside = [r for r in rets if r < 0]
    dvar = sum((r - 0) ** 2 for r in downside) / len(downside) if downside else 0.0
    dstd = math.sqrt(dvar)

    ann_factor = math.sqrt(252)
    ann_return = (1 + mean) ** 252 - 1 if mean is not None else None
    ann_vol = std * ann_factor
    sharpe = (ann_return / ann_vol) if ann_vol > 0 else None
    sortino = ((ann_return) / (dstd * ann_factor)) if dstd > 0 else None

    hit_rate = sum(1 for r in rets if r > 0) / n if n else None
    best = max(rets) * 100.0 if rets else None
    worst = min(rets) * 100.0 if rets else None

    # 30-day rolling window stats (last 30 returns; fewer if not enough)
    window = rets[-30:] if len(rets) >= 30 else rets
    w_mean = sum(window) / len(window) if window else 0.0
    w_var = sum((r - w_mean) ** 2 for r in window) / len(window) if window else 0.0
    w_std = math.sqrt(w_var)
    rolling_vol = w_std * ann_factor

    # Max drawdown from the same series we already used in /drawdown
    peak = 0.0
    max_dd = 0.0
    for r in rows:
        eq = r["equity_twd"]
        if eq > peak:
            peak = eq
        dd = (eq / peak - 1.0) * 100.0 if peak else 0.0
        if dd < max_dd:
            max_dd = dd

    return envelope({
        "empty": False,
        "n_days": n,
        "ann_return_pct": (ann_return * 100.0) if ann_return is not None else None,
        "ann_vol_pct": ann_vol * 100.0,
        "rolling_30d_vol_pct": rolling_vol * 100.0,
        "sharpe": sharpe,
        "sortino": sortino,
        "hit_rate_pct": hit_rate * 100.0 if hit_rate is not None else None,
        "best_day_pct": best,
        "worst_day_pct": worst,
        "max_drawdown_pct": max_dd,
    })


@bp.get("/api/today/calendar")
def today_calendar():
    """Per-trading-day return (% delta of equity_twd vs prior priced day),
    with calendar metadata for heatmap rendering.

    Returns: {cells: [{date, year, month, dom, weekday, return_pct}],
              months: [{year, month, label}]}
    where months is the ordered de-duplicated list of (year, month) pairs
    appearing in cells.
    """
    from datetime import date as _date

    ds = _store()
    with ds.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT date, equity_twd FROM portfolio_daily "
            "WHERE equity_twd > 0 ORDER BY date ASC"
        ).fetchall()]
    if len(rows) < 2:
        return envelope({"empty": True, "cells": [], "months": []})

    cells = []
    seen_months: list[tuple[int, int]] = []
    for prev, cur in zip(rows, rows[1:]):
        ret = (cur["equity_twd"] / prev["equity_twd"] - 1.0) * 100.0
        y, m, d = (int(p) for p in cur["date"].split("-"))
        weekday = _date(y, m, d).weekday()  # 0=Mon
        cells.append({
            "date": cur["date"], "year": y, "month": m, "dom": d,
            "weekday": weekday, "return_pct": ret,
        })
        if (y, m) not in seen_months:
            seen_months.append((y, m))

    month_labels = [
        {"year": y, "month": m,
         "label": _date(y, m, 1).strftime("%b %Y")}
        for (y, m) in seen_months
    ]
    return envelope({"empty": False, "cells": cells, "months": month_labels})


@bp.get("/api/today/freshness")
def today_freshness():
    """Global freshness widget endpoint (Phase 14 component, shipped here
    so /today's hero can render the same data)."""
    ds = _store()
    snap = ds.get_today_snapshot()
    today_tpe = _zoneinfo_today()
    if snap is None:
        return envelope({
            "data_date": None,
            "today_in_tpe": today_tpe,
            "stale_days": None,
            "band": "red",
        })
    from datetime import date as _date

    y1, m1, d1 = (int(p) for p in snap["date"].split("-"))
    y2, m2, d2 = (int(p) for p in today_tpe.split("-"))
    stale_days = (_date(y2, m2, d2) - _date(y1, m1, d1)).days
    return envelope({
        "data_date": snap["date"],
        "today_in_tpe": today_tpe,
        "stale_days": stale_days,
        "band": _staleness_band(stale_days),
    })


# --- Phase 13/15: refresh endpoint --------------------------------------


def _run_snapshot(store, portfolio):
    """Default impl — Phase 15 replaces this with snapshot_daily.run.

    Defined here as a module-level seam so tests can monkeypatch it
    before Phase 15 lands and so the import of snapshot_daily doesn't
    eagerly trigger any side effects at app startup.
    """
    try:
        from scripts import snapshot_daily as snap_mod
        return snap_mod.run(store, portfolio)
    except ImportError:
        return {"new_dates": 0, "new_rows": 0, "skipped_reason": "snapshot_daily_not_installed"}


@bp.post("/api/admin/refresh")
def admin_refresh():
    """Synchronous refresh — runs snapshot_daily, returns its summary.

    Phase 13 ships the endpoint plus a no-op fallback (`_run_snapshot`
    above) so the UI button works even before Phase 15 lands. Once
    Phase 15 lands the seam picks up the real implementation.
    """
    pdf = portfolio_store().raw
    summary = _run_snapshot(_store(), pdf)
    return envelope(summary)


@bp.get("/api/today/reconcile")
def reconcile_open_events():
    """Polled by the global banner partial. Returns a compact shape so
    every-page-load doesn't drag the full diff payload over the wire."""
    from .. import reconcile as reconcile_mod

    events = reconcile_mod.get_open_events(_store())
    out = []
    for e in events:
        try:
            payload = json.loads(e["diff_summary"]) if e["diff_summary"] else {}
        except json.JSONDecodeError:
            payload = {}
        out.append({
            "id": e["id"],
            "pdf_month": e["pdf_month"],
            "detected_at": e["detected_at"],
            "only_in_pdf_count": payload.get("only_in_pdf_count", 0),
            "only_in_overlay_count": payload.get("only_in_overlay_count", 0),
        })
    return envelope({"events": out, "count": len(out)})
