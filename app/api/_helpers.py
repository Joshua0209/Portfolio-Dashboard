"""Shared helpers for API blueprints."""
from __future__ import annotations

from functools import wraps

from flask import current_app, request

from .. import analytics, backfill_state
from ..data_store import DataStore


def store() -> DataStore:
    return current_app.extensions["store"]


def daily_store():
    return current_app.extensions["daily_store"]


def want_daily() -> bool:
    """True when the request asked for the daily-resolution branch."""
    return (request.args.get("resolution") or "").lower() == "daily"


def bank_cash_twd(month: dict) -> float:
    """TWD-equivalent bank cash from one monthly record (TWD + USD legs)."""
    return (month.get("bank_twd", 0) or 0) + (month.get("bank_usd_in_twd", 0) or 0)


def reprice_holdings_today(rows: list[dict], fallback_fx: float | None = None):
    """Reprice rows against today's daily-store closes.

    Returns the repriced rows, or `None` if the daily store has no
    snapshot yet — callers fall back to month-end values.

    Holdings without a daily price (delisted, thin volume) keep their
    month-end values, per `analytics.reprice_holdings_with_daily`.
    Closes are fetched in one batched query, not one-per-symbol.
    """
    daily = daily_store()
    snap = daily.get_today_snapshot()
    if not snap:
        return None
    fx_today = snap.get("fx_usd_twd") or fallback_fx
    codes = [r.get("code") for r in rows if r.get("code")]
    closes = daily.get_latest_closes(codes) if codes else {}
    return analytics.reprice_holdings_with_daily(
        rows, lambda c: closes.get(c), current_fx_usd_twd=fx_today
    )


def today_repriced_totals(months) -> tuple[float | None, float | None, int]:
    """Total MV + unrealized P&L using today's daily-store closes.

    Returns (mv_twd, unrealized_twd, n_repriced). Returns
    (None, None, 0) when the daily store is empty so callers can fall
    back to the month-end value.
    """
    if not months:
        return None, None, 0
    # Local import to avoid a circular: holdings -> _helpers -> holdings
    from .holdings import _holdings_for_month

    last = months[-1]
    rows = reprice_holdings_today(
        _holdings_for_month(last), fallback_fx=last.get("fx_usd_twd")
    )
    if rows is None:
        return None, None, 0
    mv = sum(h.get("mkt_value_twd", 0) for h in rows)
    upnl = sum(h.get("unrealized_pnl_twd", 0) for h in rows)
    n_repriced = sum(1 for h in rows if h.get("repriced_at"))
    return mv, upnl, n_repriced


def envelope(data, **meta):
    """Consistent JSON envelope: { ok, data, ...meta }."""
    body = {"ok": True, "data": data}
    if meta:
        body["meta"] = meta
    return body


def require_ready_or_warming(handler):
    """Decorator for endpoints that depend on the daily SQLite layer.

    INITIALIZING → 202 + progress envelope (frontend renders spinner).
    FAILED       → 503 + error envelope (frontend deep-links to the
                   Developer Tools accordion).
    READY        → call the wrapped handler normally.

    READY + empty store is treated as INITIALIZING — mirrors the
    /api/health contract in app/__init__.py: rows are required before
    callers should see "ready". Without this, an endpoint decorated to
    require ready would happily return an empty body at HTTP 200 during
    a fresh boot when the daemon hasn't written any rows yet, hiding a
    real "still warming up" state behind silent-empty responses.
    """

    @wraps(handler)
    def wrapper(*args, **kwargs):
        snap = backfill_state.get().snapshot()
        if snap["state"] == "FAILED":
            return (
                {
                    "ok": False,
                    "error": snap["error"] or "backfill failed",
                    "data": {
                        "state": "FAILED",
                        "progress": snap["progress"],
                    },
                },
                503,
            )
        if snap["state"] == "READY":
            ds_snap = daily_store().get_today_snapshot()
            if ds_snap is not None:
                return handler(*args, **kwargs)
        # INITIALIZING, or READY-but-empty
        return (
            {
                "ok": True,
                "data": {
                    "state": "INITIALIZING",
                    "progress": snap["progress"],
                },
            },
            202,
        )

    return wrapper
