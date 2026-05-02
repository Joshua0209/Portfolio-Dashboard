"""Shared helpers for routers — bank cashflows, today-repricing, daily branch.

Port of legacy app/api/_helpers.py adapted for FastAPI's Query/Depends
model. Pure functions take portfolio_store + daily_store as parameters
(no Flask globals); FastAPI's Query dependency replaces request.args.
"""
from __future__ import annotations

from typing import Any

from invest.analytics import monthly as analytics
from invest.analytics.holdings_today import current_holdings


def bank_cash_twd(month: dict) -> float:
    """TWD-equivalent bank cash from one monthly record (TWD + USD legs)."""
    return (month.get("bank_twd", 0) or 0) + (month.get("bank_usd_in_twd", 0) or 0)


def reprice_holdings_today(
    rows: list[dict],
    daily_store,
    fallback_fx: float | None = None,
) -> list[dict] | None:
    """Reprice rows against today's daily-store closes.

    Returns the repriced rows, or None if the daily store has no
    snapshot yet — callers fall back to month-end values.
    """
    snap = daily_store.get_today_snapshot()
    if not snap:
        return None
    fx_today = snap.get("fx_usd_twd") or fallback_fx
    codes = [r.get("code") for r in rows if r.get("code")]
    closes = daily_store.get_latest_closes(codes) if codes else {}
    return analytics.reprice_holdings_with_daily(
        rows, lambda c: closes.get(c), current_fx_usd_twd=fx_today
    )


def today_repriced_totals(
    months: list[dict],
    portfolio_store,
    daily_store,
) -> tuple[float | None, float | None, int]:
    """Total MV + unrealized P&L for today, sourced from current_holdings().

    See app/holdings_today.py docstring for warm/cold resolution rules.
    Returns (None, None, 0) only when there is no portfolio at all
    (months is empty). Empty rows from current_holdings() is a valid
    warm answer (user holds nothing today) and returns (0, 0, 0).
    """
    if not months:
        return None, None, 0
    rows = current_holdings(portfolio_store, daily_store)
    mv = sum(r.get("mkt_value_twd", 0) for r in rows)
    upnl = sum(r.get("unrealized_pnl_twd", 0) for r in rows)
    n_repriced = sum(1 for r in rows if r.get("repriced_at"))
    return mv, upnl, n_repriced


def envelope(data: Any, **meta) -> dict[str, Any]:
    """Consistent JSON envelope: {ok, data, ...meta}.

    Mirrors legacy app/api/_helpers.py:envelope. New routers will
    eventually consolidate to invest.http.envelope.success(); this
    name is kept for ergonomic parity during the port.
    """
    body: dict[str, Any] = {"ok": True, "data": data}
    if meta:
        body["meta"] = meta
    return body
