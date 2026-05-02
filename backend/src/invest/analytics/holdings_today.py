"""Single source of truth for "what's held today, valued today".

Every endpoint that asks the question calls `current_holdings()`. The
function picks one data path internally — callers don't branch between
PDF projection and positions_daily. That caller-level fork was the
structural cause of the Bug 3 family: equity card, equity curve, sector
donut, /tax, /risk could all disagree even though they were asking the
same thing.

Resolution rules:
  - Warm: positions_daily has rows for the latest priced day → return
    those rows, enriched with PDF metadata (name, avg_cost, venue,
    ccy, type) that the daily cache doesn't store.
  - Cold: positions_daily empty for that day → PDF month-end holdings
    repriced with today's close per-symbol; symbols without a daily
    close keep their month-end value.

PDF still drives write-time correctness: positions_daily is populated
by the PDF parser (source='pdf') and overlay (source='overlay'). The
write-time UPSERT in trade_overlay.py guarantees PDF wins on conflict.
Readers see one table.
"""
from __future__ import annotations

from typing import Any

from . import monthly as analytics


def current_holdings(store, daily_store) -> list[dict[str, Any]]:
    """Today's holdings, valued today. See module docstring for rules.

    The presence of a portfolio_daily snapshot is the signal that
    backfill has run — positions_daily is then authoritative even when
    empty (user holds nothing on that date is a valid answer, not a
    cue to fall back to PDF). Falling back to PDF only when the daily
    store has *no snapshot at all* is what kills the Bug 3 fork: a
    user who sold their entire position post-PDF is correctly reported
    as holding nothing, instead of showing PDF-projected ghost shares.
    """
    if not store.months:
        return []
    last = store.months[-1]

    snapshot = daily_store.get_today_snapshot()
    if snapshot:
        as_of = snapshot["date"]
        positions = daily_store.get_positions_snapshot(as_of) or []
        pdf_meta = _build_pdf_metadata(last)
        return [_to_api_row(p, pdf_meta, as_of) for p in positions]

    return _pdf_month_end_repriced(last, daily_store)


def holdings_for_month(month: dict) -> list[dict]:
    """PDF month-end holdings → normalized API row shape.

    For historical views (/api/holdings/snapshot/<month>) where the
    literal question is "what was held as of this past month-end".
    Do NOT use this for "today" — call current_holdings() instead.
    """
    rows: list[dict] = []
    fx = month.get("fx_usd_twd", 1.0) or 1.0
    for h in month.get("tw", {}).get("holdings", []):
        rows.append(_normalize_tw(h))
    for h in month.get("foreign", {}).get("holdings", []):
        rows.append(_normalize_foreign(h, fx))
    return rows


def _pdf_month_end_repriced(last: dict, daily_store) -> list[dict]:
    """Cold path: PDF month-end rows repriced with today's close per-symbol.

    Per-symbol fallback: tickers with a daily close get today's price;
    tickers without (delisted, thin volume, fresh listing) keep their
    month-end value. When the daily store has no snapshot at all, every
    row keeps its month-end value (true cold start).
    """
    rows = holdings_for_month(last)
    if not rows:
        return rows
    snap = daily_store.get_today_snapshot()
    if not snap:
        return rows
    fx_today = snap.get("fx_usd_twd") or last.get("fx_usd_twd")
    codes = [r["code"] for r in rows if r.get("code")]
    closes = daily_store.get_latest_closes(codes) if codes else {}
    return analytics.reprice_holdings_with_daily(
        rows, lambda c: closes.get(c), current_fx_usd_twd=fx_today
    )


def _normalize_tw(h: dict) -> dict:
    cost = h.get("cost", 0)
    upnl = h.get("unrealized_pnl", 0)
    return {
        "venue": "TW",
        "code": h.get("code"),
        "name": h.get("name"),
        "type": h.get("type"),
        "ccy": "TWD",
        "qty": h.get("qty", 0),
        "avg_cost": h.get("avg_cost", 0),
        "cost_local": cost,
        "cost_twd": cost,
        "ref_price": h.get("ref_price", 0),
        "mkt_value_local": h.get("mkt_value", 0),
        "mkt_value_twd": h.get("mkt_value", 0),
        "unrealized_pnl_local": upnl,
        "unrealized_pnl_twd": upnl,
        "unrealized_pct": (upnl / cost) if cost else 0,
    }


def _normalize_foreign(h: dict, fx: float) -> dict:
    ccy = h.get("ccy", "USD")
    rate = fx if ccy == "USD" else 1.0
    cost_local = h.get("cost", 0)
    mkt_local = h.get("mkt_value", 0)
    upnl_local = h.get("unrealized_pnl", 0)
    qty = h.get("qty", 0)
    return {
        "venue": "Foreign",
        "code": h.get("code"),
        "name": h.get("name"),
        "type": h.get("market"),
        "ccy": ccy,
        "qty": qty,
        "avg_cost": (cost_local / qty) if qty else 0,
        "cost_local": cost_local,
        "cost_twd": cost_local * rate,
        "ref_price": h.get("close", 0),
        "mkt_value_local": mkt_local,
        "mkt_value_twd": mkt_local * rate,
        "unrealized_pnl_local": upnl_local,
        "unrealized_pnl_twd": upnl_local * rate,
        "unrealized_pct": (upnl_local / cost_local) if cost_local else 0,
    }


def _build_pdf_metadata(last: dict) -> dict[str, dict]:
    """Index of {code: PDF metadata} used to enrich positions_daily rows."""
    meta: dict[str, dict] = {}
    for h in last.get("tw", {}).get("holdings", []):
        code = h.get("code")
        if code:
            meta[code] = {
                "venue": "TW",
                "ccy": "TWD",
                "name": h.get("name") or code,
                "avg_cost": h.get("avg_cost", 0),
                "type": h.get("type", "現股"),
            }
    for h in last.get("foreign", {}).get("holdings", []):
        code = h.get("code")
        if code:
            qty = h.get("qty") or 0
            cost = h.get("cost", 0) or 0
            meta[code] = {
                "venue": "Foreign",
                "ccy": h.get("ccy", "USD"),
                "name": h.get("name") or code,
                "avg_cost": (cost / qty) if qty else 0,
                "type": h.get("market", "USA"),
            }
    return meta


def _default_meta(code: str, position_type: str | None) -> dict:
    """Best-effort metadata for an overlay-only ticker with no PDF history.

    TW codes are numeric strings (e.g. "2330"); foreign codes start with
    an alpha char (e.g. "AAPL"). Guard against the empty-string edge case
    from parser gaps — an empty code is unknown, not TW or Foreign.
    """
    is_tw = bool(code) and not code[0].isalpha()
    return {
        "venue": "TW" if is_tw else "Foreign",
        "ccy": "TWD",
        "name": code or "?",
        "avg_cost": 0.0,
        "type": position_type or "現股",
    }


def _to_api_row(position: dict, pdf_meta: dict[str, dict], as_of: str) -> dict[str, Any]:
    """positions_daily row → API holding shape."""
    code = position["symbol"]
    meta = pdf_meta.get(code)
    if meta is None:
        meta = _default_meta(code, position.get("type"))

    cost_local = float(position.get("cost_local", 0) or 0)
    mv_local = float(position.get("mv_local", 0) or 0)
    mv_twd = float(position.get("mv_twd", 0) or 0)
    qty = float(position.get("qty", 0) or 0)

    cost_twd = cost_local * (mv_twd / mv_local) if mv_local else cost_local
    upnl_twd = mv_twd - cost_twd
    upnl_local = mv_local - cost_local
    ref_price = (mv_local / qty) if qty else 0
    upnl_pct = (upnl_twd / cost_twd) if cost_twd else 0

    return {
        "venue": meta["venue"],
        "code": code,
        "name": meta["name"],
        "type": meta["type"],
        "ccy": meta["ccy"],
        "qty": qty,
        "avg_cost": meta["avg_cost"],
        "cost_local": cost_local,
        "cost_twd": cost_twd,
        "ref_price": ref_price,
        "mkt_value_local": mv_local,
        "mkt_value_twd": mv_twd,
        "unrealized_pnl_local": upnl_local,
        "unrealized_pnl_twd": upnl_twd,
        "unrealized_pct": upnl_pct,
        "repriced_at": as_of,
        "source": position.get("source") or "pdf",
    }
