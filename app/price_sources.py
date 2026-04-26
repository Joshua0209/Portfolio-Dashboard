"""Symbol-class router for daily prices.

`get_prices(symbol, currency, start, end, store=None)` dispatches to the
right backend with dynamic market discovery and caching:

  TW (currency == "TWD"):
    1. Consult `symbol_market` cache (if `store` given). On hit, dispatch
       directly — no probing.
    2. On miss: probe TWSE first; on empty, probe TPEX; on empty, mark
       'unknown'. Persist the verdict so future calls skip the probe.

  Foreign (currency != "TWD"):
    Phase 6 wires yfinance. Until then, raise NotImplementedError so a
    stray caller fails loudly instead of silently dropping foreign rows.

The router also splits [start, end] into calendar-month batches (TWSE and
TPEX both serve one month at a time) and tags each row with `symbol`,
`currency`, and `source`.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from app.tpex_client import fetch_month as tpex_fetch_month
from app.twse_client import fetch_month as twse_fetch_month
from app.yfinance_client import fetch_fx as yfinance_fetch_fx
from app.yfinance_client import fetch_prices as yfinance_fetch_prices

if TYPE_CHECKING:
    from app.daily_store import DailyStore

log = logging.getLogger(__name__)


def months_in_range(start: str, end: str) -> list[tuple[int, int]]:
    """Inclusive list of (year, month) calendar months touching [start, end]."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    if e < s:
        return []
    out: list[tuple[int, int]] = []
    y, m = s.year, s.month
    while (y, m) <= (e.year, e.month):
        out.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _tag(rows: list[dict], symbol: str, currency: str, source: str) -> list[dict]:
    return [
        {**r, "symbol": symbol, "currency": currency, "source": source}
        for r in rows
    ]


# --- symbol_market cache helpers -----------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _get_cached_market(store: "DailyStore | None", symbol: str) -> str | None:
    if store is None:
        return None
    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", (symbol,)
        ).fetchone()
    return row["market"] if row else None


def _persist_market(store: "DailyStore | None", symbol: str, market: str) -> None:
    if store is None:
        return
    now = _now_iso()
    with store.connect_rw() as conn:
        conn.execute(
            """
            INSERT INTO symbol_market(symbol, market, resolved_at, last_verified_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                market = excluded.market,
                last_verified_at = excluded.last_verified_at
            """,
            (symbol, market, now, now),
        )


# --- Public entry --------------------------------------------------------


def get_prices(
    symbol: str,
    currency: str,
    start: str,
    end: str,
    store: "DailyStore | None" = None,
) -> list[dict]:
    """Return [{date, close, volume, symbol, currency, source}, ...] for
    the inclusive window [start, end]."""
    if currency == "TWD":
        return _get_prices_tw(symbol, start, end, store)
    return _get_prices_foreign(symbol, currency, start, end)


def get_fx_rates(ccy: str, start: str, end: str) -> list[dict]:
    """Return [{date, ccy, rate, source}, ...] for ccy → TWD over the
    inclusive window. Used by backfill_runner to populate `fx_daily`."""
    raw = yfinance_fetch_fx(ccy, start, end)
    seen: set[str] = set()
    out: list[dict] = []
    for r in raw:
        d = r["date"]
        if d < start or d > end:
            continue
        if d in seen:
            continue
        seen.add(d)
        out.append({
            "date": d,
            "ccy": ccy,
            "rate": float(r["rate"]),
            "source": "yfinance",
        })
    return out


# --- TW dispatch with cache + dynamic discovery --------------------------


def _fetch_months(
    fetch_fn, symbol: str, start: str, end: str
) -> list[dict]:
    """Walk months and apply the [start, end] window filter + dedupe."""
    seen: set[str] = set()
    out: list[dict] = []
    for y, m in months_in_range(start, end):
        rows = fetch_fn(symbol, y, m)
        for r in rows:
            d = r["date"]
            if d < start or d > end:
                continue
            if d in seen:
                continue
            seen.add(d)
            out.append(r)
    return out


def _get_prices_tw(
    symbol: str, start: str, end: str, store: "DailyStore | None"
) -> list[dict]:
    cached = _get_cached_market(store, symbol)

    if cached == "twse":
        rows = _fetch_months(twse_fetch_month, symbol, start, end)
        return _tag(rows, symbol, "TWD", "twse")
    if cached == "tpex":
        rows = _fetch_months(tpex_fetch_month, symbol, start, end)
        return _tag(rows, symbol, "TWD", "tpex")
    if cached == "unknown":
        # Already probed both and got nothing; don't re-burn the WAF budget.
        return []

    # Cache miss: probe TWSE → TPEX → mark unknown.
    twse_rows = _fetch_months(twse_fetch_month, symbol, start, end)
    if twse_rows:
        _persist_market(store, symbol, "twse")
        return _tag(twse_rows, symbol, "TWD", "twse")

    tpex_rows = _fetch_months(tpex_fetch_month, symbol, start, end)
    if tpex_rows:
        _persist_market(store, symbol, "tpex")
        return _tag(tpex_rows, symbol, "TWD", "tpex")

    _persist_market(store, symbol, "unknown")
    return []


def _get_prices_foreign(
    symbol: str, currency: str, start: str, end: str
) -> list[dict]:
    """Foreign-equity branch: yfinance bulk pull, window-filtered."""
    raw = yfinance_fetch_prices(symbol, start, end)
    seen: set[str] = set()
    out: list[dict] = []
    for r in raw:
        d = r["date"]
        if d < start or d > end:
            continue
        if d in seen:
            continue
        seen.add(d)
        out.append(r)
    return _tag(out, symbol, currency, "yfinance")
