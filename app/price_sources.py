"""Symbol-class router for daily prices.

`get_prices(symbol, currency, start, end)` dispatches to the right backend:

  - TW (currency == TWD)        → TWSE (Phase 2)
                                  → TPEX fallback     (added Phase 5)
                                  → yfinance:.TW fallback (added Phase 6)
  - foreign (currency != TWD)   → yfinance plain symbol  (added Phase 6)

Phase 2 wires only the TWSE branch. Foreign currency calls raise
NotImplementedError so a stray Phase 3 caller fails loudly instead of
silently ignoring its USD positions.

The router is responsible for:
  - splitting the [start, end] window into calendar-month batches
    (TWSE only serves one month at a time)
  - filtering returned rows back to [start, end]
  - tagging each row with `symbol`, `currency`, and `source`
"""
from __future__ import annotations

import logging
from datetime import date

from app.twse_client import fetch_month as twse_fetch_month

log = logging.getLogger(__name__)


def months_in_range(start: str, end: str) -> list[tuple[int, int]]:
    """Inclusive list of (year, month) calendar months touching [start, end].

    Both endpoints are ISO YYYY-MM-DD strings. The 'first of month' walk is
    deliberate: TWSE's `date=YYYYMM01` parameter ignores the day, so we want
    one batch per calendar month regardless of where the window starts/ends.
    """
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


def get_prices(
    symbol: str, currency: str, start: str, end: str
) -> list[dict]:
    """Return [{date, close, volume, symbol, currency, source}, ...] for
    the inclusive window [start, end]."""
    if currency == "TWD":
        return _get_prices_tw(symbol, start, end)
    raise NotImplementedError(
        f"price_sources phase 2 only handles TWD; got currency={currency!r}. "
        "Foreign + FX wired in Phase 6."
    )


def _get_prices_tw(symbol: str, start: str, end: str) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for y, m in months_in_range(start, end):
        rows = twse_fetch_month(symbol, y, m)
        for r in rows:
            d = r["date"]
            if d < start or d > end:
                continue
            if d in seen:
                continue
            seen.add(d)
            out.append(r)
    return _tag(out, symbol, "TWD", "twse")
