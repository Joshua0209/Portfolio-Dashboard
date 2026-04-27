"""Symbol-class router for daily prices.

`get_prices(symbol, currency, start, end, store=None)` dispatches to the
right backend with dynamic market discovery and caching:

  TW (currency == "TWD"):
    All TW symbols are fetched via yfinance using suffix conventions:
      `.TW`  → TWSE-listed (e.g. 2330.TW)
      `.TWO` → TPEX/OTC    (e.g. 5483.TWO)

    1. Consult `symbol_market` cache (if `store` given). On hit ('twse' or
       'tpex'), dispatch directly with the corresponding suffix — no probe.
    2. On miss: probe `.TW` first; on empty, probe `.TWO`; on empty, mark
       'unknown'. Persist the verdict so future calls skip the probe.

    The 'twse' / 'tpex' verdict labels describe the *market the symbol is
    listed on* (TWSE vs TPEX), not which API was called — yfinance is the
    only backend now.

  Foreign (currency != "TWD"):
    yfinance directly with the bare symbol (e.g. SPY, SNDK).

The router defers to `_fetch_yfinance_with_set_minus` for both TW and
foreign paths — same helper, same set-minus / mark-checked semantics.
Rows are tagged with `symbol` (bare code, no Yahoo suffix), `currency`,
and `source='yfinance'`.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.yfinance_client import fetch_fx as yfinance_fetch_fx
from app.yfinance_client import fetch_prices as yfinance_fetch_prices

if TYPE_CHECKING:
    from app.daily_store import DailyStore

log = logging.getLogger(__name__)


def _yesterday_iso(today: str) -> str:
    return (date.fromisoformat(today) - timedelta(days=1)).isoformat()


def coalesce_date_ranges(dates: list[str]) -> list[tuple[str, str]]:
    """Merge consecutive ISO dates into [start, end] ranges.

    [2025-11-04, 2025-11-05, 2025-11-06, 2025-11-15] →
    [(2025-11-04, 2025-11-06), (2025-11-15, 2025-11-15)]
    Input must be sorted ascending.
    """
    if not dates:
        return []
    out: list[tuple[str, str]] = []
    range_start = prev = date.fromisoformat(dates[0])
    for s in dates[1:]:
        d = date.fromisoformat(s)
        if (d - prev).days == 1:
            prev = d
            continue
        out.append((range_start.isoformat(), prev.isoformat()))
        range_start = prev = d
    out.append((range_start.isoformat(), prev.isoformat()))
    return out


def _mark_range(
    store: "DailyStore | None",
    symbol: str,
    start: str,
    end: str,
    today: str | None,
) -> None:
    """Day-granular: mark exactly [start, min(end, today-1)]."""
    if store is None or today is None:
        return
    effective_end = min(end, _yesterday_iso(today))
    if effective_end >= start:
        store.mark_dates_checked(symbol, start, effective_end)


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


def _yahoo_suffix_for_market(market: str) -> str:
    """Map a `symbol_market.market` verdict to its Yahoo Finance suffix."""
    if market == "twse":
        return ".TW"
    if market == "tpex":
        return ".TWO"
    raise ValueError(f"no Yahoo suffix for market verdict {market!r}")


# --- Public entry --------------------------------------------------------


def get_prices(
    symbol: str,
    currency: str,
    start: str,
    end: str,
    store: "DailyStore | None" = None,
    today: str | None = None,
) -> list[dict]:
    """Return [{date, close, volume, symbol, currency, source}, ...] for
    the inclusive window [start, end].

    When `store` and `today` are both provided, dates already present in
    `dates_checked` are skipped and successful fetches mark their covered
    range (clipped to today-1)."""
    if currency == "TWD":
        return _get_prices_tw(symbol, start, end, store, today)
    return _get_prices_foreign(symbol, currency, start, end, store, today)


def get_yfinance_prices(
    symbol: str,
    start: str,
    end: str,
    store: "DailyStore | None" = None,
    today: str | None = None,
) -> list[dict]:
    """Direct yfinance fetch for already-Yahoo-suffixed tickers (benchmarks).

    Same set-minus / mark-checked semantics as `_get_prices_foreign`, but
    bypasses the TW/foreign currency router. Returned rows are unwrapped —
    the caller tags them (the benchmark backfill writes to the `prices`
    table with its own `currency` and `source` values)."""
    return _fetch_yfinance_with_set_minus(symbol, start, end, store, today)


def get_fx_rates(
    ccy: str,
    start: str,
    end: str,
    store: "DailyStore | None" = None,
    today: str | None = None,
) -> list[dict]:
    """Return [{date, ccy, rate, source}, ...] for ccy → TWD over the
    inclusive window. Used by backfill_runner to populate `fx_daily`.

    `dates_checked` keys for FX use `FX:<ccy>` to keep them in a separate
    namespace from equity symbols."""
    fx_key = f"FX:{ccy}"
    if store is not None and today is not None:
        # Clip to yesterday: matches _mark_range's today-clip below, so a
        # same-day re-run finds nothing missing instead of re-issuing a
        # single-day yfinance FX call per currency.
        effective_end = min(end, _yesterday_iso(today))
        if effective_end < start:
            return []
        missing = store.find_missing_dates(fx_key, start, effective_end)
        if not missing:
            return []
        ranges = coalesce_date_ranges(missing)
    else:
        ranges = [(start, end)]

    seen: set[str] = set()
    out: list[dict] = []
    for r_start, r_end in ranges:
        raw = yfinance_fetch_fx(ccy, r_start, r_end)
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
        _mark_range(store, fx_key, r_start, r_end, today)
    return out


# --- TW dispatch with cache + dynamic discovery --------------------------


def _get_prices_tw(
    symbol: str,
    start: str,
    end: str,
    store: "DailyStore | None",
    today: str | None,
) -> list[dict]:
    cached = _get_cached_market(store, symbol)

    if cached in ("twse", "tpex"):
        suffix = _yahoo_suffix_for_market(cached)
        rows = _fetch_yfinance_with_set_minus(
            f"{symbol}{suffix}", start, end, store, today, cache_key=symbol,
        )
        return _tag(rows, symbol, "TWD", "yfinance")
    if cached == "unknown":
        # Already probed both suffixes and got nothing; skip the round-trip.
        return []

    # Cache miss: probe `.TW` → `.TWO` → mark unknown. The probes pass
    # store=None so a fruitless first-suffix probe doesn't mark dates as
    # checked — that would prevent the second-suffix probe from running on
    # a future call. After a successful probe we explicitly mark the
    # window against the bare symbol so subsequent calls hit the cache.
    twse_rows = _fetch_yfinance_with_set_minus(
        f"{symbol}.TW", start, end, None, None,
    )
    if twse_rows:
        _persist_market(store, symbol, "twse")
        _mark_range(store, symbol, start, end, today)
        return _tag(twse_rows, symbol, "TWD", "yfinance")

    tpex_rows = _fetch_yfinance_with_set_minus(
        f"{symbol}.TWO", start, end, None, None,
    )
    if tpex_rows:
        _persist_market(store, symbol, "tpex")
        _mark_range(store, symbol, start, end, today)
        return _tag(tpex_rows, symbol, "TWD", "yfinance")

    _persist_market(store, symbol, "unknown")
    return []


def _fetch_yfinance_with_set_minus(
    yf_symbol: str,
    start: str,
    end: str,
    store: "DailyStore | None",
    today: str | None,
    cache_key: str | None = None,
) -> list[dict]:
    """Day-granular yfinance fetch with set-minus + mark-checked.

    `yf_symbol` is the ticker passed to yfinance (may carry a `.TW`/`.TWO`
    suffix). `cache_key` is the key used for `dates_checked` lookups and
    marks; defaults to `yf_symbol`. The split lets the TW path query
    `2330.TW` against yfinance while caching against the bare `2330` that
    lives in the `prices` table.

    Used by `_get_prices_tw`, `_get_prices_foreign`, and the public
    `get_yfinance_prices` (for benchmarks). Returns un-tagged rows;
    callers tag with their own currency/source.

    Missing-dates check clips at yesterday to match `_mark_range` — today
    is never persisted as checked, so leaving it in the missing set would
    re-issue a single-day yfinance call on every same-day re-run.
    """
    cache_key = cache_key or yf_symbol
    if store is not None and today is not None:
        effective_end = min(end, _yesterday_iso(today))
        if effective_end < start:
            return []
        missing = store.find_missing_dates(cache_key, start, effective_end)
        if not missing:
            return []
        ranges = coalesce_date_ranges(missing)
    else:
        ranges = [(start, end)]

    seen: set[str] = set()
    out: list[dict] = []
    for r_start, r_end in ranges:
        raw = yfinance_fetch_prices(yf_symbol, r_start, r_end)
        for r in raw:
            d = r["date"]
            if d < start or d > end:
                continue
            if d in seen:
                continue
            seen.add(d)
            out.append(r)
        _mark_range(store, cache_key, r_start, r_end, today)
    return out


def _get_prices_foreign(
    symbol: str,
    currency: str,
    start: str,
    end: str,
    store: "DailyStore | None",
    today: str | None,
) -> list[dict]:
    """Foreign-equity branch: yfinance bulk pull, set-minus aware, window-filtered."""
    rows = _fetch_yfinance_with_set_minus(symbol, start, end, store, today)
    return _tag(rows, symbol, currency, "yfinance")
