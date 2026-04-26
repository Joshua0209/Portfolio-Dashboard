"""Cold-start daily backfill: portfolio.json + TWSE → SQLite cache.

This module is the bridge from the JSON source-of-truth to the daily
SQLite cache. It walks the trade ledger and holdings tables in
data/portfolio.json, computes a per-symbol fetch window per spec §6.1
(BACKFILL_FLOOR=2025-08-01, [max(first_trade, FLOOR), max(last_trade,
last_held_date)]), pulls prices via app.price_sources.get_prices, and
UPSERTs into the prices / positions_daily / portfolio_daily tables.

Phase 3 wires the TW path only. Foreign + FX land in Phase 6.

The Phase 9 background-thread wrapper (and INITIALIZING/READY/FAILED
state machine) is layered on top of this in a later commit; for now,
run_tw_backfill() is a synchronous function that any caller (the
scripts/backfill_daily.py CLI, future tests, the eventual daemon) can
invoke directly.
"""
from __future__ import annotations

import calendar
import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from app.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore
from app.price_sources import get_prices

log = logging.getLogger(__name__)


# --- Date utilities -------------------------------------------------------


def _today_iso() -> str:
    """Indirection so tests can pin 'today' deterministically."""
    return date.today().isoformat()


def month_end_iso(yyyy_mm: str) -> str:
    """'2025-02' → '2025-02-28' (handles leap years)."""
    y, m = (int(p) for p in yyyy_mm.split("-"))
    last_day = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-{last_day:02d}"


def _normalize_trade_date(d: str) -> str:
    """Trade dates in portfolio.json are 'YYYY/MM/DD' — normalize to ISO."""
    if "/" in d:
        return d.replace("/", "-")
    return d


# --- Per-symbol windowing -------------------------------------------------


def compute_fetch_window(
    trade_dates: Iterable[str],
    held_months: Iterable[str],
    latest_data_month: str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> tuple[str, str] | None:
    """Compute (fetch_start, fetch_end) per spec §6.1, or None to skip.

    All dates are ISO YYYY-MM-DD; held_months and latest_data_month are
    YYYY-MM. Returns None when:
      - no history at all (no trades, no holdings), or
      - the symbol's entire active window precedes `floor`.

    "Currently held" = the symbol's latest_held_month equals the latest
    month present in portfolio.json. In that case, fetch_end = today (the
    PDF stops at month-end but the position carries forward to today).
    """
    today = today or _today_iso()
    trade_list = sorted({_normalize_trade_date(d) for d in trade_dates})
    held_list = sorted(set(held_months))
    if not trade_list and not held_list:
        return None

    first_trade = trade_list[0] if trade_list else None
    last_trade = trade_list[-1] if trade_list else None
    last_held_month = held_list[-1] if held_list else None

    currently_held = last_held_month == latest_data_month
    if currently_held:
        last_held_date = today
    elif last_held_month:
        last_held_date = month_end_iso(last_held_month)
    else:
        last_held_date = None

    fetch_start = max(first_trade or floor, floor)

    end_candidates = [d for d in (last_trade, last_held_date) if d]
    fetch_end = max(end_candidates) if end_candidates else None

    if fetch_end is None or fetch_end < floor:
        return None

    if fetch_start > fetch_end:
        return None

    return (fetch_start, fetch_end)


# --- Portfolio.json walkers -----------------------------------------------


def iter_tw_symbols_with_metadata(portfolio: dict) -> Iterable[dict]:
    """Yield {code, trade_dates, held_months} for every distinct TW symbol
    that appears in trade ledger or holdings."""
    trade_idx: dict[str, list[str]] = {}
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "TW":
            continue
        code = t.get("code")
        if not code:
            continue
        trade_idx.setdefault(code, []).append(_normalize_trade_date(t["date"]))

    held_idx: dict[str, set[str]] = {}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        if not ym:
            continue
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = h.get("qty", 0) or 0
            if qty <= 0:
                continue
            held_idx.setdefault(code, set()).add(ym)

    codes = set(trade_idx) | set(held_idx)
    for code in sorted(codes):
        yield {
            "code": code,
            "trade_dates": trade_idx.get(code, []),
            "held_months": sorted(held_idx.get(code, set())),
        }


def _latest_data_month(portfolio: dict) -> str:
    months = portfolio.get("months", [])
    return months[-1]["month"] if months else ""


# --- Position derivation --------------------------------------------------


def _qty_history_for_symbol(
    portfolio: dict, code: str
) -> list[tuple[str, float]]:
    """Return [(date, signed_qty_change), ...] for one TW symbol, sorted by date.

    Buys add positive qty; sells subtract. Used to compute end-of-day qty
    on any trading day by cumulative sum.
    """
    out: list[tuple[str, float]] = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "TW" or t.get("code") != code:
            continue
        d = _normalize_trade_date(t["date"])
        side = t.get("side", "")
        qty = float(t.get("qty", 0) or 0)
        sign = 1 if "買" in side else -1
        out.append((d, sign * qty))
    out.sort(key=lambda r: r[0])
    return out


def _derive_positions_and_portfolio(
    store: DailyStore, portfolio: dict
) -> dict[str, int]:
    """Walk every priced trading day in the prices table, compute end-of-day
    qty per TW symbol from the trade ledger, multiply by close → mv_local,
    and aggregate to portfolio_daily.equity_twd.

    Phase 3 simplification: TW only (currency is always TWD), no overlay,
    cost = avg_cost from latest holdings (not FIFO per-day). Phase 6 adds
    foreign + FX; Phase 11 adds overlay.
    """
    qty_changes: dict[str, list[tuple[str, float]]] = {}
    cost_at: dict[str, float] = {}  # last-seen avg_cost from holdings

    for entry in iter_tw_symbols_with_metadata(portfolio):
        code = entry["code"]
        qty_changes[code] = _qty_history_for_symbol(portfolio, code)

    for m in portfolio.get("months", []):
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if code:
                cost_at[code] = float(h.get("avg_cost", 0) or 0)

    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT DISTINCT date FROM prices ORDER BY date"
        ).fetchall()
        priced_dates = [r[0] for r in rows]
        all_prices = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT date, symbol, close FROM prices"
            ).fetchall()
        }

    if not priced_dates:
        return {"positions_rows": 0, "portfolio_rows": 0}

    n_positions = 0
    n_portfolio = 0
    with store.connect_rw() as conn:
        for d in priced_dates:
            day_equity = 0.0
            day_n = 0
            for code, changes in qty_changes.items():
                qty = sum(q for date_, q in changes if date_ <= d)
                if qty <= 0:
                    continue
                close = all_prices.get((d, code))
                if close is None:
                    continue
                mv_local = qty * close
                cost = cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    """,
                    (d, code, qty, cost, mv_local, mv_local, "現股", "pdf"),
                )
                n_positions += 1
                day_equity += mv_local
                day_n += 1
            if day_n == 0:
                continue
            conn.execute(
                """
                INSERT INTO portfolio_daily(
                    date, equity_twd, cash_twd, fx_usd_twd, n_positions, has_overlay
                ) VALUES (?, ?, NULL, ?, ?, 0)
                ON CONFLICT(date) DO UPDATE SET
                    equity_twd = excluded.equity_twd,
                    fx_usd_twd = excluded.fx_usd_twd,
                    n_positions = excluded.n_positions
                """,
                # FX placeholder = 0 in TW-only phase. Phase 6 fills fx_daily
                # and switches portfolio_daily.fx_usd_twd to the real rate.
                (d, day_equity, 0.0, day_n),
            )
            n_portfolio += 1

    return {"positions_rows": n_positions, "portfolio_rows": n_portfolio}


# --- Public entry ---------------------------------------------------------


def _persist_symbol_prices(
    store: DailyStore, code: str, rows: list[dict]
) -> int:
    """Write one symbol's price rows + symbol_market row in a single tx.

    Per-symbol commits so progress is visible during long cold-starts and
    a crash mid-backfill doesn't lose previously-fetched symbols.
    """
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with store.connect_rw() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO prices(date, symbol, close, currency, source, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    close = excluded.close,
                    currency = excluded.currency,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at
                """,
                (r["date"], r["symbol"], r["close"], r["currency"], r["source"], now),
            )
        conn.execute(
            """
            INSERT INTO symbol_market(symbol, market, resolved_at, last_verified_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                market = excluded.market,
                last_verified_at = excluded.last_verified_at
            """,
            (code, "twse" if rows else "unknown", now, now),
        )
    return len(rows)


def run_tw_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
    limit: int | None = None,
    only_codes: set[str] | None = None,
) -> dict[str, Any]:
    """Run a TW-only backfill against the given DailyStore.

    Per-symbol transactions (so progress is visible and crashes don't lose
    earlier work). After all symbols are fetched, derive positions_daily
    and portfolio_daily in their own pass.

    `limit`: optional cap on number of symbols processed (--limit flag in
    the CLI). Useful for smoke tests on a subset without waiting for the
    full ~5–8 minutes that 30+ TW codes × 8 months × ~1.2s/fetch implies
    (the plan's "≤90s" target assumes a smaller portfolio).

    `only_codes`: if set, only fetch these symbols. Skip-tracking still
    runs for the rest.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))

    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    skipped: list[str] = []
    fetched: list[str] = []
    rows_written = 0

    candidates = list(iter_tw_symbols_with_metadata(portfolio))
    processed = 0

    for entry in candidates:
        code = entry["code"]
        window = compute_fetch_window(
            trade_dates=entry["trade_dates"],
            held_months=entry["held_months"],
            latest_data_month=latest_month,
            floor=floor,
            today=today,
        )
        if window is None:
            skipped.append(code)
            log.info("backfill: skipping %s (out of floor)", code)
            continue
        if only_codes is not None and code not in only_codes:
            continue
        if limit is not None and processed >= limit:
            log.info("backfill: --limit reached at %d, remaining symbols deferred", limit)
            break
        start, end = window
        log.info("backfill: %s [%s..%s]", code, start, end)
        try:
            rows = get_prices(code, "TWD", start, end)
        except NotImplementedError:
            # Defensive: shouldn't happen for TWD; log and skip.
            log.warning("backfill: get_prices NIE for %s", code)
            continue
        rows_written += _persist_symbol_prices(store, code, rows)
        fetched.append(code)
        processed += 1

    derived = _derive_positions_and_portfolio(store, portfolio)
    store.set_meta("last_known_date", today)

    summary = {
        "today": today,
        "floor": floor,
        "skipped": skipped,
        "fetched": fetched,
        "price_rows_written": rows_written,
        **derived,
    }
    log.info("backfill summary: %s", summary)
    return summary
