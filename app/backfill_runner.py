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
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from app import backfill_state
from app.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore
from app.price_sources import get_fx_rates, get_prices

log = logging.getLogger(__name__)

# Module-level so a second start() in the same process doesn't double-spawn.
_thread_lock = threading.Lock()
_active_thread: threading.Thread | None = None


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


def iter_foreign_symbols_with_metadata(portfolio: dict) -> Iterable[dict]:
    """Yield {code, currency, trade_dates, held_months} for each distinct
    foreign symbol that appears in the trade ledger or in any month's
    foreign holdings table.

    Foreign trades carry venue=='Foreign' (set by parse_statements.py). The
    holdings tables live under months[].foreign.holdings. Currency is taken
    from the trade record's `ccy` field, falling back to the most recent
    holdings record. Phase 6 wires USD; HKD/JPY follow the same path.
    """
    trade_idx: dict[str, list[str]] = {}
    ccy_idx: dict[str, str] = {}
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "Foreign":
            continue
        code = t.get("code")
        if not code:
            continue
        trade_idx.setdefault(code, []).append(_normalize_trade_date(t["date"]))
        if t.get("ccy"):
            ccy_idx[code] = t["ccy"]

    held_idx: dict[str, set[str]] = {}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        if not ym:
            continue
        for h in m.get("foreign", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = h.get("qty", 0) or 0
            if qty <= 0:
                continue
            held_idx.setdefault(code, set()).add(ym)
            if h.get("ccy"):
                ccy_idx.setdefault(code, h["ccy"])

    codes = set(trade_idx) | set(held_idx)
    for code in sorted(codes):
        yield {
            "code": code,
            "currency": ccy_idx.get(code, "USD"),
            "trade_dates": trade_idx.get(code, []),
            "held_months": sorted(held_idx.get(code, set())),
        }


def _foreign_currencies_in_scope(portfolio: dict) -> set[str]:
    """Distinct non-TWD currencies referenced by foreign holdings/trades."""
    out: set[str] = set()
    for entry in iter_foreign_symbols_with_metadata(portfolio):
        ccy = entry.get("currency")
        if ccy and ccy != "TWD":
            out.add(ccy)
    # FX backfill always covers USD even if no current foreign positions
    # — bank cash and historical positions need the curve.
    out.add("USD")
    return out


# --- Position derivation --------------------------------------------------


def _qty_history_for_symbol(
    portfolio: dict, code: str, venue: str = "TW"
) -> list[tuple[str, float]]:
    """Return [(date, signed_qty_change), ...] for one symbol on `venue`,
    sorted by date.

    Buys add positive qty; sells subtract. Foreign trades use 買進/賣出
    while TW trades use 普買/普賣 — both contain "買"/"賣" so the same
    sign rule works.
    """
    out: list[tuple[str, float]] = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != venue or t.get("code") != code:
            continue
        d = _normalize_trade_date(t["date"])
        side = t.get("side", "")
        qty = float(t.get("qty", 0) or 0)
        sign = 1 if "買" in side else -1
        out.append((d, sign * qty))
    out.sort(key=lambda r: r[0])
    return out


def _forward_fill_fx(
    fx_rows: list[tuple[str, float]], dates: Iterable[str]
) -> dict[str, float]:
    """Build a {date → rate} map across `dates` by carrying the most-recent
    rate forward (yfinance returns stale `TWD=X` rows on Asia weekends, so
    a price-day can land on a no-FX date).

    `fx_rows` must be sorted by date asc.
    """
    out: dict[str, float] = {}
    if not fx_rows:
        return out
    sorted_dates = sorted(set(dates))
    fx_idx = 0
    last_rate: float | None = None
    for d in sorted_dates:
        while fx_idx < len(fx_rows) and fx_rows[fx_idx][0] <= d:
            last_rate = fx_rows[fx_idx][1]
            fx_idx += 1
        if last_rate is None:
            # Fall back: scan ahead for the earliest rate (handles dates
            # before the first fx row, e.g. the very start of the curve).
            for fd, fr in fx_rows:
                if fd >= d:
                    last_rate = fr
                    break
        if last_rate is not None:
            out[d] = last_rate
    return out


def _derive_positions_and_portfolio(
    store: DailyStore, portfolio: dict
) -> dict[str, int]:
    """Walk every priced trading day, compute end-of-day qty per symbol
    from the trade ledger, multiply by close → mv_local, convert foreign
    via fx_daily (forward-fill on weekend gaps), and aggregate to
    portfolio_daily.equity_twd.
    """
    tw_qty_changes: dict[str, list[tuple[str, float]]] = {}
    foreign_qty_changes: dict[str, list[tuple[str, float]]] = {}
    foreign_currency: dict[str, str] = {}
    cost_at: dict[str, float] = {}

    for entry in iter_tw_symbols_with_metadata(portfolio):
        code = entry["code"]
        tw_qty_changes[code] = _qty_history_for_symbol(portfolio, code, "TW")

    for entry in iter_foreign_symbols_with_metadata(portfolio):
        code = entry["code"]
        foreign_qty_changes[code] = _qty_history_for_symbol(
            portfolio, code, "Foreign"
        )
        foreign_currency[code] = entry["currency"]

    for m in portfolio.get("months", []):
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if code:
                cost_at[code] = float(h.get("avg_cost", 0) or 0)
        for h in m.get("foreign", {}).get("holdings", []):
            code = h.get("code")
            if code:
                cost_at[code] = float(h.get("avg_cost_local", 0) or 0)

    with store.connect_ro() as conn:
        priced_dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT date FROM prices ORDER BY date"
            ).fetchall()
        ]
        all_prices = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT date, symbol, close FROM prices"
            ).fetchall()
        }
        fx_by_ccy: dict[str, list[tuple[str, float]]] = {}
        for r in conn.execute(
            "SELECT ccy, date, rate_to_twd FROM fx_daily ORDER BY ccy, date"
        ).fetchall():
            fx_by_ccy.setdefault(r[0], []).append((r[1], r[2]))

    if not priced_dates:
        return {"positions_rows": 0, "portfolio_rows": 0}

    fx_filled: dict[str, dict[str, float]] = {
        ccy: _forward_fill_fx(rows, priced_dates) for ccy, rows in fx_by_ccy.items()
    }

    n_positions = 0
    n_portfolio = 0
    with store.connect_rw() as conn:
        for d in priced_dates:
            day_equity = 0.0
            day_n = 0
            day_fx_usd = fx_filled.get("USD", {}).get(d, 0.0)

            # TW positions — local == TWD, mv_twd == mv_local
            for code, changes in tw_qty_changes.items():
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

            # Foreign positions — convert mv_local via fx_filled
            for code, changes in foreign_qty_changes.items():
                qty = sum(q for date_, q in changes if date_ <= d)
                if qty <= 0:
                    continue
                close = all_prices.get((d, code))
                if close is None:
                    continue
                ccy = foreign_currency.get(code, "USD")
                fx = fx_filled.get(ccy, {}).get(d)
                if fx is None or fx == 0:
                    # No FX for this day — skip rather than write a wrong
                    # mv_twd. portfolio_daily for this date may end up with
                    # only TW positions, which is the correct degraded state.
                    continue
                mv_local = qty * close
                mv_twd = mv_local * fx
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
                    (d, code, qty, cost, mv_local, mv_twd, "foreign", "pdf"),
                )
                n_positions += 1
                day_equity += mv_twd
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
                (d, day_equity, day_fx_usd, day_n),
            )
            n_portfolio += 1

    return {"positions_rows": n_positions, "portfolio_rows": n_portfolio}


# --- Public entry ---------------------------------------------------------


def _persist_symbol_prices(
    store: DailyStore, code: str, rows: list[dict]
) -> int:
    """Write one symbol's price rows in a single tx.

    Per-symbol commits so progress is visible during long cold-starts and
    a crash mid-backfill doesn't lose previously-fetched symbols.

    `symbol_market` writes happen inside `price_sources.get_prices()` —
    the router knows which exchange responded and persists the verdict
    there (so OTC symbols land as 'tpex', not 'twse'). The runner only
    handles the prices table.
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
    return len(rows)


def _persist_fx_rows(store: DailyStore, ccy: str, rows: list[dict]) -> int:
    """UPSERT fx_daily rows for one currency. Idempotent on (date, ccy) PK."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with store.connect_rw() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO fx_daily(date, ccy, rate_to_twd, source, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(date, ccy) DO UPDATE SET
                    rate_to_twd = excluded.rate_to_twd,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at
                """,
                (r["date"], r["ccy"], r["rate"], r["source"], now),
            )
    return len(rows)


def run_fx_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> dict[str, Any]:
    """Populate fx_daily for every foreign currency in scope across
    [floor, today]. Per spec §6.1, FX is dense across the whole equity-curve
    window, not per-symbol — the curve always needs a TWD reference.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()

    rows_written = 0
    by_ccy: dict[str, int] = {}
    for ccy in sorted(_foreign_currencies_in_scope(portfolio)):
        log.info("fx backfill: %s [%s..%s]", ccy, floor, today)
        rows = fetch_with_dlq(
            store, "fx_rates", ccy, get_fx_rates, ccy, floor, today
        )
        if rows is None:
            continue
        n = _persist_fx_rows(store, ccy, rows)
        by_ccy[ccy] = n
        rows_written += n

    return {"fx_rows_written": rows_written, "by_ccy": by_ccy}


def run_foreign_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
    limit: int | None = None,
    only_codes: set[str] | None = None,
) -> dict[str, Any]:
    """Fetch yfinance prices for each foreign symbol in portfolio.json,
    using the same per-symbol fetch-window logic as the TW backfill."""
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    skipped: list[str] = []
    fetched: list[str] = []
    rows_written = 0
    processed = 0

    for entry in iter_foreign_symbols_with_metadata(portfolio):
        code = entry["code"]
        currency = entry["currency"]
        window = compute_fetch_window(
            trade_dates=entry["trade_dates"],
            held_months=entry["held_months"],
            latest_data_month=latest_month,
            floor=floor,
            today=today,
        )
        if window is None:
            skipped.append(code)
            continue
        if only_codes is not None and code not in only_codes:
            continue
        if limit is not None and processed >= limit:
            break
        start, end = window
        log.info("foreign backfill: %s [%s..%s]", code, start, end)
        rows = fetch_with_dlq(
            store, "foreign_prices", code,
            lambda c=code, ccy=currency, s=start, e=end: get_prices(
                c, ccy, s, e, store=store
            ),
        )
        if rows is None:
            continue
        rows_written += _persist_symbol_prices(store, code, rows)
        fetched.append(code)
        processed += 1

    return {
        "skipped": skipped,
        "fetched": fetched,
        "price_rows_written": rows_written,
    }


def run_full_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> dict[str, Any]:
    """End-to-end Phase 6 backfill: TW prices, FX rates, foreign prices,
    then derive positions_daily / portfolio_daily.

    The three sub-runs are independent enough that any one can fail (DLQ
    in Phase 10) without blocking the others. The derivation pass must
    run last because it consumes prices + fx_daily.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    # 1. TW prices (wrapped in fetch_with_dlq per Phase 10)
    tw_skipped: list[str] = []
    tw_fetched: list[str] = []
    tw_rows = 0
    for entry in iter_tw_symbols_with_metadata(portfolio):
        code = entry["code"]
        window = compute_fetch_window(
            trade_dates=entry["trade_dates"],
            held_months=entry["held_months"],
            latest_data_month=latest_month,
            floor=floor,
            today=today,
        )
        if window is None:
            tw_skipped.append(code)
            continue
        start, end = window
        rows = fetch_with_dlq(
            store, "tw_prices", code,
            lambda c=code, s=start, e=end: get_prices(c, "TWD", s, e, store=store),
        )
        if rows is None:
            continue
        tw_rows += _persist_symbol_prices(store, code, rows)
        tw_fetched.append(code)

    # 2. FX
    fx_summary = run_fx_backfill(store, portfolio_path, floor=floor, today=today)

    # 3. Foreign equities
    fr_summary = run_foreign_backfill(
        store, portfolio_path, floor=floor, today=today
    )

    # 4. Derive positions_daily + portfolio_daily
    derived = _derive_positions_and_portfolio(store, portfolio)

    # 5. Phase 11 overlay: post-PDF gap filled from Shioaji (no-op without
    #    creds). Layered AFTER the PDF derivation so PDF rows are already
    #    in place; the overlay only writes to dates strictly after the
    #    last PDF month-end and never overwrites source='pdf' rows.
    from . import trade_overlay
    from .shioaji_client import ShioajiClient
    overlay_summary = {"overlay_trades": 0, "skipped_reason": "no_gap"}
    try:
        gap = trade_overlay.compute_gap_window(portfolio, today=today)
        if gap is not None:
            overlay_summary = trade_overlay.merge(
                store, portfolio, ShioajiClient(), gap[0], gap[1]
            )
    except Exception:  # noqa: BLE001 — overlay must never abort the backfill
        log.exception("trade_overlay.merge raised; continuing without overlay")

    store.set_meta("last_known_date", today)

    summary = {
        "today": today,
        "floor": floor,
        "tw_skipped": tw_skipped,
        "tw_fetched": tw_fetched,
        "tw_price_rows": tw_rows,
        "fx_rows": fx_summary["fx_rows_written"],
        "foreign_skipped": fr_summary["skipped"],
        "foreign_fetched": fr_summary["fetched"],
        "foreign_price_rows": fr_summary["price_rows_written"],
        "overlay": overlay_summary,
        **derived,
    }
    log.info("full backfill summary: %s", summary)
    return summary


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
        rows = fetch_with_dlq(
            store, "tw_prices", code,
            lambda c=code, s=start, e=end: get_prices(c, "TWD", s, e, store=store),
        )
        if rows is None:
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


# --- Phase 10: failed-tasks DLQ ------------------------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def fetch_with_dlq(
    store: DailyStore,
    task_type: str,
    target: str,
    fn,
    *args,
    **kwargs,
):
    """Wrap an external fetch so a single-symbol failure becomes a row in
    `failed_tasks` instead of aborting the run. Returns fn's value on
    success, or None on failure.

    De-duping rule (per spec §10): an "open" row exists per
    (task_type, target) where resolved_at IS NULL. A second failure for
    the same target bumps `attempts` and updates `last_attempt_at`
    instead of inserting a duplicate. Once a row is resolved, a fresh
    failure creates a new open row.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — boundary by design
        message = f"{type(exc).__name__}: {exc}"
        now = _now_utc_iso()
        log.warning(
            "fetch_with_dlq: %s/%s failed: %s", task_type, target, message
        )
        with store.connect_rw() as conn:
            existing = conn.execute(
                """
                SELECT id, attempts FROM failed_tasks
                WHERE task_type = ? AND target = ? AND resolved_at IS NULL
                """,
                (task_type, target),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    """
                    UPDATE failed_tasks
                    SET attempts = ?, last_attempt_at = ?, error_message = ?
                    WHERE id = ?
                    """,
                    (existing["attempts"] + 1, now, message, existing["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO failed_tasks(
                        task_type, target, error_message,
                        attempts, first_seen_at, last_attempt_at
                    ) VALUES (?, ?, ?, 1, ?, ?)
                    """,
                    (task_type, target, message, now, now),
                )
        return None


def retry_open_tasks(store: DailyStore, resolver) -> dict[str, int]:
    """Walk every open failed_tasks row and retry it.

    `resolver(row) -> callable`: caller-supplied factory that returns
    the no-arg fn to retry for a given row. On success, sets resolved_at
    on the row. On failure, bumps attempts.

    Used by the /api/admin/retry-failed endpoint and by
    scripts/retry_failed_tasks.py. The split between this function and
    fetch_with_dlq is deliberate: fetch_with_dlq runs *during* a
    backfill, retry_open_tasks runs against the persisted DLQ later.
    """
    with store.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM failed_tasks WHERE resolved_at IS NULL"
        ).fetchall()]

    resolved = 0
    still_failing = 0
    for row in rows:
        retry_fn = resolver(row)
        try:
            retry_fn()
        except Exception as exc:  # noqa: BLE001 — same boundary
            now = _now_utc_iso()
            with store.connect_rw() as conn:
                conn.execute(
                    """
                    UPDATE failed_tasks
                    SET attempts = attempts + 1,
                        last_attempt_at = ?,
                        error_message = ?
                    WHERE id = ?
                    """,
                    (now, f"{type(exc).__name__}: {exc}", row["id"]),
                )
            still_failing += 1
            continue
        now = _now_utc_iso()
        with store.connect_rw() as conn:
            conn.execute(
                "UPDATE failed_tasks SET resolved_at = ? WHERE id = ?",
                (now, row["id"]),
            )
        resolved += 1

    return {"resolved": resolved, "still_failing": still_failing}


# --- Phase 9: background thread + state machine --------------------------


def _data_already_ready(store: DailyStore) -> bool:
    """READY shortcut: portfolio_daily has at least one row, so we don't
    need to re-fetch on every Flask boot."""
    return store.get_today_snapshot() is not None


def _worker(store: DailyStore, portfolio_path: Path) -> None:
    """Body of the background backfill thread.

    Wraps run_full_backfill in a top-level try/except so any unhandled
    exception (network, schema drift, FK violation) becomes a FAILED
    state instead of silently killing the daemon.
    """
    state = backfill_state.get()
    state.mark_initializing()
    try:
        log.info("backfill worker: starting")
        run_full_backfill(store, portfolio_path)
        state.mark_ready()
        log.info("backfill worker: READY")
    except Exception as exc:  # noqa: BLE001 — top-level guard
        log.exception("backfill worker: FAILED")
        state.mark_failed(f"{type(exc).__name__}: {exc}")


def start(
    store: DailyStore, portfolio_path: Path | str
) -> threading.Thread | None:
    """Spawn the daemon backfill thread, or no-op if already running /
    data already populated.

    Returns:
      - the new (or live) Thread on a real spawn,
      - None if data was already READY (no work to do).
    """
    global _active_thread
    portfolio_path = Path(portfolio_path)

    with _thread_lock:
        if _active_thread is not None and _active_thread.is_alive():
            log.info("backfill start: thread already running")
            return _active_thread

        if _data_already_ready(store):
            log.info("backfill start: data already populated, marking READY")
            backfill_state.get().mark_ready()
            return None

        t = threading.Thread(
            target=_worker,
            args=(store, portfolio_path),
            name="backfill-worker",
            daemon=True,
        )
        _active_thread = t
        t.start()
        return t
