#!/usr/bin/env python3
"""Phase 15 — incremental daily refresh.

Fills the gap between `meta.last_known_date` and "today" without doing a
full cold-start backfill. Two callsites:
  - CLI:        python scripts/snapshot_daily.py
  - Endpoint:   POST /api/admin/refresh  (calls scripts.snapshot_daily.run)

Idempotent: re-running back-to-back is a no-op (`already_current`).
WAL-safe: a Flask process holding the DB open will see the new rows on
its next connect_ro() call without any restart.

Per spec §12 this script must never reference reconciliation. The static
grep test in tests/test_reconcile.py protects that invariant once this
file exists.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore  # noqa: E402

log = logging.getLogger(__name__)


# --- Indirections so tests can pin behavior ---------------------------------


def _today_iso() -> str:
    """Today as YYYY-MM-DD. Indirection for unit tests to pin the date."""
    return date.today().isoformat()


def _next_day(d: str) -> str:
    y, m, dd = (int(p) for p in d.split("-"))
    return date(y, m, dd).fromordinal(date(y, m, dd).toordinal() + 1).isoformat()


def _get_prices(symbol: str, ccy: str, start: str, end: str, store: DailyStore | None = None):
    """Indirection for the price-source router. Tests monkeypatch this
    so they don't hit TWSE / yfinance."""
    from app.price_sources import get_prices
    return get_prices(symbol, ccy, start, end, store=store)


def _get_fx_rates(ccy: str, start: str, end: str):
    from app.price_sources import get_fx_rates
    return get_fx_rates(ccy, start, end)


# --- Window math -----------------------------------------------------------


def compute_increment_window(store: DailyStore) -> tuple[str, str] | None:
    """Return (start, end) for the incremental fetch, or None if the
    store is already at today.

    On a fresh DB with no `last_known_date` meta row yet (cold start
    that hasn't been completed by backfill_runner), the window falls
    back to [BACKFILL_FLOOR, today] so a CLI-only user can populate
    everything in one go.
    """
    today = _today_iso()
    last_known = store.get_meta("last_known_date")
    if last_known is None:
        floor = store.get_meta("backfill_floor") or BACKFILL_FLOOR_DEFAULT
        return (floor, today)
    if last_known >= today:
        return None
    return (_next_day(last_known), today)


# --- Persistence helpers ---------------------------------------------------


def _persist_prices(store: DailyStore, rows: list[dict]) -> int:
    if not rows:
        return 0
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


def _persist_fx(store: DailyStore, ccy: str, rows: list[dict]) -> int:
    if not rows:
        return 0
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
                (r["date"], ccy, r["rate"], r["source"], now),
            )
    return len(rows)


# --- Symbol iteration ------------------------------------------------------


def _held_tw_symbols(portfolio: dict) -> list[str]:
    """Symbols still held at the latest PDF month-end. Snapshots only
    refresh prices for symbols that *currently matter* — historical-only
    codes don't need new bars."""
    months = portfolio.get("months", [])
    if not months:
        return []
    latest = months[-1]
    return [
        h["code"] for h in latest.get("tw", {}).get("holdings", [])
        if h.get("code") and (h.get("qty", 0) or 0) > 0
    ]


def _held_foreign_symbols(portfolio: dict) -> list[tuple[str, str]]:
    """[(symbol, currency)] for non-TW holdings still active at latest month."""
    months = portfolio.get("months", [])
    if not months:
        return []
    latest = months[-1]
    out: list[tuple[str, str]] = []
    for h in latest.get("foreign", {}).get("holdings", []):
        code = h.get("code")
        if code and (h.get("qty", 0) or 0) > 0:
            out.append((code, h.get("ccy") or "USD"))
    return out


# --- Main entry ------------------------------------------------------------


def run(store: DailyStore, portfolio: dict) -> dict[str, Any]:
    """Run one incremental refresh against `store`.

    Returns a summary dict the /api/admin/refresh endpoint surfaces back
    to the UI. Never raises — fetch failures land in failed_tasks via
    backfill_runner.fetch_with_dlq, like the cold-start path.
    """
    window = compute_increment_window(store)
    if window is None:
        return {
            "new_dates": 0,
            "new_rows": 0,
            "skipped_reason": "already_current",
            "window": None,
        }

    start, end = window
    log.info("snapshot_daily: incremental window [%s..%s]", start, end)

    from app import backfill_runner

    new_rows = 0
    new_dates: set[str] = set()

    # 1. TW prices for currently-held codes
    for code in _held_tw_symbols(portfolio):
        rows = backfill_runner.fetch_with_dlq(
            store, "tw_prices", code,
            lambda c=code, s=start, e=end: _get_prices(c, "TWD", s, e, store=store),
        )
        if rows is None:
            continue
        for r in rows:
            new_dates.add(r["date"])
        new_rows += _persist_prices(store, rows)

    # 2. Foreign prices
    for code, ccy in _held_foreign_symbols(portfolio):
        rows = backfill_runner.fetch_with_dlq(
            store, "foreign_prices", code,
            lambda c=code, ccy=ccy, s=start, e=end: _get_prices(c, ccy, s, e, store=store),
        )
        if rows is None:
            continue
        for r in rows:
            new_dates.add(r["date"])
        new_rows += _persist_prices(store, rows)

    # 3. FX (always at least USD, regardless of current foreign holdings)
    needed_ccys: set[str] = {"USD"}
    for _, ccy in _held_foreign_symbols(portfolio):
        if ccy and ccy != "TWD":
            needed_ccys.add(ccy)
    for ccy in sorted(needed_ccys):
        rows = backfill_runner.fetch_with_dlq(
            store, "fx_rates", ccy,
            lambda c=ccy, s=start, e=end: _get_fx_rates(c, s, e),
        )
        if rows is None:
            continue
        new_rows += _persist_fx(store, ccy, rows)

    # 4. Re-derive positions_daily / portfolio_daily for the window
    backfill_runner._derive_positions_and_portfolio(store, portfolio)

    # 5. Phase 11 overlay (no-op without creds; same try/except contract
    # as the cold-start path)
    try:
        from app import trade_overlay
        from app.shioaji_client import ShioajiClient

        gap = trade_overlay.compute_gap_window(portfolio, today=end)
        if gap is not None:
            trade_overlay.merge(store, portfolio, ShioajiClient(), gap[0], gap[1])
    except Exception:  # noqa: BLE001 — overlay must never abort snapshot
        log.exception("trade_overlay.merge raised; continuing without overlay")

    store.set_meta("last_known_date", end)

    summary = {
        "new_dates": len(new_dates),
        "new_rows": new_rows,
        "skipped_reason": None,
        "window": [start, end],
    }
    log.info("snapshot_daily summary: %s", summary)
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--portfolio", default=str(ROOT / "data" / "portfolio.json"))
    p.add_argument("--db", default=str(ROOT / "data" / "dashboard.db"))
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    portfolio_path = Path(args.portfolio)
    if not portfolio_path.exists():
        print(f"error: portfolio.json not found at {portfolio_path}", file=sys.stderr)
        return 2

    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    store = DailyStore(Path(args.db))
    store.init_schema()

    summary = run(store, portfolio)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
