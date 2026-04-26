#!/usr/bin/env python3
"""Cold-start daily-prices backfill.

Reads data/portfolio.json, computes per-symbol fetch windows clipped to
BACKFILL_FLOOR (2025-08-01), pulls prices from TWSE/TPEX (TW), yfinance
(foreign), plus FX rates from yfinance, and populates data/dashboard.db.

Default mode: full backfill (TW + foreign + FX). Use --tw-only to limit
to TW (Phase 3 behavior, useful for incremental testing).

Idempotent — re-running UPSERTs everywhere.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.backfill_runner import run_full_backfill, run_tw_backfill  # noqa: E402
from app.daily_store import DailyStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tw-only", action="store_true",
                        help="Skip foreign + FX (Phase 3 behavior).")
    parser.add_argument("--portfolio", type=Path,
                        default=ROOT / "data" / "portfolio.json",
                        help="Path to portfolio.json (default: data/portfolio.json).")
    parser.add_argument("--db", type=Path,
                        default=ROOT / "data" / "dashboard.db",
                        help="Path to dashboard.db (default: data/dashboard.db).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="DEBUG-level logging on the runner.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap symbols (smoke-testing; --tw-only path only).")
    parser.add_argument("--only", action="append", default=None, metavar="CODE",
                        help="Only backfill these codes (--tw-only path only).")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.portfolio.exists():
        print(f"error: portfolio not found at {args.portfolio}", file=sys.stderr)
        return 1

    store = DailyStore(args.db)
    store.init_schema()

    t0 = time.monotonic()
    if args.tw_only:
        only = set(args.only) if args.only else None
        summary = run_tw_backfill(
            store, args.portfolio, limit=args.limit, only_codes=only,
        )
        elapsed = time.monotonic() - t0
        print(f"--- TW-only backfill complete in {elapsed:.1f}s ---")
        print(f"  fetched:  {len(summary['fetched'])} symbols "
              f"({summary['price_rows_written']} price rows)")
        print(f"  skipped:  {len(summary['skipped'])} symbols (out of floor)")
        print(f"  positions_daily rows: {summary['positions_rows']}")
        print(f"  portfolio_daily rows: {summary['portfolio_rows']}")
        return 0

    summary = run_full_backfill(store, args.portfolio)
    elapsed = time.monotonic() - t0
    print(f"--- full backfill complete in {elapsed:.1f}s ---")
    print(f"  TW fetched:      {len(summary['tw_fetched'])} symbols "
          f"({summary['tw_price_rows']} price rows)")
    print(f"  foreign fetched: {len(summary['foreign_fetched'])} symbols "
          f"({summary['foreign_price_rows']} price rows)")
    print(f"  FX rows:         {summary['fx_rows']}")
    print(f"  positions_daily: {summary['positions_rows']}")
    print(f"  portfolio_daily: {summary['portfolio_rows']}")
    print(f"  meta.last_known_date = {summary['today']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
