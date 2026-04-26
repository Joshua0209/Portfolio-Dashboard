#!/usr/bin/env python3
"""Cold-start daily-prices backfill (TW-only in Phase 3).

Reads data/portfolio.json, computes per-symbol fetch windows clipped to
BACKFILL_FLOOR (2025-08-01), pulls daily prices from TWSE, and populates
data/dashboard.db. Idempotent — re-running UPSERTs.

Foreign + FX wiring lands in Phase 6; the --tw-only flag is kept now for
forward compatibility with future flags (--no-fx, --skip-overlay).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.backfill_runner import run_tw_backfill  # noqa: E402
from app.daily_store import DailyStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tw-only", action="store_true",
                        help="Skip foreign + FX (Phase 6). Default in Phase 3.")
    parser.add_argument("--portfolio", type=Path,
                        default=ROOT / "data" / "portfolio.json",
                        help="Path to portfolio.json (default: data/portfolio.json).")
    parser.add_argument("--db", type=Path,
                        default=ROOT / "data" / "dashboard.db",
                        help="Path to dashboard.db (default: data/dashboard.db).")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="DEBUG-level logging on the runner.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap the number of symbols backfilled (smoke-testing).")
    parser.add_argument("--only", action="append", default=None, metavar="CODE",
                        help="Only backfill these codes (repeatable: --only 2330 --only 2454).")
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

    only = set(args.only) if args.only else None

    t0 = time.monotonic()
    summary = run_tw_backfill(
        store, args.portfolio, limit=args.limit, only_codes=only,
    )
    elapsed = time.monotonic() - t0

    print(f"--- backfill complete in {elapsed:.1f}s ---")
    print(f"  fetched:  {len(summary['fetched'])} symbols ({summary['price_rows_written']} price rows)")
    print(f"  skipped:  {len(summary['skipped'])} symbols (out of floor)")
    print(f"  positions_daily rows: {summary['positions_rows']}")
    print(f"  portfolio_daily rows: {summary['portfolio_rows']}")
    print(f"  meta.last_known_date = {summary['today']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
