#!/usr/bin/env python3
"""Cold-start daily-prices backfill.

Reads data/portfolio.json, computes per-symbol fetch windows clipped to
BACKFILL_FLOOR (2025-08-01), pulls prices from yfinance (TW symbols via
`.TW`/`.TWO` suffix, foreign as bare tickers) plus FX rates, and
populates data/dashboard.db.

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

# Mirror app/__init__.py:_load_env so the cold-start CLI sees Shioaji
# creds (and other env vars) without the operator having to source .env
# in their shell first. override=False keeps real shell env wins.
try:
    from dotenv import load_dotenv  # noqa: E402
    _ENV_PATH = ROOT / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH, override=False)
except ImportError:
    pass

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
    parser.add_argument("--max-failures-per-market", type=int, default=3,
                        metavar="N",
                        help="Trip the circuit breaker after N fetch failures "
                             "in a single market (tw/fx/foreign/benchmark) and "
                             "skip its remaining tasks. Default 3.")
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
            max_failures_per_market=args.max_failures_per_market,
        )
        elapsed = time.monotonic() - t0
        _render_tw_only_summary(summary, store, elapsed)
        return 0

    summary = run_full_backfill(
        store, args.portfolio,
        max_failures_per_market=args.max_failures_per_market,
    )
    elapsed = time.monotonic() - t0
    _render_full_summary(summary, store, elapsed)
    return 0


# --- Rendering ------------------------------------------------------------


def _format_duration(seconds: float) -> str:
    """4.2s for under a minute; 4m 5s past that."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(int(round(seconds)), 60)
    return f"{minutes}m {secs:02d}s"


def _section(title: str) -> None:
    bar = "─" * max(len(title), 50)
    print()
    print(bar)
    print(title)
    print(bar)


def _render_skipped(skipped: list[str], skip_reasons: dict[str, str]) -> None:
    if not skipped:
        print("  (none)")
        return
    width = max(len(c) for c in skipped)
    for code in sorted(skipped):
        reason = skip_reasons.get(code, "unknown")
        print(f"  {code:<{width}}  {reason}")


def _render_dlq(store: "DailyStore") -> None:
    failed = store.get_failed_tasks()
    if not failed:
        print("  (clean)")
        return
    width = max(len(t["target"]) for t in failed)
    for t in failed:
        print(
            f"  {t['task_type']:<14} {t['target']:<{width}}  "
            f"attempts={t['attempts']}  {t['error_message']}"
        )


def _render_tripped(summary: dict) -> None:
    tripped = summary.get("tripped_markets") or []
    if not tripped:
        print("  (none)")
        return
    breaker_skipped = summary.get("circuit_breaker_skipped") or {}
    threshold = summary.get("max_failures_per_market", "?")
    for upstream in tripped:
        targets = breaker_skipped.get(upstream, [])
        print(f"  {upstream}: tripped at {threshold} failures "
              f"— {len(targets)} task(s) skipped")
        for t in targets[:10]:
            print(f"    - {t}")
        if len(targets) > 10:
            print(f"    ... and {len(targets) - 10} more")


def _render_tw_only_summary(
    summary: dict, store: "DailyStore", elapsed: float
) -> None:
    _section(f"TW-only backfill complete in {_format_duration(elapsed)}")
    print(f"  fetched:              {len(summary['fetched'])} symbols "
          f"({summary['price_rows_written']} price rows)")
    print(f"  positions_daily rows: {summary['positions_rows']}")
    print(f"  portfolio_daily rows: {summary['portfolio_rows']}")
    print(f"  meta.last_known_date: {summary['today']}")

    _section(f"Skipped — {len(summary['skipped'])} symbol(s)")
    _render_skipped(summary["skipped"], summary.get("skip_reasons", {}))

    _section("Circuit breaker — tripped markets")
    _render_tripped(summary)

    _section("DLQ — open failed tasks")
    _render_dlq(store)


def _render_full_summary(
    summary: dict, store: "DailyStore", elapsed: float
) -> None:
    _section(f"Full backfill complete in {_format_duration(elapsed)}")
    print(f"  TW fetched:           {len(summary['tw_fetched'])} symbols "
          f"({summary['tw_price_rows']} price rows)")
    print(f"  foreign fetched:      {len(summary['foreign_fetched'])} symbols "
          f"({summary['foreign_price_rows']} price rows)")
    print(f"  benchmark fetched:    {len(summary['benchmark_fetched'])} symbols "
          f"({summary['benchmark_price_rows']} price rows)")
    print(f"  FX rows:              {summary['fx_rows']}")
    print(f"  positions_daily rows: {summary['positions_rows']}")
    print(f"  portfolio_daily rows: {summary['portfolio_rows']}")
    overlay = summary.get("overlay", {})
    overlay_n = overlay.get("overlay_trades", 0)
    overlay_note = overlay.get("skipped_reason") or "applied"
    print(f"  Shioaji overlay:      {overlay_n} trade(s) ({overlay_note})")
    deferred = summary.get("deferred_count", 0)
    print(f"  retried tasks:        {deferred} (deferred and retried mid-run)")
    print(f"  meta.last_known_date: {summary['today']}")
    print(f"  backfill floor:       {summary['floor']}")

    tw_skipped = summary.get("tw_skipped", [])
    fr_skipped = summary.get("foreign_skipped", [])
    total_skipped = len(tw_skipped) + len(fr_skipped)
    _section(f"Skipped — {total_skipped} symbol(s)")
    if tw_skipped:
        print("  TW:")
        _render_skipped(tw_skipped, summary.get("tw_skip_reasons", {}))
    if fr_skipped:
        print("  Foreign:")
        _render_skipped(fr_skipped, summary.get("foreign_skip_reasons", {}))
    if not total_skipped:
        print("  (none)")

    _section("Circuit breaker — tripped markets")
    _render_tripped(summary)

    _section("DLQ — open failed tasks")
    _render_dlq(store)


if __name__ == "__main__":
    raise SystemExit(main())
