"""scripts/retry_failed_tasks.py — Phase 10 DLQ retry CLI.

Walk every open `failed_tasks` row and retry it via the live fetch
helpers. Mirrors the /api/admin/retry-failed endpoint so an operator
can drain the DLQ without standing up Flask.

Usage:
    python scripts/retry_failed_tasks.py [/path/to/dashboard.db]

Exits 0 when every open row resolved, 1 when at least one is still
failing — matches the pattern used by scripts/validate_data.py so a
shell pipeline can `&&` them together.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app import backfill_runner  # noqa: E402
from app.daily_store import DailyStore  # noqa: E402

log = logging.getLogger("retry_failed_tasks")


def build_resolver(store: DailyStore):
    """Return a resolver(row) -> callable for retry_open_tasks.

    Mirrors app/api/today.py::_build_retry_resolver. We deliberately
    duplicate the dispatch instead of importing the request-scoped one,
    because the CLI must run without a Flask app context.

    Each branch returns a callable that fetches AND persists — see
    retry_open_tasks docstring for the contract.
    """
    from app import price_sources

    def resolver(row):
        ttype = row["task_type"]
        target = row["target"]
        floor = store.get_meta("backfill_floor") or "2025-08-01"
        today = store.get_meta("last_known_date") or floor
        if ttype == "tw_prices":
            def _do() -> None:
                rows = price_sources.get_prices(
                    target, "TWD", floor, today, store=store, today=today,
                )
                backfill_runner._persist_symbol_prices(store, target, rows)
            return _do
        if ttype == "foreign_prices":
            def _do() -> None:
                rows = price_sources.get_prices(
                    target, "USD", floor, today, store=store, today=today,
                )
                backfill_runner._persist_symbol_prices(store, target, rows)
            return _do
        if ttype == "fx_rates":
            def _do() -> None:
                rows = price_sources.get_fx_rates(
                    target, floor, today, store=store, today=today,
                )
                backfill_runner._persist_fx_rows(store, target, rows)
            return _do
        if ttype == "benchmark_prices":
            def _do() -> None:
                rows = price_sources.get_yfinance_prices(
                    target, floor, today, store=store, today=today,
                )
                ccy = "TWD" if target.endswith((".TW", ".TWO")) else "USD"
                tagged = [
                    {**r, "symbol": target, "currency": ccy, "source": "yfinance"}
                    for r in rows
                ]
                backfill_runner._persist_symbol_prices(store, target, tagged)
            return _do
        raise ValueError(f"unknown task_type: {ttype}")

    return resolver


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Retry open failed_tasks rows.")
    parser.add_argument(
        "db_path", nargs="?",
        default=str(PROJECT_ROOT / "data" / "dashboard.db"),
        help="Path to dashboard.db (default: data/dashboard.db).",
    )
    args = parser.parse_args(argv)

    store = DailyStore(Path(args.db_path))
    summary = backfill_runner.retry_open_tasks(store, build_resolver(store))
    log.info(
        "retry_failed_tasks: resolved=%d still_failing=%d",
        summary["resolved"],
        summary["still_failing"],
    )
    return 0 if summary["still_failing"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
