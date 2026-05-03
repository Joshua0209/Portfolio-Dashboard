"""scripts/retry_failed_tasks.py — DLQ retry CLI (Phase 14.4).

Walk every open ``failed_tasks`` row and retry it via the live fetch
helpers. Mirrors ``POST /api/admin/retry-failed`` so an operator can
drain the DLQ without standing up FastAPI.

Routes through ``invest.jobs.retry_failed.run`` (Phase 14.4) — the
modular SQLModel-backed entry point. The resolver builder constructed
here adapts ``FailedTask.task_type`` + ``FailedTask.payload['target']``
to the same price/fx fetch helpers the cold-start backfill uses.

Usage:
    python scripts/retry_failed_tasks.py [/path/to/dashboard.db]

Exits 0 when every open row resolved, 1 when at least one is still
failing — same shell-pipeline contract as ``scripts/validate_data.py``.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
_BACKEND_SRC = PROJECT_ROOT / "backend" / "src"
if str(_BACKEND_SRC) not in sys.path:
    sys.path.insert(0, str(_BACKEND_SRC))

from sqlmodel import Session, SQLModel, create_engine  # noqa: E402

from invest.jobs import backfill_runner, retry_failed  # noqa: E402
from invest.persistence.daily_store import DailyStore  # noqa: E402
from invest.persistence.models.failed_task import FailedTask  # noqa: E402

log = logging.getLogger("retry_failed_tasks")


def build_resolver(store: DailyStore) -> Callable[[FailedTask], Callable[[], None]]:
    """Return a resolver(task) -> callable for ``retry_failed.run``.

    Mirrors the request-scoped resolver in the FastAPI admin router.
    Inlines instead of importing so the CLI runs without a FastAPI
    app context.

    The callable MUST fetch AND persist the rows for the given DLQ
    entry. The drain in ``retry_failed.run`` discards the return value
    — a fetch-only resolver would mark the row resolved while losing
    the data. (Same contract as the legacy ``retry_open_tasks``.)
    """
    from invest.prices import sources as price_sources

    def resolver(task: FailedTask) -> Callable[[], None]:
        ttype = task.task_type
        target = (task.payload or {}).get("target")
        if not target:
            raise ValueError(
                f"failed_task id={task.id} has no payload['target']; "
                f"payload={task.payload!r}"
            )
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

    db = Path(args.db_path)
    store = DailyStore(db)
    engine = create_engine(f"sqlite:///{db}", connect_args={"timeout": 5})
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        summary = retry_failed.run(session, build_resolver(store))
    log.info(
        "retry_failed_tasks: resolved=%d still_failing=%d",
        summary["resolved"],
        summary["still_failing"],
    )
    return 0 if summary["still_failing"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
