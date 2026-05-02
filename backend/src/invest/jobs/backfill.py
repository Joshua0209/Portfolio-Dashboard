"""Cold-start backfill — daemon-thread wrapper around the daily-state
machine + per-symbol fetch orchestration + positions materialization.

The fetch_orchestrator parameter is the seam between the state-
machine wrapper and the actual price/FX fetching code. Phase 2's
invest.prices.price_service.fetch_and_store and
invest.prices.fx_provider.fetch_and_store_fx satisfy the orchestrator
contract for production runs (Cycle 51 wires them up). Tests pass a
no-op or seeding lambda — keeping yfinance out of the test path.

This module deliberately doesn't drive per-symbol iteration itself.
The legacy backfill_runner.py fused fetch orchestration with state
management in a 1,725-line monolith; Phase 7 keeps state management
here and pushes fetch loops into invest.prices (where the
single-source-of-truth DLQ logic already lives).
"""
from __future__ import annotations

import logging
import threading
from datetime import date as _date
from typing import Callable, Optional

from sqlmodel import Session, select

from invest.core import state as state_module
from invest.jobs import _positions
from invest.persistence.models.portfolio_daily import PortfolioDaily

log = logging.getLogger(__name__)

FetchOrchestrator = Callable[[Session, _date, _date], None]
SessionFactory = Callable[[], Session]


_thread_lock = threading.Lock()
_active_thread: Optional[threading.Thread] = None


def data_already_ready(session: Session) -> bool:
    first = session.exec(select(PortfolioDaily).limit(1)).first()
    return first is not None


def run_full_backfill(
    session: Session,
    *,
    start: _date,
    end: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> dict[str, int]:
    state = state_module.get()
    state.mark_initializing()
    try:
        fetch_orchestrator(session, start, end)
        result = _positions.build_daily(session, start, end)
    except Exception as exc:
        state.mark_failed(str(exc))
        log.exception("backfill failed")
        raise
    state.mark_ready()
    return result


def _worker(
    session_factory: SessionFactory,
    start: _date,
    end: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> None:
    with session_factory() as session:
        try:
            run_full_backfill(
                session,
                start=start,
                end=end,
                fetch_orchestrator=fetch_orchestrator,
            )
        except Exception:
            log.exception("backfill worker exited with error")


def start(
    session_factory: SessionFactory,
    *,
    start: _date,
    end: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> Optional[threading.Thread]:
    global _active_thread

    with _thread_lock:
        if _active_thread is not None and _active_thread.is_alive():
            log.info("backfill start: thread already running")
            return _active_thread

        with session_factory() as probe_session:
            if data_already_ready(probe_session):
                log.info(
                    "backfill start: data already populated, marking READY"
                )
                state_module.get().mark_ready()
                return None

        t = threading.Thread(
            target=_worker,
            args=(session_factory, start, end, fetch_orchestrator),
            name="invest-backfill-worker",
            daemon=True,
        )
        _active_thread = t
        t.start()
        return t


def _reset_thread_for_test() -> None:
    global _active_thread
    with _thread_lock:
        _active_thread = None
