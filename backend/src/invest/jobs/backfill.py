"""Cold-start daily backfill — production path + SQLModel scaffold.

Phase 14.3c: this module hosts BOTH the production path (ported byte-
identically from the retired ``app/backfill_runner.py`` monolith — the
DailyStore + portfolio.json + raw-SQL writer that backs
``scripts/backfill_daily.py``) AND the SQLModel-backed scaffold
(``run_full_backfill_sqlmodel`` / ``start_sqlmodel`` / ``_worker_sqlmodel``)
that will become canonical when the request path moves to
``trades``-table aggregation (PLAN-modularization §14.4+).

Co-located by the same precedent as Phase 14.2's ``jobs.snapshot``:
the SQLModel scaffold keeps its tests; the production helpers retain
the canonical entry-point names so consumers (``scripts/backfill_daily.py``,
``scripts/snapshot_daily.py``, the lifespan hook) need only swap the
module path.

Production entry points (DailyStore-backed):
  run_full_backfill(store, portfolio_path) — orchestrates TW + FX +
                                              foreign + benchmark fetch,
                                              overlay merge, and derive.
  run_tw_backfill(store, portfolio_path)   — TW-only subset for smoke.
  start(store, portfolio_path)             — daemon-thread wrapper with
                                              the INITIALIZING / READY /
                                              FAILED state machine.

SQLModel scaffold (Trade-table aggregator, future canonical):
  run_full_backfill_sqlmodel(session, *, start, end, fetch_orchestrator)
  start_sqlmodel(session_factory, *, start, end, fetch_orchestrator)
  data_already_ready(session) — READY shortcut against PortfolioDaily.
"""
from __future__ import annotations

import calendar
import json
import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from datetime import date as _date
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Optional

from sqlmodel import Session, select

from invest.core import state as backfill_state
from invest.core import state as state_module
from invest.jobs import _positions
from invest.persistence.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.prices.sources import get_yfinance_prices

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLModel scaffold path (Phase 14.3+, future canonical)
# ---------------------------------------------------------------------------


FetchOrchestrator = Callable[[Session, _date, _date], None]
SessionFactory = Callable[[], Session]


_thread_lock = threading.Lock()
_active_thread: Optional[threading.Thread] = None


def data_already_ready(session: Session) -> bool:
    first = session.exec(select(PortfolioDaily).limit(1)).first()
    return first is not None


def run_full_backfill_sqlmodel(
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


def _worker_sqlmodel(
    session_factory: SessionFactory,
    start: _date,
    end: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> None:
    with session_factory() as session:
        try:
            run_full_backfill_sqlmodel(
                session,
                start=start,
                end=end,
                fetch_orchestrator=fetch_orchestrator,
            )
        except Exception:
            log.exception("backfill worker exited with error")


def start_sqlmodel(
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
            target=_worker_sqlmodel,
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


# ---------------------------------------------------------------------------
# Production path — DailyStore + portfolio.json (canonical until Phase 14.4+)
# ---------------------------------------------------------------------------
#
# Helpers below are ported byte-identically from the retired
# ``app/backfill_runner.py`` monolith (Phase 14.3c). They drive the
# request path until the SQLModel-backed entry points above replace
# them in PLAN-modularization §14.4+.


# --- Phase 14.3a: SQLModel-session helper for price_service routing -------


@contextmanager
def _with_session(store: DailyStore) -> Iterator[Any]:
    """Yield a SQLModel Session bound to the same SQLite file as `store`.

    The price-fetch path now goes through ``invest.prices.price_service``
    which expects PriceRepo + FailedTaskRepo + SymbolMarketRepo wired
    onto a session. We open a short-lived engine + session per fetch
    site — same pattern as scripts/retry_failed_tasks.py.
    """
    from sqlmodel import Session, create_engine

    engine = create_engine(
        f"sqlite:///{store.path}",
        connect_args={"timeout": 5},
    )
    try:
        with Session(engine) as session:
            yield session
    finally:
        engine.dispose()


class _YFinancePriceClient:
    """Module-function wrapper exposing the PriceClient Protocol shape.

    ``invest.prices.yfinance_client`` is a module of free functions;
    ``price_service`` wants an object with ``fetch_prices(symbol,
    start, end)``. Wrapping per-fetch keeps the indirection cheap and
    lets tests monkeypatch ``yfinance_client.fetch_prices`` directly.
    """

    def fetch_prices(self, symbol: str, start: str, end: str) -> list[dict]:
        from invest.prices import yfinance_client as _yfc

        return _yfc.fetch_prices(symbol, start, end)


def _fetch_range_via_price_service(
    store: DailyStore,
    symbol: str,
    currency: str,
    start: str,
    end: str,
) -> int:
    """Adapt price_service.fetch_and_store_range to the store-backed
    backfill loops.

    Returns the count of price rows persisted (0 on miss/failure). DLQ
    rows are written by price_service inside the same session.
    """
    from datetime import date as _date2

    from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
    from invest.persistence.repositories.price_repo import PriceRepo
    from invest.persistence.repositories.symbol_market_repo import (
        SymbolMarketRepo,
    )
    from invest.prices import price_service

    start_d = _date2.fromisoformat(start)
    end_d = _date2.fromisoformat(end)
    with _with_session(store) as session:
        return price_service.fetch_and_store_range(
            symbol,
            currency,
            start_d,
            end_d,
            price_repo=PriceRepo(session),
            dlq=FailedTaskRepo(session),
            client=_YFinancePriceClient(),
            market_repo=SymbolMarketRepo(session),
        )


# File lives at <root>/backend/src/invest/jobs/backfill.py.
# Walk up four parents to land on the project root.
_PROJECT_ROOT_STR = str(Path(__file__).resolve().parents[4])
_HOME_STR = str(Path.home())


def _sanitize_error_message(msg: str) -> str:
    """Strip absolute filesystem paths from DLQ-persisted exception text.

    `failed_tasks.error_message` is exposed via the unauthenticated
    `/api/admin/failed-tasks` endpoint; full paths leak host layout when
    the dashboard is reachable from a tunnel or LAN. Replace project
    root and $HOME with placeholders. Truncate to 500 chars to keep
    pathological tracebacks from filling the row."""
    if not msg:
        return msg
    out = msg.replace(_PROJECT_ROOT_STR, "<project>").replace(_HOME_STR, "~")
    if len(out) > 500:
        out = out[:497] + "..."
    return out


# Module-level so a second start() in the same process doesn't double-spawn.
_prod_thread_lock = threading.Lock()
_prod_active_thread: threading.Thread | None = None


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


def describe_skip(
    trade_dates: Iterable[str],
    held_months: Iterable[str],
    floor: str,
) -> str:
    """Human-readable explanation for why compute_fetch_window returned None.

    Mirrors the early-return cases in compute_fetch_window so the CLI can
    surface "why was this symbol skipped?" without re-deriving it.
    """
    trade_list = sorted({_normalize_trade_date(d) for d in trade_dates})
    held_list = sorted(set(held_months))
    if not trade_list and not held_list:
        return "no trades or holdings on file"
    last_trade = trade_list[-1] if trade_list else None
    last_held_date = month_end_iso(held_list[-1]) if held_list else None
    last_activity = max(d for d in (last_trade, last_held_date) if d)
    if last_activity < floor:
        return f"last activity {last_activity} predates floor {floor}"
    return "no overlap with backfill window"


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

    Phase 14.3a: writes SQLModel-shape columns (no `fetched_at`/PK on
    (date, symbol); instead `ingested_at` + autoincrement id +
    UNIQUE constraint on (date, symbol)). The benchmark backfill is
    the last in-tree caller; portfolio TW + foreign price-fetch sites
    now route through ``price_service.fetch_and_store_range``.
    """
    if not rows:
        return 0
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with store.connect_rw() as conn:
        for r in rows:
            conn.execute(
                """
                INSERT INTO prices(date, symbol, close, currency, source, ingested_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, symbol) DO UPDATE SET
                    close = excluded.close,
                    currency = excluded.currency,
                    source = excluded.source,
                    ingested_at = excluded.ingested_at
                """,
                (r["date"], r["symbol"], r["close"], r["currency"], r["source"], now),
            )
    return len(rows)


class _YFinanceFxClient:
    """Module-function wrapper exposing the FxClient Protocol shape.

    Symmetric to :class:`_YFinancePriceClient` (Phase 14.3a) — wraps
    ``invest.prices.yfinance_client.fetch_fx`` so ``fx_provider`` can
    be wired via constructor injection.
    """

    def fetch_fx(self, ccy: str, start: str, end: str) -> list[dict]:
        from invest.prices import yfinance_client as _yfc

        return _yfc.fetch_fx(ccy, start, end)


def _fetch_range_via_fx_provider(
    store: DailyStore,
    ccy: str,
    start: str,
    end: str,
) -> int:
    """Adapt fx_provider.fetch_and_store_range to the store-backed
    backfill loops.

    Returns the count of FX rows persisted (0 on miss/failure or TWD
    identity short-circuit). DLQ rows are written by fx_provider
    inside the same session.
    """
    from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
    from invest.persistence.repositories.fx_repo import FxRepo
    from invest.prices import fx_provider

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    with _with_session(store) as session:
        return fx_provider.fetch_and_store_range(
            ccy,
            start_d,
            end_d,
            fx_repo=FxRepo(session),
            dlq=FailedTaskRepo(session),
            client=_YFinanceFxClient(),
        )


def run_fx_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> dict[str, Any]:
    """Populate fx_rates for every foreign currency in scope across
    [floor, today]. Per spec §6.1, FX is dense across the whole equity-curve
    window, not per-symbol — the curve always needs a TWD reference.

    Phase 14.3b: routes through ``fx_provider.fetch_and_store_range``
    instead of ``get_fx_rates`` + ``_persist_fx_rows``. DLQ writes
    happen inside ``fx_provider`` (SQLModel-shape).
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()

    rows_written = 0
    by_ccy: dict[str, int] = {}
    for ccy in sorted(_foreign_currencies_in_scope(portfolio)):
        log.info("fx backfill: %s [%s..%s]", ccy, floor, today)
        n = _fetch_range_via_fx_provider(store, ccy, floor, today)
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
    using the same per-symbol fetch-window logic as the TW backfill.

    Phase 14.3a: routes through ``price_service.fetch_and_store_range``
    instead of ``get_prices`` + ``_persist_symbol_prices``. DLQ writes
    happen inside ``price_service`` (SQLModel-shape).
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    skipped: list[str] = []
    skip_reasons: dict[str, str] = {}
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
            skip_reasons[code] = describe_skip(
                entry["trade_dates"], entry["held_months"], floor,
            )
            continue
        if only_codes is not None and code not in only_codes:
            continue
        if limit is not None and processed >= limit:
            break
        start, end = window
        log.info("foreign backfill: %s [%s..%s]", code, start, end)
        n = _fetch_range_via_price_service(store, code, currency, start, end)
        if n > 0:
            rows_written += n
            fetched.append(code)
        processed += 1

    return {
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "fetched": fetched,
        "price_rows_written": rows_written,
    }


def run_benchmark_backfill(
    store: DailyStore,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> dict[str, Any]:
    """Fetch daily yfinance prices for benchmark strategy tickers.

    Strategy tickers (`0050.TW`, `SPY`, `QQQ`, etc.) are fetched directly
    via yfinance — they bypass the TW/foreign router because their Yahoo
    symbols already carry the venue suffix and the router would otherwise
    probe TWSE for symbols that don't exist there. Rows land in the same
    `prices` table as portfolio tickers; key collisions are impossible
    because portfolio rows use bare codes (`0050`, `2330`) while strategy
    rows use Yahoo-suffixed (`0050.TW`, `2330.TW`).
    """
    from invest import benchmarks as bm  # local import to avoid eager yfinance import

    today = today or _today_iso()
    tickers: set[tuple[str, str]] = set()
    for strat in bm.STRATEGIES:
        ccy = "TWD" if strat.market == "TW" else "USD"
        for t in strat.weights:
            tickers.add((t, ccy))

    fetched: list[str] = []
    rows_written = 0
    for ticker, ccy in sorted(tickers):
        log.info("benchmark backfill: %s [%s..%s]", ticker, floor, today)
        rows = fetch_with_dlq(
            store, "benchmark_prices", ticker,
            lambda t=ticker, s=floor, e=today, td=today: get_yfinance_prices(
                t, s, e, store=store, today=td,
            ),
        )
        if rows is None:
            continue
        # Tag with symbol/currency/source — get_yfinance_prices returns
        # bare {date, close, volume} rows (the price_sources router does
        # the tagging in the normal portfolio path, but we bypass it here).
        tagged = [
            {**r, "symbol": ticker, "currency": ccy, "source": "yfinance"}
            for r in rows
        ]
        rows_written += _persist_symbol_prices(store, ticker, tagged)
        fetched.append(ticker)

    return {"fetched": fetched, "price_rows_written": rows_written}


@dataclass
class FetchTask:
    """One unit of network work for the round-robin orchestrator.

    Each task carries everything needed to execute, persist, and (on
    second-pass failure) emit a DLQ row. `upstream` groups tasks for
    round-robin scheduling and stats accumulation; `dlq_task_type`
    matches the existing `failed_tasks.task_type` taxonomy."""

    upstream: str           # tw | fx | foreign | benchmark
    target: str             # symbol or ccy — used in DLQ writes + log lines
    descriptor: str         # human label for log lines, e.g. "2330 [..]"
    dlq_task_type: str      # tw_prices | fx_rates | foreign_prices | benchmark_prices
    fetch_fn: "Callable[[], list[dict]]"
    persist_fn: "Callable[[list[dict]], int]"


def _round_robin(queues: dict[str, list[FetchTask]]) -> Iterable[FetchTask]:
    """Yield one task per non-empty queue per cycle until all drain.

    Insertion order of the queues dict defines the rotation order:
    tw -> fx -> foreign -> benchmark -> tw -> ... This spreads
    consecutive upstream calls across different hosts so no single one
    sees back-to-back hits (yfinance throttles aggressively on bursts)."""
    while any(queues.values()):
        for upstream in list(queues.keys()):
            if queues[upstream]:
                yield queues[upstream].pop(0)


def _try_fetch(fn) -> tuple[Any, BaseException | None]:
    """Call fn; return (rows, None) on success, (None, exc) on failure.
    Distinct from fetch_with_dlq: this never writes to the DLQ — that's
    the caller's choice based on which retry pass we're on."""
    try:
        return (fn(), None)
    except Exception as exc:  # noqa: BLE001 — boundary by design
        return (None, exc)


def _record_dlq_failure(
    store: DailyStore, task_type: str, target: str, exc: BaseException
) -> None:
    """Mirror of fetch_with_dlq's exception branch — writes / bumps a row
    in failed_tasks. Used by the deferred-retry pass when a task fails a
    second time.

    Phase 14.3a: writes SQLModel-shape columns
    (``payload`` JSON with ``{"target": target}``, ``error``,
    ``first_failed_at``, ``last_failed_at``). The legacy
    ``target``/``error_message``/``first_seen_at``/``last_attempt_at``
    columns no longer exist after the SQLModel-canonical schema lands.
    """
    message = _sanitize_error_message(f"{type(exc).__name__}: {exc}")
    now = _now_utc_iso()
    payload_json = json.dumps({"target": target})
    log.warning("retry pass: %s/%s failed again: %s", task_type, target, message)
    with store.connect_rw() as conn:
        existing = conn.execute(
            """
            SELECT id, attempts FROM failed_tasks
            WHERE task_type = ?
              AND json_extract(payload, '$.target') = ?
              AND resolved_at IS NULL
            """,
            (task_type, target),
        ).fetchone()
        if existing is not None:
            conn.execute(
                """
                UPDATE failed_tasks
                SET attempts = ?, last_failed_at = ?, error = ?
                WHERE id = ?
                """,
                (existing["attempts"] + 1, now, message, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO failed_tasks(
                    task_type, payload, error,
                    attempts, first_failed_at, last_failed_at
                ) VALUES (?, ?, ?, 1, ?, ?)
                """,
                (task_type, payload_json, message, now, now),
            )


def _build_tw_tasks(
    store: DailyStore,
    portfolio: dict,
    floor: str,
    today: str,
    latest_month: str,
) -> tuple[list[FetchTask], list[str], dict[str, str]]:
    """Build TW fetch tasks routed through ``price_service.fetch_and_store_range``.

    Phase 14.3a: per-task ``fetch_fn`` opens a SQLModel session against
    the store, calls price_service, and returns its persisted-row count
    wrapped in a synthetic single-element list so the round-robin
    orchestrator's persist_fn (count-the-rows) treats it correctly.
    DLQ writes happen inside price_service — the orchestrator's
    deferred-retry pass will re-call the fetch_fn, which is idempotent
    (open DLQ row gets bumped, success resolves).
    """
    tasks: list[FetchTask] = []
    skipped: list[str] = []
    skip_reasons: dict[str, str] = {}
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
            skipped.append(code)
            skip_reasons[code] = describe_skip(
                entry["trade_dates"], entry["held_months"], floor,
            )
            continue
        start, end = window

        def _make_fetch(c: str, s: str, e: str):
            def _fetch() -> list[dict]:
                n = _fetch_range_via_price_service(store, c, "TWD", s, e)
                # Encode the count as a list of N empty placeholders so
                # the orchestrator's `persist_fn(rows or []) -> len(rows)`
                # convention surfaces the right `tw_price_rows` total.
                return [{}] * n
            return _fetch

        tasks.append(FetchTask(
            upstream="tw",
            target=code,
            descriptor=f"{code} [{start}..{end}]",
            dlq_task_type="tw_prices",
            fetch_fn=_make_fetch(code, start, end),
            persist_fn=(lambda rows: len(rows)),
        ))
    return tasks, skipped, skip_reasons


def _build_fx_tasks(
    store: DailyStore, portfolio: dict, floor: str, today: str,
) -> list[FetchTask]:
    """Build FX fetch tasks routed through ``fx_provider.fetch_and_store_range``.

    Phase 14.3b: same pattern as ``_build_tw_tasks`` (Phase 14.3a) — fetch_fn
    opens a SQLModel session, calls fx_provider, and surfaces an N-placeholder
    list to the orchestrator so the row counts plumb through. The
    orchestrator's deferred-retry / circuit-breaker semantics for FX become
    unreachable: fx_provider catches its own exceptions and writes a DLQ row
    directly. Same trade-off the price-fetch tasks made in 14.3a.
    """
    tasks: list[FetchTask] = []
    for ccy in sorted(_foreign_currencies_in_scope(portfolio)):

        def _make_fetch(c: str, s: str, e: str):
            def _fetch() -> list[dict]:
                n = _fetch_range_via_fx_provider(store, c, s, e)
                return [{}] * n
            return _fetch

        tasks.append(FetchTask(
            upstream="fx",
            target=ccy,
            descriptor=f"{ccy} [{floor}..{today}]",
            dlq_task_type="fx_rates",
            fetch_fn=_make_fetch(ccy, floor, today),
            persist_fn=(lambda rows: len(rows)),
        ))
    return tasks


def _build_foreign_tasks(
    store: DailyStore,
    portfolio: dict,
    floor: str,
    today: str,
    latest_month: str,
) -> tuple[list[FetchTask], list[str], dict[str, str]]:
    """Build foreign fetch tasks routed through ``price_service.fetch_and_store_range``.

    Phase 14.3a: same pattern as ``_build_tw_tasks`` — fetch_fn opens a
    SQLModel session, calls price_service, and surfaces an N-placeholder
    list to the orchestrator so the row counts plumb through.
    """
    tasks: list[FetchTask] = []
    skipped: list[str] = []
    skip_reasons: dict[str, str] = {}
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
            skip_reasons[code] = describe_skip(
                entry["trade_dates"], entry["held_months"], floor,
            )
            continue
        start, end = window

        def _make_fetch(c: str, ccy: str, s: str, e: str):
            def _fetch() -> list[dict]:
                n = _fetch_range_via_price_service(store, c, ccy, s, e)
                return [{}] * n
            return _fetch

        tasks.append(FetchTask(
            upstream="foreign",
            target=code,
            descriptor=f"{code} ({currency}) [{start}..{end}]",
            dlq_task_type="foreign_prices",
            fetch_fn=_make_fetch(code, currency, start, end),
            persist_fn=(lambda rows: len(rows)),
        ))
    return tasks, skipped, skip_reasons


def _build_benchmark_tasks(
    store: DailyStore, floor: str, today: str,
) -> list[FetchTask]:
    from invest import benchmarks as bm  # local import: avoid eager yfinance load
    seen: set[tuple[str, str]] = set()
    for strat in bm.STRATEGIES:
        ccy = "TWD" if strat.market == "TW" else "USD"
        for t in strat.weights:
            seen.add((t, ccy))

    tasks: list[FetchTask] = []
    for ticker, ccy in sorted(seen):
        def make_persist(t: str, c: str):
            def persist(rows: list[dict]) -> int:
                tagged = [
                    {**r, "symbol": t, "currency": c, "source": "yfinance"}
                    for r in rows
                ]
                return _persist_symbol_prices(store, t, tagged)
            return persist

        tasks.append(FetchTask(
            upstream="benchmark",
            target=ticker,
            descriptor=f"{ticker} [{floor}..{today}]",
            dlq_task_type="benchmark_prices",
            fetch_fn=(lambda t=ticker, s=floor, e=today:
                      get_yfinance_prices(t, s, e, store=store, today=today)),
            persist_fn=make_persist(ticker, ccy),
        ))
    return tasks


def run_full_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
    max_failures_per_market: int = 3,
) -> dict[str, Any]:
    """End-to-end backfill: TW + FX + foreign + benchmark prices, derived
    positions, Shioaji overlay.

    Round-robin scheduling across upstreams (tw -> fx -> foreign -> benchmark)
    spreads consecutive calls across different APIs so no single upstream
    sees back-to-back hits. On per-task failure, the task is deferred to
    a single retry pass; second-pass failures land in `failed_tasks`.

    Circuit breaker: when an upstream accumulates `max_failures_per_market`
    fetch failures (across both passes), every remaining task in that
    upstream is short-circuited - both not-yet-attempted first-pass tasks
    and already-deferred tasks. A circuit-broken task still gets a DLQ row
    so the operator can see what was abandoned.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    # Build queues - task descriptors capture closures over (start, end, today).
    tw_tasks, tw_skipped, tw_skip_reasons = _build_tw_tasks(
        store, portfolio, floor, today, latest_month,
    )
    fx_tasks = _build_fx_tasks(store, portfolio, floor, today)
    fr_tasks, fr_skipped, fr_skip_reasons = _build_foreign_tasks(
        store, portfolio, floor, today, latest_month,
    )
    bm_tasks = _build_benchmark_tasks(store, floor, today)

    # Insertion order defines round-robin rotation.
    queues: dict[str, list[FetchTask]] = {
        "tw": list(tw_tasks),
        "fx": list(fx_tasks),
        "foreign": list(fr_tasks),
        "benchmark": list(bm_tasks),
    }

    fetched: dict[str, list[str]] = {"tw": [], "fx": [], "foreign": [], "benchmark": []}
    rows_by: dict[str, int] = {"tw": 0, "fx": 0, "foreign": 0, "benchmark": 0}
    deferred: list[FetchTask] = []
    failures_by_upstream: dict[str, int] = {k: 0 for k in queues}
    tripped: set[str] = set()
    breaker_skipped: dict[str, list[str]] = {k: [] for k in queues}

    def _note_failure(upstream: str) -> None:
        failures_by_upstream[upstream] += 1
        if (
            upstream not in tripped
            and failures_by_upstream[upstream] >= max_failures_per_market
        ):
            tripped.add(upstream)
            log.warning(
                "%s: circuit breaker tripped after %d failures - "
                "skipping remaining tasks in this market",
                upstream, failures_by_upstream[upstream],
            )

    total = sum(len(q) for q in queues.values())
    log.info("=== Phase 1/3: Round-robin fetch (%d task(s)) ===", total)
    for task in _round_robin(queues):
        if task.upstream in tripped:
            breaker_skipped[task.upstream].append(task.target)
            # Record to DLQ so retry_failed_tasks can resume them later;
            # without this row, breaker-skipped first-pass tasks are
            # silently lost - see KNOWN HAZARD comment on run_tw_backfill.
            _record_dlq_failure(
                store, task.dlq_task_type, task.target,
                RuntimeError(
                    f"circuit_breaker: {task.upstream} market exceeded "
                    f"{max_failures_per_market} failures"
                ),
            )
            continue
        log.info("%s: %s", task.upstream, task.descriptor)
        rows, exc = _try_fetch(task.fetch_fn)
        if exc is not None:
            log.warning(
                "%s: %s failed (%s) - deferring", task.upstream, task.target, exc,
            )
            _note_failure(task.upstream)
            deferred.append(task)
            continue
        n = task.persist_fn(rows or [])
        fetched[task.upstream].append(task.target)
        rows_by[task.upstream] += n

    if deferred:
        log.info("=== Phase 2/3: Retry pass (%d deferred) ===", len(deferred))
        for task in deferred:
            if task.upstream in tripped:
                breaker_skipped[task.upstream].append(task.target)
                _record_dlq_failure(
                    store, task.dlq_task_type, task.target,
                    RuntimeError(
                        f"circuit_breaker: {task.upstream} market exceeded "
                        f"{max_failures_per_market} failures"
                    ),
                )
                continue
            log.info("retry %s: %s", task.upstream, task.descriptor)
            rows, exc = _try_fetch(task.fetch_fn)
            if exc is not None:
                _note_failure(task.upstream)
                _record_dlq_failure(store, task.dlq_task_type, task.target, exc)
                continue
            n = task.persist_fn(rows or [])
            fetched[task.upstream].append(task.target)
            rows_by[task.upstream] += n
    else:
        log.info("=== Phase 2/3: Retry pass - skipped (no deferrals) ===")

    log.info("=== Phase 3/3: Overlay + derive positions + portfolio ===")

    # Single-writer architecture (mirrors snapshot_daily.run, 2026-05-01):
    # merge() runs FIRST so trades_overlay is populated before derive()'s
    # cash walk reads it. Bug 2 fix: previously the order was derive ->
    # overlay, so post-PDF broker sells debited mv via overlay's
    # positions_daily writes but never credited cash via derive's trades
    # walk - equity_twd fake-dropped on every overlay rotation day.
    from invest.brokerage import trade_overlay
    from invest.brokerage.shioaji_client import ShioajiClient
    overlay_summary = {"overlay_trades": 0, "skipped_reason": "no_gap"}
    try:
        gap = trade_overlay.compute_gap_window(portfolio, today=today)
        if gap is not None:
            overlay_summary = trade_overlay.merge(
                store, portfolio, ShioajiClient(), gap[0], gap[1]
            )
    except Exception:  # noqa: BLE001 - overlay must never abort the backfill
        log.exception("trade_overlay.merge raised; continuing without overlay")

    derived = _positions._derive_positions_and_portfolio(store, portfolio)

    store.set_meta("last_known_date", today)

    summary = {
        "today": today,
        "floor": floor,
        "tw_skipped": tw_skipped,
        "tw_skip_reasons": tw_skip_reasons,
        "tw_fetched": fetched["tw"],
        "tw_price_rows": rows_by["tw"],
        "fx_rows": rows_by["fx"],
        "foreign_skipped": fr_skipped,
        "foreign_skip_reasons": fr_skip_reasons,
        "foreign_fetched": fetched["foreign"],
        "foreign_price_rows": rows_by["foreign"],
        "benchmark_fetched": fetched["benchmark"],
        "benchmark_price_rows": rows_by["benchmark"],
        "deferred_count": len(deferred),
        "tripped_markets": sorted(tripped),
        "circuit_breaker_skipped": breaker_skipped,
        "max_failures_per_market": max_failures_per_market,
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
    max_failures_per_market: int = 3,
) -> dict[str, Any]:
    """Run a TW-only backfill against the given DailyStore.

    Per-symbol transactions (so progress is visible and crashes don't lose
    earlier work). After all symbols are fetched, derive positions_daily
    and portfolio_daily in their own pass.

    `limit`: optional cap on number of symbols processed (--limit flag in
    the CLI). Useful for smoke tests on a subset without waiting for the
    full ~5-8 minutes that 30+ TW codes x 8 months x ~1.2s/fetch implies
    (the plan's "<=90s" target assumes a smaller portfolio).

    `only_codes`: if set, only fetch these symbols. Skip-tracking still
    runs for the rest.

    `max_failures_per_market`: trip the TW circuit breaker after this many
    fetch failures and skip every remaining symbol. Aligned with the
    multi-market breaker in run_full_backfill.

    KNOWN HAZARD - circuit-breaker silent loss (legacy from TWSE-direct era):
    when the breaker trips, alphabetically-later codes are added to
    `breaker_skipped` *without ever being attempted* and never enter
    `failed_tasks`, so `retry_failed_tasks.py` cannot recover them. A
    cold-start run during a TWSE-WAF flare therefore left ~half the user's
    portfolio without daily prices, and `_derive_positions_and_portfolio`
    silently dropped any mid-month position whose code lacked both a daily
    price and a same-month PDF ref_price (i.e. positions exited before the
    next month-end). This caused systematic mid-month under-counting of
    equity_twd.

    Now mitigated: TW is routed through yfinance (.TW / .TWO probing) in
    app/price_sources.py, removing the TWSE freeze that was the dominant
    failure mode. The breaker is retained as defence-in-depth, but if it
    ever trips again, breaker-skipped codes should also be persisted to
    failed_tasks so retry_failed_tasks.py can resume them.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))

    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    skipped: list[str] = []
    skip_reasons: dict[str, str] = {}
    fetched: list[str] = []
    breaker_skipped: list[str] = []
    rows_written = 0
    failures = 0
    tripped = False

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
            reason = describe_skip(
                entry["trade_dates"], entry["held_months"], floor,
            )
            skipped.append(code)
            skip_reasons[code] = reason
            log.info("backfill: skipping %s (%s)", code, reason)
            continue
        if only_codes is not None and code not in only_codes:
            continue
        if limit is not None and processed >= limit:
            log.info("backfill: --limit reached at %d, remaining symbols deferred", limit)
            break
        if tripped:
            breaker_skipped.append(code)
            _record_dlq_failure(
                store, "tw_prices", code,
                RuntimeError(
                    f"circuit_breaker: tw market exceeded "
                    f"{max_failures_per_market} failures"
                ),
            )
            continue
        start, end = window
        log.info("backfill: %s [%s..%s]", code, start, end)
        # Phase 14.3a: route through price_service. The "fetch failed"
        # signal is captured by counting open DLQ rows after the call;
        # price_service writes its own DLQ row on Outcome A, so we
        # detect failures by checking count_open() before/after.
        before = _count_open_dlq_for(store, "fetch_price", code)
        n = _fetch_range_via_price_service(store, code, "TWD", start, end)
        after = _count_open_dlq_for(store, "fetch_price", code)
        if after > before:
            failures += 1
            if failures >= max_failures_per_market:
                tripped = True
                log.warning(
                    "tw: circuit breaker tripped after %d failures - "
                    "skipping remaining symbols",
                    failures,
                )
            continue
        rows_written += n
        fetched.append(code)
        processed += 1

    derived = _positions._derive_positions_and_portfolio(store, portfolio)
    store.set_meta("last_known_date", today)

    summary = {
        "today": today,
        "floor": floor,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "fetched": fetched,
        "price_rows_written": rows_written,
        "tripped_markets": ["tw"] if tripped else [],
        "circuit_breaker_skipped": {"tw": breaker_skipped} if breaker_skipped else {},
        "max_failures_per_market": max_failures_per_market,
        **derived,
    }
    log.info("backfill summary: %s", summary)
    return summary


# --- Phase 10: failed-tasks DLQ ------------------------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _count_open_dlq_for(
    store: DailyStore, task_type: str, symbol: str
) -> int:
    """Count open DLQ rows for ``(task_type, symbol)`` against the
    SQLModel-shape ``failed_tasks`` table.

    price_service writes ``payload['symbol']`` (not ``payload['target']``);
    fetch_with_dlq writes ``payload['target']``. We accept either so a
    single helper covers both producers.
    """
    with store.connect_ro() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM failed_tasks
            WHERE task_type = ? AND resolved_at IS NULL
              AND (
                  json_extract(payload, '$.symbol') = ?
                  OR json_extract(payload, '$.target') = ?
              )
            """,
            (task_type, symbol, symbol),
        ).fetchone()
    return int(row[0] or 0)


def fetch_with_dlq(
    store: DailyStore,
    task_type: str,
    target: str,
    fn: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Wrap an external fetch so a single-symbol failure becomes a row in
    `failed_tasks` instead of aborting the run. Returns fn's value on
    success, or None on failure.

    De-duping rule (per spec section 10): an "open" row exists per
    (task_type, target) where resolved_at IS NULL. A second failure for
    the same target bumps `attempts` and updates `last_attempt_at`
    instead of inserting a duplicate. Once a row is resolved, a fresh
    failure creates a new open row.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 - boundary by design
        message = _sanitize_error_message(f"{type(exc).__name__}: {exc}")
        now = _now_utc_iso()
        payload_json = json.dumps({"target": target})
        log.warning(
            "fetch_with_dlq: %s/%s failed: %s", task_type, target, message
        )
        with store.connect_rw() as conn:
            existing = conn.execute(
                """
                SELECT id, attempts FROM failed_tasks
                WHERE task_type = ?
                  AND json_extract(payload, '$.target') = ?
                  AND resolved_at IS NULL
                """,
                (task_type, target),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    """
                    UPDATE failed_tasks
                    SET attempts = ?, last_failed_at = ?, error = ?
                    WHERE id = ?
                    """,
                    (existing["attempts"] + 1, now, message, existing["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO failed_tasks(
                        task_type, payload, error,
                        attempts, first_failed_at, last_failed_at
                    ) VALUES (?, ?, ?, 1, ?, ?)
                    """,
                    (task_type, payload_json, message, now, now),
                )
        return None


# Note (Phase 14.4): the legacy `retry_open_tasks` walked the legacy
# `failed_tasks` schema (target/error_message/first_seen_at). Drainage
# now routes through `invest.jobs.retry_failed.run` against the SQLModel
# `failed_tasks` shape (payload/error/first_failed_at). The legacy
# `fetch_with_dlq` producer above stays in place; it's the only legacy-
# format DLQ writer left after Phase 14.3 (used by the benchmark fetch
# path).


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
    except Exception as exc:  # noqa: BLE001 - top-level guard
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
    global _prod_active_thread
    portfolio_path = Path(portfolio_path)

    with _prod_thread_lock:
        if _prod_active_thread is not None and _prod_active_thread.is_alive():
            log.info("backfill start: thread already running")
            return _prod_active_thread

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
        _prod_active_thread = t
        t.start()
        return t
