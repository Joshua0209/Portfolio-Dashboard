"""Incremental snapshot — gap-fill from last_known_date to today.

Operator-triggered (POST /api/admin/refresh, scripts/snapshot_daily.py).
Synchronous; no daily-state machine involvement (snapshot runs
against an already-warm layer; the state machine is about cold-
start lifecycle only).

This module hosts two co-located entry points reflecting the in-flight
modularization (Phase 14):

  ``run(store, portfolio)``
      Production path. Operates on ``DailyStore`` (raw SQLite) + the
      PDF-aggregate portfolio dict. Handles TW/foreign price fetch,
      FX gap-fill, the 3-source broker overlay, the post-overlay audit
      hook, and DLQ-backed external fetches via ``backfill_runner``.
      Backs both ``POST /api/admin/refresh`` and
      ``scripts/snapshot_daily.py``.

  ``run_incremental(session, *, today, fetch_orchestrator)``
      SQLModel-backed scaffold for the future Trade-table aggregator
      (Phase 14.3+). Pure orchestration — defers all external fetches
      to the caller-supplied ``fetch_orchestrator`` and walks the
      ``trades`` table via ``_positions.build_daily``. Not yet on the
      request path; covered by ``tests/jobs/test_snapshot.py``.

Per spec the module must never reference reconciliation. The static
grep test in legacy ``tests/test_reconcile.py`` protects that
invariant.
"""
from __future__ import annotations

import logging
from datetime import date as _date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from sqlmodel import Session, desc, select

from invest.jobs import _positions, backfill_runner
from invest.persistence.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore
from invest.persistence.models.portfolio_daily import PortfolioDaily

log = logging.getLogger(__name__)

FetchOrchestrator = Callable[[Session, _date, _date], None]


# ---------------------------------------------------------------------------
# SQLModel scaffold path (Phase 14.3+)
# ---------------------------------------------------------------------------


def find_last_known_date(session: Session) -> Optional[_date]:
    stmt = (
        select(PortfolioDaily)
        .order_by(desc(PortfolioDaily.date))
        .limit(1)
    )
    row = session.exec(stmt).first()
    return row.date if row else None


def run_incremental(
    session: Session,
    *,
    today: _date,
    fetch_orchestrator: FetchOrchestrator,
) -> dict[str, Any]:
    last = find_last_known_date(session)
    if last is None:
        return {
            "skipped_reason": "no_prior_data_call_backfill",
            "positions_rows": 0,
            "portfolio_rows": 0,
        }
    if last >= today:
        return {
            "skipped_reason": "already_up_to_date",
            "positions_rows": 0,
            "portfolio_rows": 0,
            "last_known_date": last.isoformat(),
        }

    gap_start = last + timedelta(days=1)
    fetch_orchestrator(session, gap_start, today)
    result = _positions.build_daily(session, gap_start, today)
    return {
        "skipped_reason": None,
        "gap_start": gap_start.isoformat(),
        "gap_end": today.isoformat(),
        **result,
    }


# ---------------------------------------------------------------------------
# Production path — DailyStore + PortfolioStore (canonical until Phase 14.3)
# ---------------------------------------------------------------------------


def _today_iso() -> str:
    """Today as YYYY-MM-DD. Indirection for unit tests to pin the date."""
    return _date.today().isoformat()


def _next_day(d: str) -> str:
    y, m, dd = (int(p) for p in d.split("-"))
    return _date(y, m, dd).fromordinal(_date(y, m, dd).toordinal() + 1).isoformat()


def _get_prices(
    symbol: str,
    ccy: str,
    start: str,
    end: str,
    store: DailyStore | None = None,
    today: str | None = None,
):
    """Indirection for the price-source router. Tests monkeypatch this
    so they don't hit yfinance."""
    from invest.prices.sources import get_prices

    return get_prices(symbol, ccy, start, end, store=store, today=today)


def _get_fx_rates(
    ccy: str,
    start: str,
    end: str,
    store: DailyStore | None = None,
    today: str | None = None,
):
    """Pass store + today so the set-minus path skips already-checked
    dates and successful fetches mark their range — keeps incremental
    runs from re-paying for cached FX days."""
    from invest.prices.sources import get_fx_rates

    return get_fx_rates(ccy, start, end, store=store, today=today)


def compute_increment_window(store: DailyStore) -> tuple[str, str] | None:
    """Return (start, end) for the incremental fetch, or None if the
    store is already at today.

    On a fresh DB with no ``last_known_date`` meta row yet (cold start
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


def _held_tw_symbols(portfolio: dict) -> list[str]:
    """Symbols still held at the latest PDF month-end. Snapshots only
    refresh prices for symbols that *currently matter* — historical-only
    codes don't need new bars."""
    months = portfolio.get("months", [])
    if not months:
        return []
    latest = months[-1]
    return [
        h["code"]
        for h in latest.get("tw", {}).get("holdings", [])
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


def _run_overlay_safe(
    store: DailyStore,
    portfolio: dict,
    today: str,
    sdk_data: tuple | None = None,
) -> dict[str, Any]:
    """Run the Phase 11 overlay with the standard try/except contract.

    Extracted so both the price-fetch path AND the already_current path
    can invoke it. The overlay's gap window (latest PDF month-end -> today)
    is independent of price freshness — ``meta.last_known_date == today``
    must NOT prevent the overlay from running, otherwise post-PDF broker
    trades stay invisible on a day with no new price data.

    When ``sdk_data`` is provided, merge() skips its internal SDK calls
    and projects the pre-pulled tuple. ``run()`` uses this to pull SDK
    sources once, fetch prices for newly-discovered symbols, then run
    the projection — single SDK roundtrip per refresh.
    """
    overlay_summary: dict[str, Any] = {
        "overlay_trades": 0,
        "dates_written": 0,
        "skipped_reason": "no_gap",
    }
    try:
        from invest.brokerage import trade_overlay
        from invest.brokerage.shioaji_client import ShioajiClient

        gap = trade_overlay.compute_gap_window(portfolio, today=today)
        if gap is not None:
            overlay_summary = trade_overlay.merge(
                store,
                portfolio,
                ShioajiClient(),
                gap[0],
                gap[1],
                sdk_data=sdk_data,
            )
    except Exception:  # noqa: BLE001 — overlay must never abort snapshot
        log.exception("trade_overlay.merge raised; continuing without overlay")
        overlay_summary = {
            "overlay_trades": 0,
            "dates_written": 0,
            "skipped_reason": "exception",
        }
    return overlay_summary


def _run_audit_safe(
    store: DailyStore, sdk_data: tuple | None,
) -> dict[str, int]:
    """Phase 14.5 — invoke shioaji_audit AFTER overlay write completes.

    Read-side companion to _run_overlay_safe. The audit hook used to
    live inline in trade_overlay.merge() (`_fire_audit_events`) and
    fired against the legacy reconcile_events table; it now runs
    against the SQLModel ``trades`` table populated by
    ``invest.jobs.trade_backfill``.

    Defensive contract: never aborts the snapshot, returns
    {pairs_examined, events_fired} for the summary envelope. When
    sdk_data is None (no overlay gap or SDK unconfigured) the audit
    is a no-op.
    """
    if sdk_data is None:
        return {"pairs_examined": 0, "events_fired": 0}
    _session, _lots, realized_pairs = sdk_data
    try:
        from invest.reconciliation import shioaji_audit

        result = shioaji_audit.run(realized_pairs, daily_store=store)
        if result.events_fired:
            log.info(
                "shioaji_audit: fired %d reconcile event(s) over %d pair(s)",
                result.events_fired, result.pairs_examined,
            )
        return {
            "pairs_examined": result.pairs_examined,
            "events_fired": result.events_fired,
        }
    except Exception:  # noqa: BLE001 — audit must never abort snapshot
        log.exception("shioaji_audit.run raised; continuing without audit")
        return {"pairs_examined": 0, "events_fired": 0}


def _fetch_overlay_symbol_prices(
    store: DailyStore,
    portfolio: dict,
    end: str,
    pdf_symbols: set[str],
) -> tuple[set[str], int, tuple | None]:
    """Discover the broker's symbol universe, fetch prices for any
    symbols not already in the PDF set, and return everything the
    caller needs to plumb into the merge step.

    Returns ``(new_dates, new_rows_added, sdk_data)``:
      - ``new_dates`` — date strings written to the prices table during
        the discovery fetch (folded into the caller's running set).
      - ``new_rows_added`` — count for the summary.
      - ``sdk_data`` — pre-pulled (session, lots, pairs) tuple to pass
        to ``_run_overlay_safe`` so the SDK isn't called twice. ``None``
        when there's no overlay gap (PDF still in-month) or creds are
        unset (the SDK pull returned the empty tuple).
    """
    from invest.brokerage import trade_overlay
    from invest.brokerage.shioaji_client import ShioajiClient

    gap = trade_overlay.compute_gap_window(portfolio, today=end)
    if gap is None:
        return set(), 0, None
    overlay_start, overlay_end = gap

    client = ShioajiClient()
    try:
        sdk_data = trade_overlay.pull_sdk_sources(
            client, store, overlay_start, overlay_end
        )
    except Exception:  # noqa: BLE001 — discovery must not abort snapshot
        log.exception("overlay symbol discovery raised; skipping pre-fetch")
        return set(), 0, None

    session, lots, pairs = sdk_data
    overlay_symbols: set[str] = set()
    for src in (session, lots, pairs):
        for r in src:
            code = r.get("code")
            if code:
                overlay_symbols.add(code)

    extra_symbols = overlay_symbols - pdf_symbols
    if not extra_symbols:
        return set(), 0, sdk_data

    log.info(
        "overlay symbol discovery: %d codes (PDF: %d, extra: %d) -> fetching "
        "prices for extras over [%s..%s]",
        len(overlay_symbols),
        len(pdf_symbols),
        len(extra_symbols),
        overlay_start,
        overlay_end,
    )

    new_dates: set[str] = set()
    rows_added = 0
    for code in sorted(extra_symbols):
        # Phase 14.3a: route through price_service.fetch_and_store_range.
        # The price_service writes rows internally; we re-query the
        # ``prices`` table for the new (date) tuples to fold into the
        # caller's running ``new_dates`` set.
        n = backfill_runner._fetch_range_via_price_service(
            store, code, "TWD", overlay_start, overlay_end,
        )
        if n <= 0:
            continue
        rows_added += n
        with store.connect_ro() as conn:
            for r in conn.execute(
                "SELECT date FROM prices WHERE symbol = ? "
                "AND date >= ? AND date <= ?",
                (code, overlay_start, overlay_end),
            ).fetchall():
                new_dates.add(r["date"])
    return new_dates, rows_added, sdk_data


def run(store: DailyStore, portfolio: dict) -> dict[str, Any]:
    """Run one incremental refresh against ``store``.

    Returns a summary dict the ``/api/admin/refresh`` endpoint surfaces
    back to the UI. Never raises — fetch failures land in failed_tasks
    via ``backfill_runner.fetch_with_dlq``, like the cold-start path.
    """
    window = compute_increment_window(store)
    if window is None:
        # Prices already current for today, but the overlay tracks a
        # different freshness clock (latest PDF month-end -> today). Run
        # discovery+price-fetch+merge in sequence so a refresh on a
        # fully-up-to-date day still folds in any new broker trades AND
        # any overlay-only symbols (6531/7769/etc.) get their prices
        # fetched before merge() projects them.
        today = _today_iso()
        pdf_tw_symbols = set(_held_tw_symbols(portfolio))
        overlay_dates, overlay_rows, sdk_data = _fetch_overlay_symbol_prices(
            store, portfolio, today, pdf_tw_symbols,
        )
        # Single-writer architecture (2026-05-01): merge first, then
        # derive. derive folds overlay rows into portfolio_daily so we
        # always re-run it on the already_current path — the user may
        # have just bought/sold something and only the merge step
        # produced new positions_daily rows.
        overlay_summary = _run_overlay_safe(
            store, portfolio, today, sdk_data=sdk_data
        )
        audit_summary = _run_audit_safe(store, sdk_data)
        backfill_runner._derive_positions_and_portfolio(store, portfolio)
        summary = {
            "new_dates": len(overlay_dates),
            "new_rows": overlay_rows,
            "overlay": overlay_summary,
            "audit": audit_summary,
            "skipped_reason": "already_current",
            "window": None,
        }
        log.info("snapshot summary: %s", summary)
        return summary

    start, end = window
    log.info("snapshot: incremental window [%s..%s]", start, end)

    new_rows = 0
    new_dates: set[str] = set()

    # 1. TW prices for currently-held codes
    # Phase 14.3a: routed through price_service.fetch_and_store_range.
    # Per-symbol DLQ writes happen inside price_service.
    for code in _held_tw_symbols(portfolio):
        n = backfill_runner._fetch_range_via_price_service(
            store, code, "TWD", start, end,
        )
        if n <= 0:
            continue
        new_rows += n
        with store.connect_ro() as conn:
            for r in conn.execute(
                "SELECT date FROM prices WHERE symbol = ? "
                "AND date >= ? AND date <= ?",
                (code, start, end),
            ).fetchall():
                new_dates.add(r["date"])

    # 2. Foreign prices
    for code, ccy in _held_foreign_symbols(portfolio):
        n = backfill_runner._fetch_range_via_price_service(
            store, code, ccy, start, end,
        )
        if n <= 0:
            continue
        new_rows += n
        with store.connect_ro() as conn:
            for r in conn.execute(
                "SELECT date FROM prices WHERE symbol = ? "
                "AND date >= ? AND date <= ?",
                (code, start, end),
            ).fetchall():
                new_dates.add(r["date"])

    # 3. FX (always at least USD, regardless of current foreign holdings)
    needed_ccys: set[str] = {"USD"}
    for _, ccy in _held_foreign_symbols(portfolio):
        if ccy and ccy != "TWD":
            needed_ccys.add(ccy)
    for ccy in sorted(needed_ccys):
        rows = backfill_runner.fetch_with_dlq(
            store,
            "fx_rates",
            ccy,
            lambda c=ccy, s=start, e=end, t=end: _get_fx_rates(
                c, s, e, store=store, today=t,
            ),
        )
        if rows is None:
            continue
        new_rows += _persist_fx(store, ccy, rows)

    # 4. Overlay symbol discovery + price pre-fetch (added 2026-05-01)
    # Two-pass orchestration: the price-fetcher above only knows about
    # PDF-held symbols, so codes the user trades AFTER the last PDF
    # statement (e.g. 6531 bought-and-sold in April when the latest PDF
    # is March) silently dropped at merge()'s `if close is None: continue`.
    # Discover the broker's universe, fetch missing prices, and reuse the
    # SDK pull as ``sdk_data`` so the overlay merge doesn't pay for a 2nd
    # SDK roundtrip.
    pdf_tw_symbols = set(_held_tw_symbols(portfolio))
    overlay_dates, overlay_rows, sdk_data = _fetch_overlay_symbol_prices(
        store, portfolio, end, pdf_tw_symbols,
    )
    new_dates.update(overlay_dates)
    new_rows += overlay_rows

    # 5. Phase 11 overlay — runs BEFORE derive under the single-writer
    # architecture (2026-05-01). merge() writes only positions_daily;
    # derive() then folds those rows into portfolio_daily.equity_twd via
    # a source='overlay' SUM. The reverse order would have derive miss
    # the day's overlay rows.
    overlay_summary = _run_overlay_safe(store, portfolio, end, sdk_data=sdk_data)

    # 5b. Phase 14.5 — broker-vs-PDF buy-leg audit. Read-only against
    # the SQLModel `trades` table; emits reconcile events on (date, qty)
    # mismatches. Defensive: skipped silently if `trades` is empty.
    audit_summary = _run_audit_safe(store, sdk_data)

    # 6. Derive positions_daily / portfolio_daily — authoritative writer
    # of portfolio_daily. Walks PDF holdings, then sums any overlay rows
    # the merge step just wrote.
    backfill_runner._derive_positions_and_portfolio(store, portfolio)

    store.set_meta("last_known_date", end)

    summary = {
        "new_dates": len(new_dates),
        "new_rows": new_rows,
        "overlay": overlay_summary,
        "audit": audit_summary,
        "skipped_reason": None,
        "window": [start, end],
    }
    log.info("snapshot summary: %s", summary)
    return summary
