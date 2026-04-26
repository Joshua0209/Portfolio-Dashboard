"""Phase 11 — post-PDF trade overlay.

Bridges the gap between the most recent PDF month-end and "now" using
trades pulled from Shioaji. The PDF parser remains canonical for any
date covered by a monthly statement; the overlay only writes rows for
dates strictly after the latest PDF month, marked `source='overlay'` so
they're trivially distinguishable from PDF-sourced positions.

When credentials are missing or no gap exists, this module is a clean
no-op — never crashes, never raises, never partial-writes.
"""
from __future__ import annotations

import calendar
import logging
from datetime import date
from typing import Any, Iterable

from .daily_store import DailyStore
from .shioaji_client import ShioajiClient

log = logging.getLogger(__name__)


# --- Gap window ---------------------------------------------------------


def _next_day(d: str) -> str:
    """ISO date 'YYYY-MM-DD' → next day's ISO date."""
    y, m, dd = (int(p) for p in d.split("-"))
    return date(y, m, dd).fromordinal(date(y, m, dd).toordinal() + 1).isoformat()


def _month_end_iso(yyyy_mm: str) -> str:
    y, m = (int(p) for p in yyyy_mm.split("-"))
    last = calendar.monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-{last:02d}"


def compute_gap_window(
    portfolio: dict, today: str
) -> tuple[str, str] | None:
    """Return (gap_start, gap_end) or None if there's no gap to fill.

    gap_start = day after the last PDF month-end (e.g. 2026-03 month →
    2026-04-01). gap_end = today. None when:
      - portfolio has no months at all, or
      - today falls inside or before the latest PDF month (no post-PDF
        time has elapsed yet — re-running the merge before fresh data
        lands is a no-op rather than an error).
    """
    months = [m.get("month") for m in portfolio.get("months", []) if m.get("month")]
    if not months:
        return None
    last_month = sorted(months)[-1]
    last_pdf_day = _month_end_iso(last_month)
    gap_start = _next_day(last_pdf_day)
    if gap_start > today:
        return None
    return (gap_start, today)


# --- Merge --------------------------------------------------------------


def _qty_history_from_portfolio(portfolio: dict) -> dict[str, list[tuple[str, float]]]:
    """Replicate backfill_runner._qty_history_for_symbol but for all TW
    codes at once. Returns {code: [(date, signed_qty_change)]} sorted."""
    out: dict[str, list[tuple[str, float]]] = {}
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "TW":
            continue
        code = t.get("code")
        if not code:
            continue
        d = t["date"].replace("/", "-") if "/" in t.get("date", "") else t.get("date", "")
        side = t.get("side", "")
        sign = 1 if "買" in side else -1
        qty = float(t.get("qty", 0) or 0)
        out.setdefault(code, []).append((d, sign * qty))
    for v in out.values():
        v.sort(key=lambda r: r[0])
    return out


def _opening_qty_at(qty_history: list[tuple[str, float]], on: str) -> float:
    """Cumulative qty at the start of `on` (i.e. through trades dated < on)."""
    return sum(q for d, q in qty_history if d < on)


def _priced_dates_in_range(
    store: DailyStore, start: str, end: str
) -> list[str]:
    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT DISTINCT date FROM prices WHERE date BETWEEN ? AND ? "
            "ORDER BY date",
            (start, end),
        ).fetchall()
    return [r[0] for r in rows]


def _close_for(store: DailyStore, code: str, d: str) -> float | None:
    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT close FROM prices WHERE date = ? AND symbol = ?", (d, code)
        ).fetchone()
    return row[0] if row else None


def _existing_pdf_rows(
    store: DailyStore, codes: Iterable[str], dates: Iterable[str]
) -> set[tuple[str, str]]:
    """Return {(date, symbol)} of positions_daily rows already sourced
    from PDF. The overlay must not overwrite these — PDF is canonical."""
    codes = list(codes)
    dates = list(dates)
    if not codes or not dates:
        return set()
    qmarks_codes = ",".join("?" * len(codes))
    qmarks_dates = ",".join("?" * len(dates))
    with store.connect_ro() as conn:
        rows = conn.execute(
            f"SELECT date, symbol FROM positions_daily "
            f"WHERE source='pdf' AND date IN ({qmarks_dates}) "
            f"AND symbol IN ({qmarks_codes})",
            (*dates, *codes),
        ).fetchall()
    return {(r[0], r[1]) for r in rows}


def merge(
    store: DailyStore,
    portfolio: dict,
    client: ShioajiClient,
    gap_start: str | None,
    gap_end: str | None,
) -> dict[str, Any]:
    """Pull overlay trades from `client`, project them onto positions_daily
    + portfolio_daily for the gap window, and return a summary dict.

    Skipped (no-op) reasons:
      - 'no_gap'                  — gap_start/gap_end not provided.
      - 'shioaji_unconfigured'    — creds missing or shioaji not installed.

    Successful runs return:
      {overlay_trades: N, dates_written: M, skipped_reason: None}
    """
    if not gap_start or not gap_end:
        return {"overlay_trades": 0, "dates_written": 0, "skipped_reason": "no_gap"}

    if not client.lazy_login():
        # Either creds missing or login outright failed; either way the
        # merge is a no-op for this run.
        return {
            "overlay_trades": 0,
            "dates_written": 0,
            "skipped_reason": "shioaji_unconfigured",
        }

    overlay_trades = client.list_trades(gap_start, gap_end)
    log.info(
        "trade_overlay: %d trades in [%s..%s]",
        len(overlay_trades), gap_start, gap_end,
    )

    # Build the qty timeline from PDF + overlay together. Overlay trades
    # are layered on top of PDF history so cumulative qty math stays
    # correct even when overlay trades reference codes the user already
    # held at the last month-end.
    qty_history = _qty_history_from_portfolio(portfolio)
    for t in overlay_trades:
        code = t["code"]
        sign = 1 if "買" in (t.get("side") or "") else -1
        qty_history.setdefault(code, []).append((t["date"], sign * float(t["qty"])))
    for v in qty_history.values():
        v.sort(key=lambda r: r[0])

    cost_at: dict[str, float] = {}
    for m in portfolio.get("months", []):
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if code:
                cost_at[code] = float(h.get("avg_cost", 0) or 0)

    # We persist overlay rows for every priced day in the gap, for every
    # code with a non-zero opening (or post-overlay-trade) qty. This
    # mirrors backfill_runner's mv-snapshot loop but scoped to the gap.
    priced_dates = _priced_dates_in_range(store, gap_start, gap_end)
    if not priced_dates:
        return {"overlay_trades": len(overlay_trades), "dates_written": 0,
                "skipped_reason": None}

    affected_codes = set(qty_history.keys())
    pdf_locked = _existing_pdf_rows(store, affected_codes, priced_dates)

    rows_written = 0
    portfolio_rows: dict[str, float] = {}  # date → equity_twd
    portfolio_n: dict[str, int] = {}        # date → n_positions

    with store.connect_rw() as conn:
        for d in priced_dates:
            for code, changes in qty_history.items():
                qty = sum(q for date_, q in changes if date_ <= d)
                if qty <= 0:
                    continue
                if (d, code) in pdf_locked:
                    # PDF is canonical; do not overwrite. Still count
                    # toward portfolio_daily so the day's equity reflects
                    # the position.
                    close = _close_for(store, code, d)
                    if close is None:
                        continue
                    mv = qty * close
                    portfolio_rows[d] = portfolio_rows.get(d, 0.0) + mv
                    portfolio_n[d] = portfolio_n.get(d, 0) + 1
                    continue
                close = _close_for(store, code, d)
                if close is None:
                    continue
                mv_local = qty * close
                cost = cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'overlay')
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    WHERE positions_daily.source = 'overlay'
                    """,
                    (d, code, qty, cost, mv_local, mv_local, "現股"),
                )
                rows_written += 1
                portfolio_rows[d] = portfolio_rows.get(d, 0.0) + mv_local
                portfolio_n[d] = portfolio_n.get(d, 0) + 1

        for d, equity in portfolio_rows.items():
            n = portfolio_n.get(d, 0)
            conn.execute(
                """
                INSERT INTO portfolio_daily(
                    date, equity_twd, cash_twd, fx_usd_twd, n_positions, has_overlay
                ) VALUES (?, ?, NULL, 0.0, ?, 1)
                ON CONFLICT(date) DO UPDATE SET
                    equity_twd = excluded.equity_twd,
                    n_positions = excluded.n_positions,
                    has_overlay = 1
                """,
                (d, equity, n),
            )

    return {
        "overlay_trades": len(overlay_trades),
        "dates_written": rows_written,
        "skipped_reason": None,
    }
