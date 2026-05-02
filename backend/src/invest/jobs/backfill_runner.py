"""Cold-start daily backfill: portfolio.json + yfinance → SQLite cache.

Walks the trade ledger and holdings tables in data/portfolio.json,
computes a per-symbol fetch window per spec §6.1
(BACKFILL_FLOOR=2025-08-01, [max(first_trade, FLOOR), max(last_trade,
last_held_date)]), and pulls TW + foreign prices via
app.price_sources.get_prices, FX rates via get_fx_rates, and benchmark
prices via get_yfinance_prices. UPSERTs into the prices / fx_daily /
positions_daily / portfolio_daily tables.

Entry points:
  run_full_backfill() — orchestrates all four upstreams (recommended)
  run_tw_backfill()   — TW-only subset (kept for targeted smoke tests)
  start()             — daemon-thread wrapper with the
                        INITIALIZING / READY / FAILED state machine;
                        called from create_app() when
                        BACKFILL_ON_STARTUP=true.
"""
from __future__ import annotations

import calendar
import json
import logging
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from invest.core import state as backfill_state
from invest.persistence.daily_store import BACKFILL_FLOOR_DEFAULT, DailyStore
from invest.prices.sources import get_fx_rates, get_prices, get_yfinance_prices

log = logging.getLogger(__name__)

# File lives at <root>/backend/src/invest/jobs/backfill_runner.py.
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
_thread_lock = threading.Lock()
_active_thread: threading.Thread | None = None


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


# --- Position derivation --------------------------------------------------


def _qty_history_for_symbol(
    portfolio: dict, code: str, venue: str = "TW"
) -> list[tuple[str, float]]:
    """Return [(date, signed_qty_change), ...] for one symbol on `venue`,
    sorted by date.

    Buys add positive qty; sells subtract. Foreign trades use 買進/賣出
    while TW trades use 普買/普賣 — both contain "買"/"賣" so the same
    sign rule works.
    """
    out: list[tuple[str, float]] = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != venue or t.get("code") != code:
            continue
        d = _normalize_trade_date(t["date"])
        side = t.get("side", "")
        qty = float(t.get("qty", 0) or 0)
        sign = 1 if "買" in side else -1
        out.append((d, sign * qty))
    out.sort(key=lambda r: r[0])
    return out


def _qty_per_priced_date_for_symbol(
    portfolio: dict,
    code: str,
    venue: str,
    priced_dates: list[str],
    overlay_deltas: list[tuple[str, float]] | None = None,
) -> dict[str, float]:
    """Map each priced date → qty held that day, anchored to PDF holdings.

    Pure trade summation breaks under stock splits — e.g. 00631L's pre-split
    trade ledger sums to 210 shares while the post-split March holding row
    is 4620 (a ~1:22 split). The PDF holdings table reflects post-split qty,
    so we use it as the anchor in split months and fall back to prior-anchor +
    intra-month trade deltas elsewhere.

    Algorithm per priced date d:
      base_qty = qty_at_(prior_PDF_month_end) + Σ trades in (prior_me, d]
      If d's month has its own PDF anchor:
          expected = qty_at_(prior_PDF_month_end) + Σ all_trades_in_month
          If anchor >> expected (≥1.5×): split was implied; use anchor for
              every day in the month (the price-side handles pre-split
              scaling separately).
          Otherwise: stick with base_qty so mid-month buys don't appear
              prematurely (e.g. 00991A's Feb 5 buy doesn't inflate Feb 2's V).
      No PDF month for d: fall back to base_qty; for dates that precede
      every snapshot (intra-month round-trips that never closed at a
      month-end), sum trades from the start.
    """
    venue_key = "tw" if venue == "TW" else "foreign"
    anchors_by_month: dict[str, float] = {}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        if not ym:
            continue
        for h in m.get(venue_key, {}).get("holdings", []):
            if h.get("code") == code:
                qty = float(h.get("qty", 0) or 0)
                if qty > 0:
                    anchors_by_month[ym] = qty
                break

    deltas = _qty_history_for_symbol(portfolio, code, venue)
    if overlay_deltas:
        # Bug 2 follow-up (2026-05-01): fold trades_overlay deltas in so a
        # code held in PDF (e.g. 00981A) reflects post-PDF buys/sells in
        # its daily qty. Without this, mv stays at the PDF anchor while
        # cash correctly tracks the overlay activity, double-counting
        # equity by the inflated mv.
        deltas = sorted(deltas + list(overlay_deltas), key=lambda r: r[0])
    sorted_yms = sorted(anchors_by_month)

    def _prior_ym_for(target_ym: str) -> str | None:
        prior: str | None = None
        for ym in sorted_yms:
            if ym < target_ym:
                prior = ym
            else:
                break
        return prior

    out: dict[str, float] = {}
    for d in priced_dates:
        d_ym = d[:7]
        prior_ym = _prior_ym_for(d_ym)
        if prior_ym is None:
            base_qty = sum(q for td, q in deltas if td <= d)
        else:
            prior_me = month_end_iso(prior_ym)
            base_qty = anchors_by_month[prior_ym] + sum(
                q for td, q in deltas if prior_me < td <= d
            )
        base_qty = max(0.0, base_qty)

        if d_ym in anchors_by_month:
            anchor_qty = anchors_by_month[d_ym]
            month_me = month_end_iso(d_ym)
            prior_me_or_zero = (
                month_end_iso(prior_ym) if prior_ym else "0000-00-00"
            )
            intra_total = sum(
                q for td, q in deltas if prior_me_or_zero < td <= month_me
            )
            expected = (anchors_by_month[prior_ym] if prior_ym else 0.0) + intra_total
            if expected > 1 and anchor_qty > expected * 1.5:
                # Split detected — month-end anchor wins; price-side scales
                # pre-split-day closes elsewhere in the pipeline.
                out[d] = anchor_qty
                continue
        out[d] = base_qty
    return out


def _forward_fill_fx(
    fx_rows: list[tuple[str, float]], dates: Iterable[str]
) -> dict[str, float]:
    """Build a {date → rate} map across `dates` by carrying the most-recent
    rate forward (yfinance returns stale `TWD=X` rows on Asia weekends, so
    a price-day can land on a no-FX date).

    `fx_rows` must be sorted by date asc.
    """
    out: dict[str, float] = {}
    if not fx_rows:
        return out
    sorted_dates = sorted(set(dates))
    fx_idx = 0
    last_rate: float | None = None
    for d in sorted_dates:
        while fx_idx < len(fx_rows) and fx_rows[fx_idx][0] <= d:
            last_rate = fx_rows[fx_idx][1]
            fx_idx += 1
        if last_rate is None:
            # Fall back: scan ahead for the earliest rate (handles dates
            # before the first fx row, e.g. the very start of the curve).
            for fd, fr in fx_rows:
                if fd >= d:
                    last_rate = fr
                    break
        if last_rate is not None:
            out[d] = last_rate
    return out


def _derive_positions_and_portfolio(
    store: DailyStore, portfolio: dict
) -> dict[str, int]:
    """Walk every priced trading day, compute end-of-day qty per symbol
    from the trade ledger, multiply by close → mv_local, convert foreign
    via fx_daily (forward-fill on weekend gaps), and aggregate to
    portfolio_daily.equity_twd.
    """
    tw_codes = [e["code"] for e in iter_tw_symbols_with_metadata(portfolio)]
    foreign_codes: list[str] = []
    foreign_currency: dict[str, str] = {}
    # Per-share avg cost from latest PDF month that held the code. Used at
    # write time as `cost_local = qty × avg_cost_at[code]` so the stored
    # cost_local matches the schema convention (total cost in local ccy)
    # that every reader (analytics.py, holdings.py, tickers.py) assumes.
    avg_cost_at: dict[str, float] = {}

    for entry in iter_foreign_symbols_with_metadata(portfolio):
        foreign_codes.append(entry["code"])
        foreign_currency[entry["code"]] = entry["currency"]

    # ref_price_by_month_code = month-end MV-per-share from holdings, used
    # as a last-resort price when neither yfinance nor forward-fill produce
    # a close. Without this, symbols whose daily fetch fails (DLQ candidates)
    # silently drop out of daily V — the daily equity curve under-tracks
    # the true portfolio for those holdings.
    ref_price_by_month_code: dict[str, dict[str, float]] = {"tw": {}, "foreign": {}}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = float(h.get("qty", 0) or 0)
            if qty > 0:
                avg = h.get("avg_cost")
                if avg is None:
                    avg = float(h.get("cost", 0) or 0) / qty
                avg_cost_at[code] = float(avg or 0)
            ref = h.get("ref_price")
            if ref and ym:
                ref_price_by_month_code["tw"][f"{code}|{ym}"] = float(ref)
        for h in m.get("foreign", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = float(h.get("qty", 0) or 0)
            if qty > 0:
                # Foreign rows often have avg_cost_local=None but carry a
                # canonical total `cost` field — derive avg from cost/qty
                # to avoid storing 0 (which would render as 100% unrealized).
                avg = h.get("avg_cost_local")
                if avg is None:
                    avg = float(h.get("cost", 0) or 0) / qty
                avg_cost_at[code] = float(avg or 0)
            close_l = h.get("close")
            if close_l and ym:
                ref_price_by_month_code["foreign"][f"{code}|{ym}"] = float(close_l)

    # Stale-overlay cleanup (single-writer architecture, 2026-05-01).
    # The per-day UPSERT below uses WHERE source='pdf' so PDF writes
    # don't clobber overlay rows during the gap window. The downside:
    # once a new PDF month lands and the gap shrinks, overlay rows for
    # now-covered dates would persist forever. Clear them up-front so
    # PDFs become canonical for any date the latest PDF month-end now
    # reaches.
    months = sorted(
        (m.get("month") for m in portfolio.get("months", []) if m.get("month")),
    )
    if months:
        latest_pdf_month_end = month_end_iso(months[-1])
        with store.connect_rw() as conn:
            conn.execute(
                "DELETE FROM positions_daily "
                "WHERE source = 'overlay' AND date <= ?",
                (latest_pdf_month_end,),
            )

    with store.connect_ro() as conn:
        priced_dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT date FROM prices ORDER BY date"
            ).fetchall()
        ]
        all_prices = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT date, symbol, close FROM prices"
            ).fetchall()
        }
        fx_by_ccy: dict[str, list[tuple[str, float]]] = {}
        for r in conn.execute(
            "SELECT ccy, date, rate_to_twd FROM fx_daily ORDER BY ccy, date"
        ).fetchall():
            fx_by_ccy.setdefault(r[0], []).append((r[1], r[2]))

    if not priced_dates:
        return {"positions_rows": 0, "portfolio_rows": 0}

    fx_filled: dict[str, dict[str, float]] = {
        ccy: _forward_fill_fx(rows, priced_dates) for ccy, rows in fx_by_ccy.items()
    }

    # Forward-fill closes across priced_dates. Two distinct gap sources:
    #   • foreign symbols: yfinance is silent on US holidays/weekends but
    #     those can still be TW trading days (and vice versa).
    #   • TW symbols: yfinance occasionally returns no rows for a date
    #     (thin-volume NaN, network blip), leaving holes in the window.
    # Without forward-fill, holdings *vanish from V* on every gap day and
    # the daily equity curve gyrates wildly (e.g. r_d = +1274% when 16
    # symbols re-appear on the next priced day). Carrying the most-recent
    # close forward keeps V continuous.
    def _build_filled(codes: list[str]) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for code in codes:
            rows_for_code = sorted(
                (d, c) for (d, sym), c in all_prices.items() if sym == code
            )
            out[code] = _forward_fill_fx(rows_for_code, priced_dates)
        return out

    tw_close_filled = _build_filled(tw_codes)
    foreign_close_filled = _build_filled(foreign_codes)

    # Stock splits cause a sudden mismatch between PDF holdings qty
    # (post-split) and yfinance close (pre-split for days before the
    # split date). e.g. 00631L Feb→Mar 2026 went 220→4620 shares while
    # the underlying price dropped ~22×. Without adjustment, daily MV
    # spikes to (4620 × pre-split 530) ≈ 2.45M, then crashes back at
    # month-end. Detect the split factor from holdings, find the split
    # day from prices, and rescale pre-split closes to the post-split scale.
    def _split_adjusted(
        venue: str, code: str, filled: dict[str, float]
    ) -> dict[str, float]:
        venue_key = "tw" if venue == "TW" else "foreign"
        anchors: list[tuple[str, float]] = []
        for m in portfolio.get("months", []):
            ym = m.get("month")
            for h in m.get(venue_key, {}).get("holdings", []):
                if h.get("code") == code:
                    qty = float(h.get("qty", 0) or 0)
                    if qty > 0 and ym:
                        anchors.append((ym, qty))
                    break
        anchors.sort()
        deltas = _qty_history_for_symbol(portfolio, code, venue)
        adjusted = dict(filled)
        for i in range(1, len(anchors)):
            prev_ym, prev_qty = anchors[i - 1]
            curr_ym, curr_qty = anchors[i]
            prev_me = month_end_iso(prev_ym)
            curr_me = month_end_iso(curr_ym)
            intra = sum(q for td, q in deltas if prev_me < td <= curr_me)
            expected = prev_qty + intra
            if expected <= 1 or curr_qty <= expected * 1.5:
                continue
            split_factor = curr_qty / expected
            month_prices = sorted(
                (d, adjusted[d]) for d in adjusted
                if d[:7] == curr_ym and adjusted[d] > 0
            )
            split_day = None
            target = 1.0 / split_factor
            for j in range(1, len(month_prices)):
                d_prev, c_prev = month_prices[j - 1]
                d_curr, c_curr = month_prices[j]
                if c_prev <= 0:
                    continue
                ratio = c_curr / c_prev
                if abs(ratio - target) / target < 0.15:
                    split_day = d_curr
                    break
            if split_day is None:
                # No price-drop signal — assume the split landed on the
                # month-end (worst case scales the whole month, which still
                # beats letting MV spike 22×).
                split_day = curr_me
            # Only days WITHIN the split month and BEFORE the split day
            # need price-scaling. PDF holdings for prior months carry
            # pre-split qty already, so their MV (pre-split qty × pre-split
            # close) is correct as-is — no scaling needed there.
            for d in list(adjusted.keys()):
                if d[:7] == curr_ym and d < split_day:
                    adjusted[d] = adjusted[d] / split_factor
        return adjusted

    tw_close_filled = {
        code: _split_adjusted("TW", code, filled)
        for code, filled in tw_close_filled.items()
    }
    foreign_close_filled = {
        code: _split_adjusted("Foreign", code, filled)
        for code, filled in foreign_close_filled.items()
    }

    # Pull overlay trade deltas once and group by code so the per-symbol
    # qty walk picks up post-PDF activity for codes that ALSO appear in
    # PDF holdings (e.g., 00981A bought in March, traded heavily in April).
    # Without this, the PDF qty stays as the anchor for every gap date
    # and mv ignores intra-gap rotations.
    overlay_deltas_by_code: dict[str, list[tuple[str, float]]] = {}
    with store.connect_ro() as conn:
        for r in conn.execute(
            "SELECT date, code, side, qty FROM trades_overlay"
        ).fetchall():
            sign = 1 if "買" in (r["side"] or "") else -1
            overlay_deltas_by_code.setdefault(r["code"], []).append(
                (r["date"], sign * float(r["qty"]))
            )

    # Pre-compute per-day qty per symbol once — anchored to PDF holdings so
    # stock splits don't drift the daily share count.
    tw_qty_by_date = {
        code: _qty_per_priced_date_for_symbol(
            portfolio, code, "TW", priced_dates,
            overlay_deltas=overlay_deltas_by_code.get(code),
        )
        for code in tw_codes
    }
    foreign_qty_by_date = {
        code: _qty_per_priced_date_for_symbol(portfolio, code, "Foreign", priced_dates)
        for code in foreign_codes
    }

    # Synthesized broker-cash schedule. The daily layer has no source for
    # broker cash balances, so we approximate: anchor at 0 on the first
    # priced day and accumulate trade.net_twd as we walk forward. Buys make
    # net_twd negative (cash leaves broker → buys position); sells make it
    # positive (cash credited). Without this offset, equity_twd plunges on
    # rotation days because the MV change from a sale isn't matched by the
    # cash credit. With it, mv − net_twd stays conserved across a buy/sell
    # pair (modulo fees, which are real costs).
    #
    # Caveats: external bank↔broker transfers aren't dated daily, so deposit
    # days appear flat instead of jumping. Dividend credits and broker-side
    # fees outside trades are also missing. Within a month with no external
    # flows this is exact for rotations.
    #
    # Bug 2 fix (2026-05-01): post-PDF overlay trades now contribute to
    # running_cash_twd via the trades_overlay table. Without this, an
    # overlay sell debited mv but never credited cash, so the equity curve
    # dropped artificially on rotation days the user sold via broker.
    pdf_trades = [
        (_normalize_trade_date(t["date"]), float(t.get("net_twd") or 0))
        for t in portfolio.get("summary", {}).get("all_trades", [])
    ]
    with store.connect_ro() as conn:
        overlay_trades = [
            (r["date"], float(r["net_twd"] or 0))
            for r in conn.execute(
                "SELECT date, net_twd FROM trades_overlay"
            ).fetchall()
        ]
    trades_chrono: list[tuple[str, float]] = sorted(
        pdf_trades + overlay_trades,
        key=lambda r: r[0],
    )

    n_positions = 0
    n_portfolio = 0
    with store.connect_rw() as conn:
        trade_idx = 0
        running_cash_twd = 0.0
        for d in priced_dates:
            while (
                trade_idx < len(trades_chrono)
                and trades_chrono[trade_idx][0] <= d
            ):
                running_cash_twd += trades_chrono[trade_idx][1]
                trade_idx += 1

            day_fx_usd = fx_filled.get("USD", {}).get(d, 0.0)

            # Single-writer architecture (2026-05-01):
            #   1. Write PDF rows but DO NOT overwrite existing overlay
            #      rows (the WHERE source='pdf' guard on UPDATE). During
            #      the gap window, overlay carries the augmented qty
            #      (e.g. user bought MORE of an existing PDF holding) —
            #      PDF's roll-forward qty is stale by definition there.
            #   2. After all PDF writes, SUM positions_daily for the
            #      day. This sees both PDF rows we just wrote AND any
            #      overlay rows merge persisted earlier in this run.
            #      Single source of truth for portfolio_daily.equity_twd.
            #
            # Stale-overlay cleanup: when a new PDF month lands and the
            # gap shrinks, overlay rows for now-covered dates would
            # persist forever under this guard. Cleared up-front via
            # _clear_stale_overlay_rows() below (called once per derive
            # run, before this loop).

            # TW positions — local == TWD, mv_twd == mv_local
            for code, qty_by_date in tw_qty_by_date.items():
                qty = qty_by_date.get(d, 0.0)
                if qty <= 0:
                    # Position fully exited (e.g., overlay sells exceeded
                    # PDF qty for 042900 — broker sold odd-lot remainder
                    # PDF parser missed). Delete any stale PDF row so the
                    # holdings reader doesn't surface a phantom position.
                    conn.execute(
                        "DELETE FROM positions_daily "
                        "WHERE date = ? AND symbol = ? AND source = 'pdf'",
                        (d, code),
                    )
                    continue
                close = tw_close_filled.get(code, {}).get(d)
                if close is None:
                    # Last resort: PDF-month ref_price for the month of d.
                    # NOTE — same-month-only fallback. If a symbol is held
                    # mid-month but exits before the next month-end, it is
                    # absent from that month's holdings table, so this lookup
                    # returns None and the row is skipped below — silently
                    # under-counting equity_twd for the holding window.
                    # Rare in practice now that TW prices route through
                    # yfinance, but if a regression brings the fallback
                    # back into play, walk back through prior months'
                    # anchors instead of giving up at d[:7].
                    close = ref_price_by_month_code["tw"].get(f"{code}|{d[:7]}")
                if close is None:
                    continue
                mv_local = qty * close
                # Total cost in local ccy (TWD for TW). Convention: every
                # reader treats positions_daily.cost_local as total, not
                # per-share — matching analytics.py:790 / holdings.py:140.
                cost_local = qty * avg_cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    WHERE positions_daily.source = 'pdf'
                    """,
                    (d, code, qty, cost_local, mv_local, mv_local, "現股", "pdf"),
                )
                n_positions += 1

            # Foreign positions — convert mv_local via fx_filled
            for code, qty_by_date in foreign_qty_by_date.items():
                qty = qty_by_date.get(d, 0.0)
                if qty <= 0:
                    conn.execute(
                        "DELETE FROM positions_daily "
                        "WHERE date = ? AND symbol = ? AND source = 'pdf'",
                        (d, code),
                    )
                    continue
                close = foreign_close_filled.get(code, {}).get(d)
                if close is None:
                    close = ref_price_by_month_code["foreign"].get(f"{code}|{d[:7]}")
                if close is None:
                    continue
                ccy = foreign_currency.get(code, "USD")
                fx = fx_filled.get(ccy, {}).get(d)
                if fx is None or fx == 0:
                    # No FX for this day — skip rather than write a wrong
                    # mv_twd. portfolio_daily for this date may end up with
                    # only TW positions, which is the correct degraded state.
                    continue
                mv_local = qty * close
                mv_twd = mv_local * fx
                cost_local = qty * avg_cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    WHERE positions_daily.source = 'pdf'
                    """,
                    (d, code, qty, cost_local, mv_local, mv_twd, "foreign", "pdf"),
                )
                n_positions += 1

            # Authoritative day-aggregate: SUM positions_daily.mv_twd
            # over BOTH sources. PRIMARY KEY (date, symbol) ensures we
            # never double-count; the WHERE source='pdf' guard above
            # ensures overlay rows survive intact during the gap window.
            agg = conn.execute(
                "SELECT COALESCE(SUM(mv_twd), 0), COUNT(*), "
                "MAX(CASE WHEN source = 'overlay' THEN 1 ELSE 0 END) "
                "FROM positions_daily WHERE date = ?",
                (d,),
            ).fetchone()
            day_mv_twd = float(agg[0] or 0.0)
            day_n = int(agg[1] or 0)
            day_has_overlay = int(agg[2] or 0)

            if day_n == 0 and running_cash_twd == 0.0:
                continue
            day_equity_twd = day_mv_twd + running_cash_twd
            conn.execute(
                """
                INSERT INTO portfolio_daily(
                    date, equity_twd, cash_twd, fx_usd_twd, n_positions, has_overlay
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    equity_twd = excluded.equity_twd,
                    cash_twd = excluded.cash_twd,
                    fx_usd_twd = excluded.fx_usd_twd,
                    n_positions = excluded.n_positions,
                    has_overlay = excluded.has_overlay
                """,
                (d, day_equity_twd, running_cash_twd, day_fx_usd, day_n,
                 day_has_overlay),
            )
            n_portfolio += 1

    return {"positions_rows": n_positions, "portfolio_rows": n_portfolio}


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
    """
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


def _persist_fx_rows(store: DailyStore, ccy: str, rows: list[dict]) -> int:
    """UPSERT fx_daily rows for one currency. Idempotent on (date, ccy) PK."""
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
                (r["date"], r["ccy"], r["rate"], r["source"], now),
            )
    return len(rows)


def run_fx_backfill(
    store: DailyStore,
    portfolio_path: Path | str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
    today: str | None = None,
) -> dict[str, Any]:
    """Populate fx_daily for every foreign currency in scope across
    [floor, today]. Per spec §6.1, FX is dense across the whole equity-curve
    window, not per-symbol — the curve always needs a TWD reference.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()

    rows_written = 0
    by_ccy: dict[str, int] = {}
    for ccy in sorted(_foreign_currencies_in_scope(portfolio)):
        log.info("fx backfill: %s [%s..%s]", ccy, floor, today)
        rows = fetch_with_dlq(
            store, "fx_rates", ccy,
            lambda c=ccy, s=floor, e=today, t=today: get_fx_rates(
                c, s, e, store=store, today=t,
            ),
        )
        if rows is None:
            continue
        n = _persist_fx_rows(store, ccy, rows)
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
    using the same per-symbol fetch-window logic as the TW backfill."""
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
        rows = fetch_with_dlq(
            store, "foreign_prices", code,
            lambda c=code, ccy=currency, s=start, e=end, t=today: get_prices(
                c, ccy, s, e, store=store, today=t,
            ),
        )
        if rows is None:
            continue
        rows_written += _persist_symbol_prices(store, code, rows)
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
    tw → fx → foreign → benchmark → tw → … This spreads consecutive
    upstream calls across different hosts so no single one sees
    back-to-back hits (yfinance throttles aggressively on bursts)."""
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
    second time."""
    message = _sanitize_error_message(f"{type(exc).__name__}: {exc}")
    now = _now_utc_iso()
    log.warning("retry pass: %s/%s failed again: %s", task_type, target, message)
    with store.connect_rw() as conn:
        existing = conn.execute(
            """
            SELECT id, attempts FROM failed_tasks
            WHERE task_type = ? AND target = ? AND resolved_at IS NULL
            """,
            (task_type, target),
        ).fetchone()
        if existing is not None:
            conn.execute(
                """
                UPDATE failed_tasks
                SET attempts = ?, last_attempt_at = ?, error_message = ?
                WHERE id = ?
                """,
                (existing["attempts"] + 1, now, message, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO failed_tasks(
                    task_type, target, error_message,
                    attempts, first_seen_at, last_attempt_at
                ) VALUES (?, ?, ?, 1, ?, ?)
                """,
                (task_type, target, message, now, now),
            )


def _build_tw_tasks(
    store: DailyStore,
    portfolio: dict,
    floor: str,
    today: str,
    latest_month: str,
) -> tuple[list[FetchTask], list[str], dict[str, str]]:
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
        tasks.append(FetchTask(
            upstream="tw",
            target=code,
            descriptor=f"{code} [{start}..{end}]",
            dlq_task_type="tw_prices",
            fetch_fn=(lambda c=code, s=start, e=end:
                      get_prices(c, "TWD", s, e, store=store, today=today)),
            persist_fn=(lambda rows, c=code: _persist_symbol_prices(store, c, rows)),
        ))
    return tasks, skipped, skip_reasons


def _build_fx_tasks(
    store: DailyStore, portfolio: dict, floor: str, today: str,
) -> list[FetchTask]:
    tasks: list[FetchTask] = []
    for ccy in sorted(_foreign_currencies_in_scope(portfolio)):
        tasks.append(FetchTask(
            upstream="fx",
            target=ccy,
            descriptor=f"{ccy} [{floor}..{today}]",
            dlq_task_type="fx_rates",
            fetch_fn=(lambda c=ccy, s=floor, e=today:
                      get_fx_rates(c, s, e, store=store, today=today)),
            persist_fn=(lambda rows, c=ccy: _persist_fx_rows(store, c, rows)),
        ))
    return tasks


def _build_foreign_tasks(
    store: DailyStore,
    portfolio: dict,
    floor: str,
    today: str,
    latest_month: str,
) -> tuple[list[FetchTask], list[str], dict[str, str]]:
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
        tasks.append(FetchTask(
            upstream="foreign",
            target=code,
            descriptor=f"{code} ({currency}) [{start}..{end}]",
            dlq_task_type="foreign_prices",
            fetch_fn=(lambda c=code, ccy=currency, s=start, e=end:
                      get_prices(c, ccy, s, e, store=store, today=today)),
            persist_fn=(lambda rows, c=code: _persist_symbol_prices(store, c, rows)),
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

    Round-robin scheduling across upstreams (tw → fx → foreign → benchmark)
    spreads consecutive calls across different APIs so no single upstream
    sees back-to-back hits. On per-task failure, the task is deferred to
    a single retry pass; second-pass failures land in `failed_tasks`.

    Circuit breaker: when an upstream accumulates `max_failures_per_market`
    fetch failures (across both passes), every remaining task in that
    upstream is short-circuited — both not-yet-attempted first-pass tasks
    and already-deferred tasks. A circuit-broken task still gets a DLQ row
    so the operator can see what was abandoned.
    """
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    today = today or _today_iso()
    latest_month = _latest_data_month(portfolio)

    # Build queues — task descriptors capture closures over (start, end, today).
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
                "%s: circuit breaker tripped after %d failures — "
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
            # silently lost — see KNOWN HAZARD comment on run_tw_backfill.
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
                "%s: %s failed (%s) — deferring", task.upstream, task.target, exc,
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
        log.info("=== Phase 2/3: Retry pass — skipped (no deferrals) ===")

    log.info("=== Phase 3/3: Overlay + derive positions + portfolio ===")

    # Single-writer architecture (mirrors snapshot_daily.run, 2026-05-01):
    # merge() runs FIRST so trades_overlay is populated before derive()'s
    # cash walk reads it. Bug 2 fix: previously the order was derive →
    # overlay, so post-PDF broker sells debited mv via overlay's
    # positions_daily writes but never credited cash via derive's trades
    # walk — equity_twd fake-dropped on every overlay rotation day.
    from invest.brokerage import trade_overlay
    from invest.brokerage.shioaji_client import ShioajiClient
    overlay_summary = {"overlay_trades": 0, "skipped_reason": "no_gap"}
    try:
        gap = trade_overlay.compute_gap_window(portfolio, today=today)
        if gap is not None:
            overlay_summary = trade_overlay.merge(
                store, portfolio, ShioajiClient(), gap[0], gap[1]
            )
    except Exception:  # noqa: BLE001 — overlay must never abort the backfill
        log.exception("trade_overlay.merge raised; continuing without overlay")

    derived = _derive_positions_and_portfolio(store, portfolio)

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
    full ~5–8 minutes that 30+ TW codes × 8 months × ~1.2s/fetch implies
    (the plan's "≤90s" target assumes a smaller portfolio).

    `only_codes`: if set, only fetch these symbols. Skip-tracking still
    runs for the rest.

    `max_failures_per_market`: trip the TW circuit breaker after this many
    fetch failures and skip every remaining symbol. Aligned with the
    multi-market breaker in run_full_backfill.

    KNOWN HAZARD — circuit-breaker silent loss (legacy from TWSE-direct era):
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
        rows = fetch_with_dlq(
            store, "tw_prices", code,
            lambda c=code, s=start, e=end, t=today: get_prices(
                c, "TWD", s, e, store=store, today=t,
            ),
        )
        if rows is None:
            failures += 1
            if failures >= max_failures_per_market:
                tripped = True
                log.warning(
                    "tw: circuit breaker tripped after %d failures — "
                    "skipping remaining symbols",
                    failures,
                )
            continue
        rows_written += _persist_symbol_prices(store, code, rows)
        fetched.append(code)
        processed += 1

    derived = _derive_positions_and_portfolio(store, portfolio)
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

    De-duping rule (per spec §10): an "open" row exists per
    (task_type, target) where resolved_at IS NULL. A second failure for
    the same target bumps `attempts` and updates `last_attempt_at`
    instead of inserting a duplicate. Once a row is resolved, a fresh
    failure creates a new open row.
    """
    try:
        return fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — boundary by design
        message = _sanitize_error_message(f"{type(exc).__name__}: {exc}")
        now = _now_utc_iso()
        log.warning(
            "fetch_with_dlq: %s/%s failed: %s", task_type, target, message
        )
        with store.connect_rw() as conn:
            existing = conn.execute(
                """
                SELECT id, attempts FROM failed_tasks
                WHERE task_type = ? AND target = ? AND resolved_at IS NULL
                """,
                (task_type, target),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    """
                    UPDATE failed_tasks
                    SET attempts = ?, last_attempt_at = ?, error_message = ?
                    WHERE id = ?
                    """,
                    (existing["attempts"] + 1, now, message, existing["id"]),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO failed_tasks(
                        task_type, target, error_message,
                        attempts, first_seen_at, last_attempt_at
                    ) VALUES (?, ?, ?, 1, ?, ?)
                    """,
                    (task_type, target, message, now, now),
                )
        return None


Resolver = Callable[[dict[str, Any]], Callable[[], Any]]


def retry_open_tasks(store: DailyStore, resolver: Resolver) -> dict[str, int]:
    """Walk every open failed_tasks row and retry it.

    `resolver(row) -> callable`: caller-supplied factory that returns a
    no-arg callable that fetches AND persists the rows for the given DLQ
    entry. On success (no exception), sets resolved_at on the row. On
    failure, bumps attempts.

    The callable MUST persist on its own — `retry_open_tasks` discards
    the return value. This mirrors the FetchTask round-robin path which
    pairs fetch_fn with persist_fn. A resolver that only fetches will
    silently mark dates_checked but lose the actual price rows; the
    contract is "do everything needed to make the original failure
    no-longer-failing."

    Used by /api/admin/retry-failed and scripts/retry_failed_tasks.py.
    """
    columns = (
        "id, task_type, target, attempts, error_message, "
        "first_seen_at, last_attempt_at, resolved_at"
    )
    with store.connect_ro() as conn:
        rows = [dict(r) for r in conn.execute(
            f"SELECT {columns} FROM failed_tasks WHERE resolved_at IS NULL"
        ).fetchall()]

    resolved = 0
    still_failing = 0
    for row in rows:
        try:
            retry_fn = resolver(row)
            retry_fn()
        except Exception as exc:  # noqa: BLE001 — same boundary
            now = _now_utc_iso()
            sanitized = _sanitize_error_message(f"{type(exc).__name__}: {exc}")
            with store.connect_rw() as conn:
                conn.execute(
                    """
                    UPDATE failed_tasks
                    SET attempts = attempts + 1,
                        last_attempt_at = ?,
                        error_message = ?
                    WHERE id = ?
                    """,
                    (now, sanitized, row["id"]),
                )
            still_failing += 1
            continue
        now = _now_utc_iso()
        with store.connect_rw() as conn:
            conn.execute(
                "UPDATE failed_tasks SET resolved_at = ? WHERE id = ?",
                (now, row["id"]),
            )
        resolved += 1

    return {"resolved": resolved, "still_failing": still_failing}


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
    except Exception as exc:  # noqa: BLE001 — top-level guard
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
    global _active_thread
    portfolio_path = Path(portfolio_path)

    with _thread_lock:
        if _active_thread is not None and _active_thread.is_alive():
            log.info("backfill start: thread already running")
            return _active_thread

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
        _active_thread = t
        t.start()
        return t
