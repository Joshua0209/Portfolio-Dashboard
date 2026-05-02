"""Phase 11 — post-PDF trade overlay.

Bridges the gap between the most recent PDF month-end and "now" using
trades pulled from Shioaji. The PDF parser remains canonical for any
date covered by a monthly statement; the overlay only writes rows for
dates strictly after the latest PDF month, marked `source='overlay'` so
they're trivially distinguishable from PDF-sourced positions.

When credentials are missing or no gap exists, this module is a clean
no-op — never crashes, never raises, never partial-writes.

Three-source merge (Path A, plan §"Code changes"):
  1. ``client.list_open_lots()`` — currently-held lots become synthetic
     '普買' opening trades (locks in 2330 cash + 00981A margin per probe).
  2. ``client.list_realized_pairs(gap_start, gap_end)`` — buy legs +
     sell summary for closed pairs whose sell date is in the window.
     Buy legs may pre-date gap_start (decision #1 option C).
  3. ``client.list_trades()`` — session-only safety net for trades
     placed since the last refresh.

All three sources are unified into a single trade-shaped list, deduped
by ``(date, code, side, int(round(qty)))``, and folded into qty_history.

Audit hook (decision #1 option C, STRICT firing rule):
  For each pair_id from list_realized_pairs, the SDK's buy-leg count
  is compared against the PDF parser's trade rows for the same
  (code, ≤sell_date) window. ANY count divergence — including legitimate
  broker consolidations — fires a reconcile event via
  ``app.reconcile.record_event()``.
"""
from __future__ import annotations

import calendar
import json
import logging
from datetime import datetime, date, timezone
from typing import Any, Iterable

from . import reconcile
from .daily_store import DailyStore
from .shioaji_client import ShioajiClient

log = logging.getLogger(__name__)


# --- Overlay net_twd math ------------------------------------------------
#
# User-confirmed approximation (decision 2026-05-01):
#   TW buy   (普買 / 資買): fee_pct = 0.001425                  → net = -gross * (1 + fee)
#   TW sell  (普賣 / 資賣): fee_pct = 0.001425, tax_pct = 0.003 → net = +gross * (1 - fee - tax)
#   Foreign  (買進 / 賣出):                                     → net = ±gross   (no fee/tax)
#
# Margin trades approximate as cash — the broker loan portion isn't
# recoverable from the read-only SDK. PDFs remain canonical for any month
# fully covered by a statement; this approximation only feeds the
# running_cash_twd walk for post-PDF dates.
TW_BUY_FEE_PCT = 0.001425
TW_SELL_FEE_PCT = 0.001425
TW_SELL_TAX_PCT = 0.003


def _compute_overlay_net_twd(
    trade: dict, fx_to_twd: float | None = None,
) -> dict[str, float]:
    """Return {fee_twd, tax_twd, gross_twd, net_twd} for one overlay trade.

    `fx_to_twd` is required for non-TWD venues; ignored for TW.
    """
    qty = float(trade.get("qty") or 0)
    price = float(trade.get("price") or 0)
    side = trade.get("side") or ""
    venue = trade.get("venue") or "TW"

    is_buy = "買" in side
    if venue == "TW":
        gross_twd = qty * price
        if is_buy:
            fee_twd = gross_twd * TW_BUY_FEE_PCT
            tax_twd = 0.0
            net_twd = -(gross_twd + fee_twd)
        else:
            fee_twd = gross_twd * TW_SELL_FEE_PCT
            tax_twd = gross_twd * TW_SELL_TAX_PCT
            net_twd = +(gross_twd - fee_twd - tax_twd)
    else:
        rate = float(fx_to_twd or 1.0)
        gross_twd = qty * price * rate
        fee_twd = 0.0
        tax_twd = 0.0
        net_twd = -gross_twd if is_buy else +gross_twd

    return {
        "fee_twd": fee_twd,
        "tax_twd": tax_twd,
        "gross_twd": gross_twd,
        "net_twd": net_twd,
    }


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


def _fx_for_date(store: DailyStore, ccy: str, d: str) -> float | None:
    """Latest fx_daily.rate_to_twd for `ccy` on or before `d` (forward-fill).

    Returns None when no FX row exists at all. The overlay's foreign
    leg is currently a no-op (the H-account is walled off behind HTTP
    406) so this function is for forward-compat — it ensures that if
    a foreign overlay trade ever lands, we won't silently approximate
    fx=1.0 on a USD trade.
    """
    if ccy == "TWD":
        return 1.0
    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT rate_to_twd FROM fx_daily WHERE ccy = ? AND date <= ? "
            "ORDER BY date DESC LIMIT 1",
            (ccy, d),
        ).fetchone()
    return row[0] if row else None


def _persist_overlay_trades(
    store: DailyStore,
    overlay_trades: list[dict],
    gap_start: str,
    gap_end: str,
) -> int:
    """Replace trades_overlay rows in [gap_start, gap_end] with the
    current deduped overlay trade list.

    DELETE-then-INSERT (rather than UPSERT-only) so a trade that
    disappears from the SDK between refreshes (e.g., broker corrected
    a fill) doesn't linger as a phantom. The PK (date, code, side, qty_int)
    matches the dedup key from `_trade_dedup_key`.

    PRE-GAP LEG FILTER (2026-05-01): list_realized_pairs returns buy legs
    that may pre-date gap_start (decision #1 option C, used by the audit
    hook to see the full broker history). Those pre-gap legs are already
    represented in PDF summary.all_trades, so persisting them here would
    cause the derive() cash walk to subtract their cost twice. Filter to
    [gap_start, gap_end] before persistence — the audit hook still sees
    the unfiltered list upstream.
    """
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows_to_insert: list[tuple] = []
    for t in overlay_trades:
        d = t.get("date") or ""
        if d < gap_start or d > gap_end:
            continue
        ccy = t.get("ccy") or "TWD"
        fx = _fx_for_date(store, ccy, d or gap_end) or 1.0
        money = _compute_overlay_net_twd(t, fx_to_twd=fx)
        rows_to_insert.append((
            t.get("date"),
            t.get("code"),
            t.get("side"),
            int(round(float(t.get("qty") or 0))),
            float(t.get("qty") or 0),
            float(t.get("price") or 0),
            money["fee_twd"],
            money["tax_twd"],
            money["gross_twd"],
            money["net_twd"],
            ccy,
            t.get("venue") or "TW",
            t.get("type") or "現股",
            t.get("pair_id"),
            t.get("_source") or "list_trades",
            fetched_at,
        ))
    with store.connect_rw() as conn:
        conn.execute(
            "DELETE FROM trades_overlay WHERE date BETWEEN ? AND ?",
            (gap_start, gap_end),
        )
        if rows_to_insert:
            conn.executemany(
                """
                INSERT INTO trades_overlay(
                    date, code, side, qty_int, qty, price,
                    fee_twd, tax_twd, gross_twd, net_twd,
                    ccy, venue, type, pair_id, source, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, code, side, qty_int) DO UPDATE SET
                    qty = excluded.qty,
                    price = excluded.price,
                    fee_twd = excluded.fee_twd,
                    tax_twd = excluded.tax_twd,
                    gross_twd = excluded.gross_twd,
                    net_twd = excluded.net_twd,
                    ccy = excluded.ccy,
                    venue = excluded.venue,
                    type = excluded.type,
                    pair_id = excluded.pair_id,
                    source = excluded.source,
                    fetched_at = excluded.fetched_at
                """,
                rows_to_insert,
            )
    log.info(
        "trades_overlay: persisted %d row(s) in [%s..%s]",
        len(rows_to_insert), gap_start, gap_end,
    )
    return len(rows_to_insert)


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


# --- 3-source unification + dedup ----------------------------------------


def _trade_dedup_key(t: dict) -> tuple:
    """Structural key for collapsing the same trade across sources.

    Plan §"Three-source dedup": (date, code, side, qty). Excludes price
    because:
      - open_lot synthetic trades don't carry a per-share price
      - small price drift across sources (920.0 vs 919.99) is normal
        broker rounding and shouldn't break dedup

    Uses int(round(qty)) — TW shares are integer-shaped (including odd-
    lot 零股 < 1000 shares).
    """
    return (
        t.get("date") or "",
        t.get("code") or "",
        t.get("side") or "",
        int(round(float(t.get("qty") or 0))),
    )


def _overlay_trades_from_lots(lots: list[dict]) -> list[dict]:
    """Project open-lot records into synthetic '普買' opening trades.

    Each lot represents a position currently held in the account; the
    overlay treats it as an opening fill on lot.date. The merge layer
    folds these into qty_history alongside real trades.
    """
    out = []
    for lot in lots:
        code = lot.get("code")
        d = lot.get("date")
        qty = lot.get("qty")
        if not code or not d or qty is None:
            continue
        out.append({
            "date": d,
            "code": code,
            "side": "普買",
            "qty": float(qty),
            "price": 0.0,  # synthetic; cost_twd is the meaningful figure
            "ccy": lot.get("ccy") or "TWD",
            "venue": lot.get("venue") or "TW",
            "type": lot.get("type") or "現股",
            "_source": "open_lot",
        })
    return out


def _overlay_trades_from_pairs(pairs: list[dict]) -> list[dict]:
    """Pass through realized-pair records as trade-shaped overlay records.

    Plan §"3-source": list_realized_pairs already emits buy legs with
    side='普買' and sell summaries with side='普賣'. We simply tag them
    with `_source='realized_pair'` so the merge layer can identify them
    if needed later, but otherwise leave the shape alone — the dedup
    key is structural and works across sources.

    Sell summaries with qty=0 (the C-fallback degenerate case) drop out
    here: their qty_history contribution is zero and they can't be
    deduped meaningfully. The audit hook still sees them via the
    upstream pairs list.
    """
    out = []
    for p in pairs:
        if p.get("side") == "普賣" and float(p.get("qty") or 0) == 0:
            # Degenerate qty=0 sell — skip projection, keep audit
            # visibility upstream.
            continue
        out.append({**p, "_source": "realized_pair"})
    return out


def _dedup_overlay_trades(trades: list[dict]) -> list[dict]:
    """Keep first occurrence per structural key. Deterministic ordering
    by source priority: realized_pair > list_trades > open_lot. Open-lot
    records are the lowest-fidelity (synthetic, no price) so they yield
    to anything else with the same key."""
    priority = {"realized_pair": 0, "list_trades": 1, "open_lot": 2}
    sorted_trades = sorted(
        trades, key=lambda t: priority.get(t.get("_source"), 99)
    )
    seen: dict[tuple, dict] = {}
    for t in sorted_trades:
        k = _trade_dedup_key(t)
        if k in seen:
            continue
        seen[k] = t
    return list(seen.values())


# --- Audit hook (Strict firing rule) -------------------------------------


def _pdf_buy_count_for(
    portfolio: dict, code: str, sell_date: str,
) -> tuple[int, list[dict]]:
    """Count PDF buy trades for a code dated on or before sell_date.

    Matches the same (code, side='普買', date ≤ sell_date) window the
    SDK's list_profit_loss_detail covers. Returns (count, trade_rows)
    so the audit event payload can show the actual trades for triage.
    """
    rows = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "TW":
            continue
        if t.get("code") != code:
            continue
        if t.get("side") != "普買":
            continue
        raw_date = t.get("date") or ""
        iso_date = raw_date.replace("/", "-") if "/" in raw_date else raw_date
        if iso_date and iso_date <= sell_date:
            rows.append(t)
    return len(rows), rows


def _pair_groups(pairs: list[dict]) -> dict[Any, dict[str, Any]]:
    """Group pairs by pair_id → {sell_date, code, buy_legs}.

    Sells without legs (qty=0 degenerate) still appear with buy_legs=[].
    """
    groups: dict[Any, dict[str, Any]] = {}
    for p in pairs:
        pid = p.get("pair_id")
        if pid is None:
            continue
        g = groups.setdefault(pid, {"sell_date": None, "code": None,
                                    "buy_legs": []})
        if p.get("side") == "普買":
            g["buy_legs"].append(p)
            if not g["code"]:
                g["code"] = p.get("code")
        elif p.get("side") == "普賣":
            g["sell_date"] = p.get("date")
            g["code"] = p.get("code")
    return groups


def _open_audit_pair_ids(store: DailyStore) -> set:
    """pair_ids that already have an undismissed audit event of type
    'broker_pdf_buy_leg_mismatch'. Re-running the merge against the
    same SDK state must not insert a duplicate row.

    Dismissed events do NOT block a refire — dismissal means "I've
    reviewed this divergence", and if the divergence is still present
    on the next run the operator wants to see it again.
    """
    seen: set = set()
    for e in reconcile.get_open_events(store):
        try:
            payload = json.loads(e["diff_summary"]) if e["diff_summary"] else {}
        except (json.JSONDecodeError, TypeError):
            continue
        if payload.get("event_type") != "broker_pdf_buy_leg_mismatch":
            continue
        pid = (payload.get("detail") or {}).get("pair_id")
        if pid is not None:
            seen.add(pid)
    return seen


def _audit_policy(
    pair_id: Any,
    code: str,
    sell_date: str,
    sdk_legs: list[dict],
    pdf_buy_rows: list[dict],
) -> dict | None:
    """Decide whether THIS realized pair should fire a reconcile event.

    Return None to stay silent, or a `detail` dict to fire (will be
    encoded inside diff_summary by record_event()).

    BACKGROUND (2026-05-01 false-positive surge):
    The original STRICT rule compared `len(sdk_legs)` against
    `len(pdf_buy_rows)` and fired on any divergence. That comparison is
    structurally apples-to-oranges:
      • sdk_legs = buy legs FIFO-consumed by THIS one sell pair
      • pdf_buy_rows = ALL buys for `code` ever, dated ≤ sell_date
    A code held for years where the user has done multiple buy/sell
    cycles will have many PDF buys that were already FIFO-closed by
    earlier sells — so the strict rule fires on every realized pair.
    Result: 8 alerts on 8 pairs, all false positives.

    Currently this returns None unconditionally — the audit hook is
    silent. The PDF-vs-overlay reconciliation via the manual "Reconcile
    this month" button (`reconcile.run_for_month`) is a separate path
    that already provides operator-triggered diff.

    TODO: replace with a policy you actually want. Three meaningful
    alternatives if you choose to add one back — see commit history /
    PLAN-shioaji-historical-trades.md §"Audit hook" for context:

      Option A — broker self-sanity:
        sdk_qty = sum(int(round(float(l.get("qty") or 0))) for l in sdk_legs)
        sell_qty = ???   # would need pl.qty (currently dropped)
        if sdk_qty != sell_qty:
            return {...}
        return None

      Option B — PDF coverage gap:
        pdf_keys = {(t["date"].replace("/", "-"),
                     int(round(float(t.get("qty") or 0))))
                    for t in pdf_buy_rows}
        missing = [l for l in sdk_legs
                   if (l["date"], int(round(float(l.get("qty") or 0))))
                   not in pdf_keys]
        if missing:
            return {"pair_id": pair_id, "code": code, "sell_date": sell_date,
                    "missing_legs": missing}
        return None
    """
    return None


def _fire_audit_events(
    store: DailyStore, portfolio: dict, pairs: list[dict],
) -> int:
    """Walk pair groups and fire whatever `_audit_policy` decides.

    Dedup contract: a pair_id with an existing OPEN audit event is
    skipped to prevent the banner count from doubling on every refresh.
    Dismissed events allow the same pair_id to refire.
    """
    already_open = _open_audit_pair_ids(store)
    fired = 0
    for pid, g in _pair_groups(pairs).items():
        if pid in already_open:
            continue
        sell_date = g.get("sell_date")
        code = g.get("code")
        if not sell_date or not code:
            continue
        _, pdf_rows = _pdf_buy_count_for(portfolio, code, sell_date)
        detail = _audit_policy(pid, code, sell_date, g["buy_legs"], pdf_rows)
        if detail is None:
            continue
        reconcile.record_event(
            store,
            event_type="broker_pdf_buy_leg_mismatch",
            detail=detail,
            pdf_month=sell_date[:7],
        )
        fired += 1
    return fired


# --- Production close resolver ------------------------------------------


def _make_close_resolver(store: DailyStore):
    """Wire a DailyStore-backed (code, date) → close lookup for
    list_open_lots. Returns None when (code, date) has no row."""
    def _resolve(code: str, iso_date: str) -> float | None:
        with store.connect_ro() as conn:
            row = conn.execute(
                "SELECT close FROM prices WHERE date = ? AND symbol = ?",
                (iso_date, code),
            ).fetchone()
        return row[0] if row else None
    return _resolve


def pull_sdk_sources(
    client: ShioajiClient,
    store: DailyStore,
    gap_start: str,
    gap_end: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Pull all 3 read-only SDK surfaces in one shot.

    Returns ``(session_trades, open_lots, realized_pairs)``. Each source
    is best-effort — ShioajiClient swallows its own exceptions and
    returns ``[]`` on failure, so a partial outage on one surface
    doesn't poison the others.

    Extracted as the single seam between two consumers (added 2026-05-01
    to fix the price-fetch race):
      1. ``snapshot_daily.run()`` calls this *before* the price-fetch
         step, derives the overlay-discovered symbol set via
         ``discover_overlay_symbols()``, and fetches prices for any
         symbols not already in the PDF set.
      2. ``merge()`` consumes the same tuple via its ``sdk_data=``
         parameter so the SDK isn't called twice in one refresh.
    """
    if not client.lazy_login():
        return [], [], []
    session_trades = client.list_trades(gap_start, gap_end) or []
    open_lots = client.list_open_lots(
        close_resolver=_make_close_resolver(store)
    ) or []
    realized_pairs = client.list_realized_pairs(gap_start, gap_end) or []
    return session_trades, open_lots, realized_pairs


def discover_overlay_symbols(
    client: ShioajiClient,
    store: DailyStore,
    gap_start: str,
    gap_end: str,
) -> set[str]:
    """Set of TW codes the overlay will reference for the given window.

    snapshot_daily uses this to widen its price-fetch list. The PDF
    parser only knows about codes still held at the latest month-end;
    the overlay discovers everything the broker reports in the same
    window — including codes that opened and closed entirely after the
    last PDF (e.g. a same-month round-trip).

    A code in this set without a price on the gap dates is the silent
    failure mode that this function was added to prevent: merge() would
    skip every overlay write at ``if close is None: continue``.
    """
    session, lots, pairs = pull_sdk_sources(client, store, gap_start, gap_end)
    out: set[str] = set()
    for src in (session, lots, pairs):
        for r in src:
            code = r.get("code")
            if code:
                out.add(code)
    return out


def merge(
    store: DailyStore,
    portfolio: dict,
    client: ShioajiClient,
    gap_start: str | None,
    gap_end: str | None,
    sdk_data: tuple[list[dict], list[dict], list[dict]] | None = None,
) -> dict[str, Any]:
    """Pull overlay trades from `client`, project them onto positions_daily
    + portfolio_daily for the gap window, and return a summary dict.

    When ``sdk_data`` is provided (the two-pass orchestration path),
    merge() skips its internal ``client`` calls entirely and projects
    the pre-pulled tuple ``(session_trades, open_lots, realized_pairs)``.
    The orchestrator (snapshot_daily) uses this to call SDK surfaces
    once, fetch prices for newly-discovered symbols in between, and
    then run the projection — without paying for a second SDK roundtrip.

    Skipped (no-op) reasons:
      - 'no_gap'                  — gap_start/gap_end not provided.
      - 'shioaji_unconfigured'    — creds missing or shioaji not installed.

    Successful runs return:
      {overlay_trades: N, dates_written: M, skipped_reason: None}
    """
    if not gap_start or not gap_end:
        log.info("trade_overlay skipped: reason=no_gap")
        return {"overlay_trades": 0, "dates_written": 0, "skipped_reason": "no_gap"}

    if sdk_data is not None:
        session_trades, open_lots, realized_pairs = sdk_data
    else:
        if not client.lazy_login():
            log.info("trade_overlay skipped: reason=shioaji_unconfigured")
            return {
                "overlay_trades": 0,
                "dates_written": 0,
                "skipped_reason": "shioaji_unconfigured",
            }
        session_trades, open_lots, realized_pairs = pull_sdk_sources(
            client, store, gap_start, gap_end
        )

    log.info(
        "trade_overlay: 3-source pull: session=%d lots=%d pairs=%d window=[%s..%s]",
        len(session_trades), len(open_lots), len(realized_pairs),
        gap_start, gap_end,
    )

    # Strict-rule audit hook fires BEFORE dedup/projection so a count
    # mismatch is surfaced even if the diff also happens to dedup down
    # to a single record.
    audit_fired = _fire_audit_events(store, portfolio, realized_pairs)
    if audit_fired:
        log.info(
            "trade_overlay audit: fired %d reconcile event(s)", audit_fired
        )

    # Unify all 3 sources into trade-shaped records, then dedup by
    # (date, code, side, int(round(qty))).
    raw_overlay = (
        _overlay_trades_from_pairs(realized_pairs)
        + [{**t, "_source": "list_trades"} for t in session_trades]
        + _overlay_trades_from_lots(open_lots)
    )
    overlay_trades = _dedup_overlay_trades(raw_overlay)

    log.info(
        "trade_overlay: deduped %d → %d records",
        len(raw_overlay), len(overlay_trades),
    )

    # Persist deduped trades to trades_overlay (Bug 2 fix, 2026-05-01).
    # Replaces the gap-window slice on every run so a refresh that drops a
    # previously-seen trade (e.g. broker corrected a fill) doesn't leave
    # phantoms behind. derive() reads PDF + this table for the cash walk.
    _persist_overlay_trades(store, overlay_trades, gap_start, gap_end)

    # Build the qty timeline from PDF + deduped overlay records. Overlay
    # trades are layered on top of PDF history so cumulative qty math
    # stays correct even when overlay trades reference codes the user
    # already held at the last month-end.
    qty_history = _qty_history_from_portfolio(portfolio)
    for t in overlay_trades:
        code = t["code"]
        sign = 1 if "買" in (t.get("side") or "") else -1
        qty_history.setdefault(code, []).append((t["date"], sign * float(t["qty"])))
    for v in qty_history.values():
        v.sort(key=lambda r: r[0])

    # Per-share avg cost from latest PDF month — combined with current
    # qty at write time to produce total cost_local. Matches the schema
    # convention used by every reader (see backfill_runner.py:422 note).
    avg_cost_at: dict[str, float] = {}
    for m in portfolio.get("months", []):
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty_pdf = float(h.get("qty", 0) or 0)
            if qty_pdf <= 0:
                continue
            avg = h.get("avg_cost")
            if avg is None:
                avg = float(h.get("cost", 0) or 0) / qty_pdf
            avg_cost_at[code] = float(avg or 0)

    # We persist overlay rows for every priced day in the gap, for every
    # code with a non-zero opening (or post-overlay-trade) qty. This
    # mirrors backfill_runner's mv-snapshot loop but scoped to the gap.
    priced_dates = _priced_dates_in_range(store, gap_start, gap_end)
    if not priced_dates:
        log.info(
            "trade_overlay merged: trades=%d dates_written=0 window=[%s..%s] "
            "(no priced dates in window — overlay rows deferred until prices arrive)",
            len(overlay_trades), gap_start, gap_end,
        )
        return {"overlay_trades": len(overlay_trades), "dates_written": 0,
                "skipped_reason": None}

    affected_codes = set(qty_history.keys())
    pdf_locked = _existing_pdf_rows(store, affected_codes, priced_dates)

    # Single-writer architecture (2026-05-01): merge() writes only
    # positions_daily for overlay-only (date, code) keys. portfolio_daily
    # is owned by backfill_runner._derive_positions_and_portfolio, which
    # folds in overlay rows via a SUM query keyed on source='overlay'.
    # This eliminates the prior class of bug where merge() and derive()
    # both wrote equity_twd with different forward-fill / qty-history
    # rules and the second writer's value silently overwrote the first.
    rows_written = 0

    with store.connect_rw() as conn:
        for d in priced_dates:
            for code, changes in qty_history.items():
                qty = sum(q for date_, q in changes if date_ <= d)
                if qty <= 0:
                    continue
                if (d, code) in pdf_locked:
                    # PDF is canonical; derive will pick up the PDF row
                    # from positions_daily on its own walk. No overlay
                    # write here.
                    continue
                close = _close_for(store, code, d)
                if close is None:
                    continue
                mv_local = qty * close
                # Total cost in local ccy. See backfill_runner.py:422 for
                # why cost_local must be total, not per-share.
                cost_local = qty * avg_cost_at.get(code, 0.0)
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
                    (d, code, qty, cost_local, mv_local, mv_local, "現股"),
                )
                rows_written += 1

    log.info(
        "trade_overlay merged: trades=%d dates_written=%d window=[%s..%s]",
        len(overlay_trades), rows_written, gap_start, gap_end,
    )
    return {
        "overlay_trades": len(overlay_trades),
        "dates_written": rows_written,
        "skipped_reason": None,
    }
