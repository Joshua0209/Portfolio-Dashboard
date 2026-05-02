"""Phase 5 — Shioaji write side.

This module is the authority flip: it pulls TW trades from the
read-only ShioajiClient and writes them as Trade rows with
source='shioaji'. The PDF-canonical UPSERT guard from the legacy
trade_overlay.py is GONE — Shioaji wins the live track.

Foreign trades are NOT touched here. Phase 0 probe outcome (per
PLAN §3) confirms AccountType.H still returns HTTP 406; foreign
remains PDF-canonical with source='pdf-foreign' written by
ingestion.trade_seeder / trade_verifier.

Two-source design (not three):
    list_realized_pairs and list_open_lots are mutually exclusive
    by SDK contract — a position is either currently held (in
    open_lots) or closed (pair-realized). They never overlap.
    list_trades (today's session fills) is intentionally NOT
    consumed here: anything in it would also be in either
    realized_pairs (today's sells) or open_lots (today's still-
    held buys), provided sync runs after settlement. Sync is
    operator-triggered or scheduled post-close, not a tick
    listener — intraday lag is a non-issue.

    Defense in depth: if the SDK ever violates the mutual-
    exclusion contract (the same (date, code, side, qty) appears
    in both surfaces), _merge_records raises ValueError. Silent
    dedup would mask a real SDK regression.

Idempotency comes from TradeRepo.replace_for_period bounded by
(source='shioaji', [start, end]). Re-running with the same
inputs replaces only Shioaji rows in the window — PDF rows in
the same date range survive (they have source='pdf').

T+1 finalization rule:
    A sync run on day T finalizes through T-1, NOT T. Today's
    data is partial (intraday or settling), so the next sync
    must re-include today in its window. SyncResult.finalized_
    through carries this for the orchestrator's high-water
    mark; the orchestrator persists it and computes the next
    window's start as finalized_through + 1.

Price derivation for open_lots:
    The SDK's StockPositionDetail carries cost_total but not
    per-share price. The Trade aggregate stores price as a
    Decimal field used by analytics (FIFO P&L cost basis). We
    derive price = Decimal(cost_total) / Decimal(qty) — Decimal
    arithmetic, not float, so qty × derived_price reconstructs
    cost_total exactly to 18 digits. No analytics change needed.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as _date, timedelta
from decimal import Decimal
from typing import Any, Callable, Iterable, Optional

from sqlmodel import Session

from invest.brokerage.shioaji_client import ShioajiClient
from invest.domain.trade import Side
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.trade_repo import TradeRepo

_log = logging.getLogger(__name__)


# --- Public API -----------------------------------------------------------


@dataclass(frozen=True)
class SyncResult:
    """Outcome of one sync run.

    Attributes
    ----------
    written
        Count of Trade rows written for source='shioaji' in the
        [start, end] window after this run.
    sources_seen
        Counts per Shioaji surface, pre-merge. Useful for ops triage:
        if realized_pair=0 every run on a real account, the SDK's
        list_profit_loss is misconfigured.
    finalized_through
        High-water mark for the orchestrator. Equals end - 1: the
        T+1 rule. Today's data is partial; the next sync must
        re-include today, so finalized_through stops one day short.
    """

    written: int
    sources_seen: dict[str, int]
    finalized_through: _date


def sync_shioaji_trades(
    client: ShioajiClient,
    session: Session,
    start: _date,
    end: _date,
    *,
    close_resolver: Optional[Callable[[str, str], float | None]] = None,
) -> SyncResult:
    """Pull realized_pairs + open_lots, merge, and write Trade rows.

    The window [start, end] is inclusive. Both surfaces are filtered
    to records with date in [start, end] before merging.

    Returns SyncResult — the client itself never raises (reconnect-
    once-then-fail-quietly contract), so a no-creds or always-failing
    session yields written=0. ValueError DOES propagate from
    _merge_records if the SDK contract (mutual exclusion of
    realized_pair and open_lot) is violated — we want loud failure
    over silent dedup in that case.
    """
    iso_start, iso_end = start.isoformat(), end.isoformat()

    realized = client.list_realized_pairs(iso_start, iso_end)
    lots = client.list_open_lots(close_resolver=close_resolver)
    lots_in_window = [
        lot for lot in lots if iso_start <= str(lot.get("date", "")) <= iso_end
    ]
    if len(lots) != len(lots_in_window):
        _log.debug(
            "sync_shioaji_trades: %d open lots from SDK, %d in window [%s, %s]",
            len(lots), len(lots_in_window), iso_start, iso_end,
        )

    sources_seen = {
        "realized_pair": len(realized),
        "open_lot": len(lots_in_window),
    }

    merged = _merge_records(realized, lots_in_window)
    rows = [r for r in (_record_to_trade(rec) for rec in merged) if r is not None]

    repo = TradeRepo(session)
    repo.replace_for_period(source="shioaji", start=start, end=end, rows=rows)

    return SyncResult(
        written=len(rows),
        sources_seen=sources_seen,
        finalized_through=end - timedelta(days=1),
    )


# --- Internals ------------------------------------------------------------


def _dedup_key(rec: dict[str, Any]) -> tuple[str, str, str, int]:
    """(date, code, side, int(round(qty))).

    Mirrors ingestion.trade_verifier so PDF-vs-Shioaji audit can
    compare records structurally. Price is intentionally excluded —
    Shioaji/PDF disagree by a few decimal places on micro-FX
    rounding, and keying on price would make every cross-source
    pair "different".
    """
    return (
        str(rec.get("date", "")),
        str(rec.get("code", "")),
        str(rec.get("side", "")),
        int(round(float(rec.get("qty", 0)))),
    )


def _merge_records(
    realized: Iterable[dict[str, Any]],
    lots: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Concat realized + lot-as-buy records.

    Defense in depth: if the same dedup key appears in both surfaces,
    the SDK's mutual-exclusion contract has been violated. Raise
    ValueError so the operator sees a hard failure, not a silently
    deduped row that papers over a real broker-side regression.
    """
    out: list[dict[str, Any]] = []
    realized_keys: set[tuple[str, str, str, int]] = set()
    for rec in realized:
        if rec.get("qty", 0) == 0:
            # Degenerate pair (legs unrecoverable). Don't write a
            # qty=0 Trade row — it's not a trade. The reconcile-
            # event channel surfaces this case (Cycle 38).
            continue
        out.append(rec)
        realized_keys.add(_dedup_key(rec))

    for lot in lots:
        buy = _lot_as_buy(lot)
        if buy is None:
            continue
        key = _dedup_key(buy)
        if key in realized_keys:
            raise ValueError(
                "SDK contract violation: same (date, code, side, qty) "
                f"appears in both list_realized_pairs and list_open_lots: "
                f"{key}. Refusing to silently dedup; investigate before "
                f"resyncing."
            )
        out.append(buy)

    return out


def _lot_as_buy(lot: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Open-lot dict → buy-trade-shape record.

    Per-share price is derived as Decimal(cost_twd) / Decimal(qty)
    — Decimal arithmetic, not float — so analytics computing
    qty × price reconstruct cost exactly to 18 digits.

    Returns None for pathological qty=0 lots (would div-by-zero).

    Note: the 'type' key (現股 / 融資 / 融券) is propagated here for
    completeness but is intentionally dropped by _record_to_trade —
    the Trade schema has no type column. Reserved for a future schema
    extension if margin/short-cost-asymmetry analytics need it.
    """
    qty = float(lot.get("qty", 0))
    if qty <= 0:
        return None
    cost = lot.get("cost_twd")
    if cost is None:
        return None
    price = Decimal(str(cost)) / Decimal(str(int(qty)))
    return {
        "date": lot.get("date"),
        "code": lot.get("code"),
        "side": "普買",
        "qty": qty,
        "price": price,
        "ccy": lot.get("ccy", "TWD"),
        "venue": lot.get("venue", "TW"),
        "type": lot.get("type", "現股"),
    }


def _record_to_trade(rec: dict[str, Any]) -> Optional[Trade]:
    """Project record dict → ORM Trade row.

    Skips records with qty<=0 (degenerate or pathological) — they're
    handled upstream by _merge_records, but the guard is cheap and
    keeps this function safe for direct callers.

    side mapping: 普買 → Side.CASH_BUY (1), 普賣 → Side.CASH_SELL (2).
    Any other side string logs a warning and defaults to CASH_SELL so
    the row is not silently dropped; the operator can investigate via
    the anomalous side value in the DB.

    Fields intentionally dropped at the ORM boundary:
      - 'type' (現股/融資/融券): Trade schema has no type column.
        Reserved for a future margin-cost-asymmetry analytics feature.
      - 'pair_id': reserved for Cycle 38 live-audit hook.
      - 'pnl': P&L is derived by analytics, not stored on raw trades.
      - 'ccy'/'venue': hard-coded to TWD/TW (Phase 0 probe confirmed
        AccountType.H returns 406; all Shioaji records are TW-only).
    """
    qty = int(round(float(rec.get("qty", 0))))
    if qty <= 0:
        return None

    side_str = str(rec.get("side", ""))
    if side_str == "普買":
        side = int(Side.CASH_BUY)
    elif side_str == "普賣":
        side = int(Side.CASH_SELL)
    else:
        _log.warning(
            "_record_to_trade: unrecognised side %r for code=%s date=%s; "
            "defaulting to CASH_SELL — investigate broker record shape",
            side_str, rec.get("code"), rec.get("date"),
        )
        side = int(Side.CASH_SELL)

    raw_price = rec.get("price", 0)
    price = raw_price if isinstance(raw_price, Decimal) else Decimal(str(raw_price))

    iso_date = str(rec.get("date"))
    y, m, d = iso_date.split("-")

    return Trade(
        date=_date(int(y), int(m), int(d)),
        code=str(rec.get("code", "")),
        side=side,
        qty=qty,
        price=price,
        currency="TWD",
        source="shioaji",
        venue="TW",
    )
