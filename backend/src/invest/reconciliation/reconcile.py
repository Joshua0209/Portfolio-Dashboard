"""Phase 12 — manual-trigger reconciliation between PDF trades and the
Shioaji-overlay trade record.

Run point: NEVER auto-fired. Spec §12 calls out three forbidden trigger
sites:
  - app/backfill_runner.py
  - scripts/snapshot_daily.py
  - scripts/parse_statements.py

The user runs `python scripts/reconcile.py --month YYYY-MM` (CLI) or
clicks "Run Reconciliation" inside the /today Developer Tools accordion
(UI). Both paths land on `run_for_month()`.

A clean diff is silent — no row written to reconcile_events. A non-empty
diff inserts one row per run; the global banner partial picks it up on
the next page load.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

from invest.persistence.daily_store import DailyStore

log = logging.getLogger(__name__)


# --- Trade tuple normalization --------------------------------------------
# What does it mean for "PDF trade X" and "overlay trade Y" to be the same
# trade? This function defines that contract.
#
# Constraints (from spec §12 + plan §3 risk note):
#   - Float prices must be rounded before comparison; raw float equality
#     is brittle (the broker might send 920.0 while yfinance/Shioaji
#     reports 919.999999998).
#   - Trade dates use 'YYYY/MM/DD' in PDF rows and 'YYYY-MM-DD' in overlay
#     rows — both must normalize to the same form.
#   - Side strings are 普買/普賣 in both sources after Phase 11's side
#     normalization (shioaji_client._normalize_side runs on overlay
#     ingest), so equality is structural.
#   - Quantity is integer-shaped in TW (board lots of 1000) but stored
#     as float; round to int.
#
# The tuple ordering matters — it's the hashable key used for set
# difference downstream.


def _normalize_trade_tuple(trade: dict) -> tuple:
    """Turn a trade dict into a hashable tuple suitable for set-equality
    matching across PDF and overlay sources.

    Returns: (date, code, side, qty, price)
      - date:  ISO 'YYYY-MM-DD' (slashes normalized to dashes)
      - code:  unchanged
      - side:  unchanged (Phase 11 already normalized to 普買/普賣)
      - qty:   int(round(qty))            — TW board lots are integer
      - price: round(price, 4)            — 4 decimals tolerates float
                                            noise but catches real diffs
                                            in cents (e.g. 920.00 vs
                                            920.05).
    """
    raw_date = trade.get("date") or ""
    iso_date = raw_date.replace("/", "-") if "/" in raw_date else raw_date
    return (
        iso_date,
        trade.get("code") or "",
        trade.get("side") or "",
        int(round(float(trade.get("qty") or 0))),
        round(float(trade.get("price") or 0), 4),
    )


# --- PDF + overlay extractors --------------------------------------------


def _pdf_trades_for_month(portfolio: dict, month: str) -> list[dict]:
    """Trades from portfolio.json filtered to one YYYY-MM month.

    The trade ledger lives in summary.all_trades — that's what the
    dashboard already reads. We deliberately don't traverse months[].tw
    because the trade table inside a month record may be a subset (the
    parser writes trades into both places but `all_trades` is the
    canonical flattening).
    """
    out: list[dict] = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != "TW":
            continue
        if (t.get("month") or "")[:7] == month:
            out.append(t)
    return out


def _overlay_trades_for_month(store: DailyStore, month: str) -> list[dict]:
    """Trades the overlay imported for dates inside `month`.

    The overlay writes its raw trades nowhere — it only persists the
    derived positions. To reconcile we need the trades themselves, so
    Phase 12 makes a second Shioaji call scoped to the month. Live data
    isn't strictly required if we cached the overlay trade list in a
    table; for v1 we re-query because reconciliation is rare and
    latency-tolerant. The wiring lives in `run_for_month()` which
    accepts an injected client to keep this function pure.
    """
    return []  # placeholder: real fetch happens in run_for_month()


# --- Diff -----------------------------------------------------------------


def diff_trades(
    pdf_rows: Iterable[dict], overlay_rows: Iterable[dict]
) -> dict[str, list[dict]]:
    """Return {only_in_pdf: [...], only_in_overlay: [...]} where each
    entry is the original (un-tupled) trade dict for human inspection.

    A "clean diff" is the canonical happy path: both lists empty.
    """
    pdf_list = list(pdf_rows)
    overlay_list = list(overlay_rows)
    pdf_idx = {_normalize_trade_tuple(t): t for t in pdf_list}
    overlay_idx = {_normalize_trade_tuple(t): t for t in overlay_list}

    only_pdf_keys = set(pdf_idx.keys()) - set(overlay_idx.keys())
    only_overlay_keys = set(overlay_idx.keys()) - set(pdf_idx.keys())
    return {
        "only_in_pdf": [pdf_idx[k] for k in sorted(only_pdf_keys)],
        "only_in_overlay": [overlay_idx[k] for k in sorted(only_overlay_keys)],
    }


# --- Persistence ----------------------------------------------------------


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _persist_event(
    store: DailyStore, month: str, diff: dict[str, list[dict]]
) -> int:
    """Insert one reconcile_events row when the diff is non-empty.

    Stores the diff as JSON in `diff_summary`; the banner reads counts
    out of it for display, the accordion shows the full payload for a
    human to triage.
    """
    only_pdf = diff["only_in_pdf"]
    only_overlay = diff["only_in_overlay"]
    if not only_pdf and not only_overlay:
        return 0
    payload = {
        "only_in_pdf_count": len(only_pdf),
        "only_in_overlay_count": len(only_overlay),
        "only_in_pdf": only_pdf,
        "only_in_overlay": only_overlay,
    }
    with store.connect_rw() as conn:
        cur = conn.execute(
            "INSERT INTO reconcile_events(pdf_month, diff_summary, detected_at) "
            "VALUES (?, ?, ?)",
            (month, json.dumps(payload, default=str, ensure_ascii=False),
             _now_utc_iso()),
        )
        return cur.lastrowid


# --- Public entry ---------------------------------------------------------


def run_for_month(
    store: DailyStore,
    portfolio: dict,
    month: str,
    overlay_client=None,
) -> dict[str, Any]:
    """Reconcile PDF trades vs Shioaji-overlay trades for one month.

    `overlay_client` is a callable: (start, end) → list[trade_dict].
    Tests pass a stub; production passes ShioajiClient().list_trades.
    When overlay_client is None or returns [] (creds unset), the
    "only_in_pdf" side is full and "only_in_overlay" is empty —
    suppressed because there's nothing to compare against; we skip
    persistence in that case so no spurious banner fires.
    """
    pdf_rows = _pdf_trades_for_month(portfolio, month)

    overlay_rows: list[dict] = []
    if overlay_client is not None:
        # Month-end-inclusive window. compute_gap_window in trade_overlay
        # uses the same boundaries (next-day-after-month-end), but here
        # we want the entire month for comparison.
        import calendar
        y, m = (int(p) for p in month.split("-"))
        last_day = calendar.monthrange(y, m)[1]
        start = f"{y:04d}-{m:02d}-01"
        end = f"{y:04d}-{m:02d}-{last_day:02d}"
        try:
            overlay_rows = list(overlay_client(start, end))
        except Exception as exc:
            log.warning("reconcile: overlay fetch failed (%s); treating as empty", exc)
            overlay_rows = []

    if not overlay_rows:
        return {
            "month": month,
            "pdf_trades": len(pdf_rows),
            "overlay_trades": 0,
            "only_in_pdf_count": 0,
            "only_in_overlay_count": 0,
            "skipped_reason": "no_overlay_data",
            "event_id": None,
        }

    diff = diff_trades(pdf_rows, overlay_rows)
    event_id = _persist_event(store, month, diff)
    return {
        "month": month,
        "pdf_trades": len(pdf_rows),
        "overlay_trades": len(overlay_rows),
        "only_in_pdf_count": len(diff["only_in_pdf"]),
        "only_in_overlay_count": len(diff["only_in_overlay"]),
        "skipped_reason": None,
        "event_id": event_id or None,
    }


def record_event(
    store: DailyStore,
    event_type: str,
    detail: dict[str, Any],
    pdf_month: str = "",
) -> int:
    """Persist an audit event into the existing reconcile_events table.

    Used by the Path A trade-overlay audit hook (plan §"Audit hook") to
    surface broker-vs-PDF disagreements. Reuses the existing schema —
    event_type + detail are encoded inside the JSON ``diff_summary``
    envelope as ``{"event_type": ..., "detail": {...}}`` so banners can
    branch on payload shape without a schema migration.

    Returns: the inserted row id (always > 0; this function is not
    silently no-op even on empty detail — empty payloads are still
    legitimate audit signals, e.g., "all expected pairs deferred").
    """
    payload = {"event_type": event_type, "detail": detail}
    with store.connect_rw() as conn:
        cur = conn.execute(
            "INSERT INTO reconcile_events(pdf_month, diff_summary, detected_at) "
            "VALUES (?, ?, ?)",
            (pdf_month, json.dumps(payload, default=str, ensure_ascii=False),
             _now_utc_iso()),
        )
        return cur.lastrowid


def get_open_events(store: DailyStore) -> list[dict[str, Any]]:
    """List undismissed reconcile_events rows for the global banner."""
    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT id, pdf_month, diff_summary, detected_at "
            "FROM reconcile_events WHERE dismissed_at IS NULL "
            "ORDER BY detected_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def dismiss_event(store: DailyStore, event_id: int) -> bool:
    """Mark one event as dismissed. Returns True if a row was updated."""
    with store.connect_rw() as conn:
        cur = conn.execute(
            "UPDATE reconcile_events SET dismissed_at = ? "
            "WHERE id = ? AND dismissed_at IS NULL",
            (_now_utc_iso(), event_id),
        )
        return cur.rowcount > 0
