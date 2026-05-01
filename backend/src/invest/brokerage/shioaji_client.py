"""Read-only Shioaji client (Phase 5 — brokerage-authority flip).

Hard invariants (verified by tests/brokerage/test_shioaji_client.py):

  1. This module imports ONLY the read-only Shioaji API surface.
     No order placement. No CA activation. No cancel/update. None
     of it. Phase 5 inverts the source-of-truth contract from
     PDF-canonical to Shioaji-canonical, which makes the read-only
     guard MORE important, not less — Shioaji now writes the live
     track, so a future commit quietly adding an Order import
     would silently enable trade execution.

  2. When SINOPAC_API_KEY / SINOPAC_SECRET_KEY are unset, the
     client is a trivially-stubbed no-op: all three surfaces
     return []. The dashboard is fully functional in this mode
     (PDF-only data via ingestion/trade_seeder).

  3. On session invalidation mid-run, the client transparently
     logs out, re-logs-in once, and retries. A second consecutive
     failure returns [] rather than propagating — the data layer
     never crashes the UI.

If the `shioaji` package is not installed at all (dev environments
that skip the ~200MB pyzmq dependency), the module imports cleanly
and behaves as if creds were unset.

Exposes three surfaces, one per Shioaji read endpoint:

  list_trades(start, end)         → today-window fills (session)
  list_open_lots(close_resolver)  → currently-held lots
  list_realized_pairs(begin, end) → buy legs + sell summary per pair
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_TPE = ZoneInfo("Asia/Taipei")

# Optional dependency — never fail the import path if unavailable.
# Narrow to ImportError/ModuleNotFoundError so a partially-installed
# shioaji that throws something else during its own __init__ surfaces
# loudly instead of silently being treated as absent.
try:
    import shioaji as _shioaji  # noqa: F401
    _SHIOAJI_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    _shioaji = None  # type: ignore[assignment]
    _SHIOAJI_AVAILABLE = False


def to_taipei_date(utc_dt: datetime) -> str:
    """UTC → 'YYYY-MM-DD' in Asia/Taipei.

    Trade timestamps come back from Shioaji in UTC (or naive — the SDK
    is inconsistent across endpoints). The portfolio's canonical
    "trade date" is the TPE business date, so a UTC late-evening
    timestamp must roll into the next TPE day to match what the user
    sees in their statement.
    """
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(_TPE).date().isoformat()


def _make_session() -> Any:
    """Construct a fresh `shioaji.Shioaji()` session.

    Indirection lets tests substitute a fake without touching the
    import line.
    """
    if not _SHIOAJI_AVAILABLE:
        raise RuntimeError("shioaji module is not installed")
    return _shioaji.Shioaji()  # type: ignore[union-attr]


# StockOrderCond.{Cash, MarginTrading, ShortSelling} → 現股 / 融資 / 融券.
# Mirrors the PDF parser's holdings_detail.type vocabulary so analytics
# layers (FIFO P&L, 融資 cost-asymmetry handling) don't need a
# translation step. Unknown conds → '現股' as the safest default.
_COND_TO_TYPE: dict[str, str] = {
    "Cash": "現股",
    "MarginTrading": "融資",
    "ShortSelling": "融券",
}


def _enum_value(obj: Any, default: str = "") -> str:
    """Pull `.value` off an enum-shaped attr, or fall back to str()."""
    if obj is None:
        return default
    val = getattr(obj, "value", None)
    if val is not None:
        return str(val)
    return str(obj) or default


def _normalize_side(raw: str) -> str:
    """Buy → 普買, Sell → 普賣.

    The PDF parser writes 普買/普賣 for TW cash trades. The Shioaji
    sync must emit the same strings so trade_verifier can compare
    records by structural equality without a translation step.
    """
    if not raw:
        return ""
    s = raw.lower()
    if s.startswith("buy") or s == "b":
        return "普買"
    if s.startswith("sell") or s == "s":
        return "普賣"
    return raw


def _extract_fills(
    trade: Any, start_date: str, end_date: str,
) -> list[dict[str, Any]]:
    """One Trade-like object → 0+ fill records.

    shioaji 1.3.x Trade has `.contract` (code, currency), `.order`
    (action enum), and `.status.deals[]` where each Deal has
    (ts: float epoch, quantity, price). One Trade with two partial
    fills emits two records — the merge layer dedups by
    (date, code, side, qty) so summing fills prematurely would
    collapse legitimate dual-fill orders.

    Records outside [start_date, end_date] in TPE date are dropped.
    Malformed records (missing ts/qty/price) are skipped silently —
    best-effort, never crash.
    """
    contract = getattr(trade, "contract", None)
    order = getattr(trade, "order", None)
    status = getattr(trade, "status", None)
    if contract is None or order is None or status is None:
        return []

    code = getattr(contract, "code", None) or ""
    ccy = getattr(contract, "currency", None) or "TWD"
    action = getattr(order, "action", None)
    if action is not None:
        side_str = getattr(action, "value", None) or str(action)
    else:
        side_str = ""

    deals = getattr(status, "deals", None) or []
    out: list[dict[str, Any]] = []
    for d in deals:
        ts_epoch = getattr(d, "ts", None)
        qty = getattr(d, "quantity", None)
        price = getattr(d, "price", None)
        if ts_epoch is None or qty is None or price is None:
            continue
        ts_dt = datetime.fromtimestamp(float(ts_epoch), tz=timezone.utc)
        tpe_date = to_taipei_date(ts_dt)
        if not (start_date <= tpe_date <= end_date):
            continue
        out.append({
            "date": tpe_date,
            "code": str(code),
            "side": _normalize_side(side_str),
            "qty": float(qty),
            "price": float(price),
            "ccy": ccy,
            "venue": "TW",
        })
    return out


def _extract_lot(
    lot: Any,
    close_resolver: Callable[[str, str], float | None] | None,
) -> dict[str, Any] | None:
    """One StockPositionDetail → project record, or None if unusable.

    The SDK's `quantity` field is ALWAYS 0 for 零股 (Phase 0 probe
    finding). qty is derived from MV/close on the lot's entry date,
    where close comes from the injected resolver (typically a
    DailyStore-backed lookup). When the close is unavailable, skip
    the lot entirely — silent partial data is worse than 'we don't
    know'; the merge layer can't reconcile qty=None.
    """
    code = getattr(lot, "code", None)
    entry_date = getattr(lot, "date", None)
    cost_total = getattr(lot, "price", None)         # SDK quirk: total NTD
    mv_total = getattr(lot, "last_price", None)      # SDK quirk: total NTD

    if not code or not entry_date or cost_total is None or mv_total is None:
        return None

    close = None
    if close_resolver is not None:
        try:
            close = close_resolver(str(code), str(entry_date))
        except Exception:
            close = None
    if close is None or close == 0:
        log.warning(
            "list_open_lots: close unavailable for %s on %s; skipping lot",
            code, entry_date,
        )
        return None

    qty = float(round(float(mv_total) / float(close)))
    cond_str = _enum_value(getattr(lot, "cond", None))
    type_label = _COND_TO_TYPE.get(cond_str, "現股")
    ccy = _enum_value(getattr(lot, "currency", None), default="TWD") or "TWD"

    return {
        "date": str(entry_date),
        "code": str(code),
        "qty": qty,
        "cost_twd": float(cost_total),
        "mv_twd": float(mv_total),
        "type": type_label,
        "ccy": ccy,
        "venue": "TW",
    }


def _qty_from_leg(leg: Any) -> float:
    """Buy-leg qty from cost/price.

    SDK quirk: leg.quantity is always 0 for 零股. Recover from
    cost (total NTD) ÷ price (per-share NTD). round() is used because
    the broker's cost field already absorbs fee rounding errors —
    but the share count is integer-shaped (TW shares are whole numbers,
    including odd-lot 零股 which is integer < 1000).
    """
    cost = getattr(leg, "cost", None)
    price = getattr(leg, "price", None)
    if cost is None or price is None or float(price) == 0:
        return 0.0
    return float(round(float(cost) / float(price)))


def _extract_realized_pair(
    pl: Any, legs: list[Any],
) -> list[dict[str, Any]]:
    """One (summary, [legs]) → N+1 records.

    N buy-leg records (side='普買') with qty=cost/price, plus
    one sell summary (side='普賣') with qty=sum-of-leg-qtys.

    When `legs` is empty (degenerate C-fallback case from plan
    §Path A — list_profit_loss_detail rate-limited or partial-
    response failure), the sell summary still emits with qty=0 so
    the merge layer can fire a reconcile event. Without this row
    the pair disappears silently and the operator never knows the
    broker acknowledged it.
    """
    pair_id = getattr(pl, "id", None)
    code = getattr(pl, "code", None)
    sell_date = getattr(pl, "date", None)
    sell_price = getattr(pl, "price", None)
    pnl = getattr(pl, "pnl", None)

    if not code or not sell_date:
        return []

    cond_str = _enum_value(getattr(pl, "cond", None))
    type_label = _COND_TO_TYPE.get(cond_str, "現股")

    out: list[dict[str, Any]] = []
    leg_qty_sum = 0.0
    for leg in legs:
        leg_date = getattr(leg, "date", None)
        leg_price = getattr(leg, "price", None)
        leg_cost = getattr(leg, "cost", None)
        if not leg_date or leg_price is None or leg_cost is None:
            continue
        leg_cond = _enum_value(getattr(leg, "cond", None))
        leg_type = _COND_TO_TYPE.get(leg_cond, type_label)
        qty = _qty_from_leg(leg)
        leg_qty_sum += qty
        out.append({
            "date": str(leg_date),
            "code": str(code),
            "side": "普買",
            "qty": qty,
            "price": float(leg_price),
            "cost_twd": float(leg_cost),
            "ccy": "TWD",
            "venue": "TW",
            "type": leg_type,
            "pair_id": pair_id,
        })

    out.append({
        "date": str(sell_date),
        "code": str(code),
        "side": "普賣",
        "qty": leg_qty_sum,
        "price": float(sell_price) if sell_price is not None else 0.0,
        "ccy": "TWD",
        "venue": "TW",
        "type": type_label,
        "pair_id": pair_id,
        "pnl": float(pnl) if pnl is not None else 0.0,
    })
    return out


class ShioajiClient:
    """Lazy-login read-only wrapper.

    Construction is cheap and side-effect-free: no network call until
    `.list_trades()`/`list_open_lots()`/`list_realized_pairs()` is
    invoked. That lets the FastAPI factory instantiate one of these
    unconditionally and only pay the login cost on the rare
    sync-refresh path.
    """

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else os.environ.get("SINOPAC_API_KEY", "")
        self._secret_key = (
            secret_key if secret_key is not None
            else os.environ.get("SINOPAC_SECRET_KEY", "")
        )
        self._api: Any = None
        self._unconfigured_logged = False

    @property
    def configured(self) -> bool:
        """True only if creds AND the shioaji package are both present."""
        return bool(self._api_key and self._secret_key) and _SHIOAJI_AVAILABLE

    def lazy_login(self) -> bool:
        """Bring the session up if needed. Returns True on success.

        Idempotent. The "disabled" line is written exactly once per
        client lifetime, with a reason tag so operators can tell
        "creds missing" apart from "package missing".
        """
        if not self.configured:
            if not self._unconfigured_logged:
                if not _SHIOAJI_AVAILABLE:
                    log.info(
                        "Shioaji package not installed "
                        "(pip install 'shioaji>=1.2'); trade overlay disabled"
                    )
                elif not (self._api_key and self._secret_key):
                    log.info(
                        "Shioaji credentials not set "
                        "(SINOPAC_API_KEY / SINOPAC_SECRET_KEY); "
                        "trade overlay disabled"
                    )
                else:
                    log.info("Shioaji client not configured; trade overlay disabled")
                self._unconfigured_logged = True
            return False
        if self._api is not None:
            return True
        try:
            api = _make_session()
            api.login(api_key=self._api_key, secret_key=self._secret_key)
            self._api = api
            return True
        except Exception:
            log.exception("shioaji login failed")
            return False

    def list_trades(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """Fills within [start_date, end_date], inclusive, in project-shape:

            {date, code, side: 普買|普賣, qty, price, ccy, venue}

        Returns []: not configured / login fails / both attempts fail.
        Never raises.

        SDK note (shioaji 1.3.x): api.list_trades() takes no args and
        returns only the *current session*'s trades (typically today's).
        We still accept (start_date, end_date) so callers don't change,
        but for any date earlier than today in TPE the SDK simply has
        nothing to return. Multi-day broker history needs a persistent
        broker-deals table fed daily, which this client does not own.
        """
        return self._with_reconnect(
            lambda: self._fetch_trades(start_date, end_date),
            label="list_trades",
        )

    def list_open_lots(
        self,
        close_resolver: Callable[[str, str], float | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Currently-held TW lots as project-shape records.

        `close_resolver(code, iso_date) → float | None` injects the
        close-price lookup used to derive qty from MV. Production wires
        a DailyStore-backed lookup; tests pass a dict-stub. None or
        unresolved → that lot is skipped (warning logged).

        Returns []: not configured / login fails / both attempts fail.
        Never raises.
        """
        return self._with_reconnect(
            lambda: self._fetch_open_lots(close_resolver),
            label="list_open_lots",
        )

    def list_realized_pairs(
        self, begin_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        """Closed-pair fills (buy legs + sell summary) where the SELL
        date is in [begin_date, end_date], inclusive.

        Two-step SDK call:
          1. list_profit_loss(stock_account, begin, end) → summary rows
          2. list_profit_loss_detail(stock_account, pl.id) → BUY legs

        Each pair yields N+1 records. Per locked decision #1 (option C),
        buy legs may pre-date begin_date — the window filters SELL dates
        only, never BUY dates. This gives the audit hook full visibility
        regardless of when the buy happened.

        Returns []: not configured / login fails / both attempts fail.
        Never raises. If list_profit_loss_detail returns empty for any
        id, the sell summary still emits with qty=0 (degenerate signal).
        """
        return self._with_reconnect(
            lambda: self._fetch_realized_pairs(begin_date, end_date),
            label="list_realized_pairs",
        )

    # --- internals --------------------------------------------------------

    def _with_reconnect(
        self,
        fetch: Callable[[], list[dict[str, Any]]],
        label: str,
    ) -> list[dict[str, Any]]:
        """Reconnect-once wrapper.

        Every public surface shares the same retry shape; centralising
        it removes three near-identical try/except ladders and keeps
        the surfaces themselves declarative.
        """
        if not self.lazy_login():
            return []
        try:
            return fetch()
        except Exception as exc:
            log.warning("shioaji %s failed (%s); reconnecting once", label, exc)
            self._api = None
            if not self.lazy_login():
                return []
            try:
                return fetch()
            except Exception:
                log.exception("shioaji %s failed after reconnect", label)
                return []

    def _fetch_trades(
        self, start_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        raw = self._api.list_trades()
        out: list[dict[str, Any]] = []
        for trade in raw or []:
            out.extend(_extract_fills(trade, start_date, end_date))
        return out

    def _fetch_open_lots(
        self,
        close_resolver: Callable[[str, str], float | None] | None,
    ) -> list[dict[str, Any]]:
        raw = self._api.list_position_detail(self._api.stock_account)
        out: list[dict[str, Any]] = []
        for lot in raw or []:
            rec = _extract_lot(lot, close_resolver)
            if rec is not None:
                out.append(rec)
        return out

    def _fetch_realized_pairs(
        self, begin_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        pl_rows = self._api.list_profit_loss(
            self._api.stock_account,
            begin_date=begin_date, end_date=end_date,
        ) or []
        out: list[dict[str, Any]] = []
        for pl in pl_rows:
            pair_id = getattr(pl, "id", None)
            try:
                legs = self._api.list_profit_loss_detail(
                    self._api.stock_account, detail_id=pair_id,
                ) or []
            except Exception as exc:
                log.warning(
                    "list_profit_loss_detail failed for pair_id=%s (%s); "
                    "emitting empty-leg summary so merge layer can banner",
                    pair_id, exc,
                )
                legs = []
            out.extend(_extract_realized_pair(pl, list(legs)))
        return out
