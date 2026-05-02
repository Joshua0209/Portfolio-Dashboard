"""Read-only Shioaji client for the post-PDF trade overlay (Phase 11).

Hard invariants (verified by tests/test_shioaji_client.py):
  - This module imports ONLY the read-only Shioaji API surface. No Order
    placement, no CA activation, no cancel/update — none of it. The
    overlay's job is to read trades that have already happened, never to
    initiate any.
  - When SINOPAC_API_KEY / SINOPAC_SECRET_KEY are unset, the client is
    a trivially-stubbed no-op: list_trades() returns []. The dashboard
    is fully functional in this mode (PDF-only data).
  - On session invalidation mid-run, the client transparently logs out,
    re-logs-in once, and retries. A second consecutive failure returns
    [] rather than propagating — the data layer never crashes the UI.

If the `shioaji` package is not installed at all (e.g. dev environments
that skip the ~200MB pyzmq dependency), the module still imports cleanly
and behaves as if creds were unset.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_TPE = ZoneInfo("Asia/Taipei")

# Optional dependency — never fail the import path if unavailable. We
# narrow to ImportError/ModuleNotFoundError so a partially-installed or
# corrupted shioaji that throws a different exception during its own
# __init__ surfaces loudly instead of silently being treated as absent.
try:
    import shioaji as _shioaji  # noqa: F401  (only the module handle, no symbols)
    _SHIOAJI_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    _shioaji = None  # type: ignore[assignment]
    _SHIOAJI_AVAILABLE = False


def to_taipei_date(utc_dt: datetime) -> str:
    """Localize a UTC datetime → Asia/Taipei calendar date as ISO 'YYYY-MM-DD'.

    Why this exists: trade timestamps come back from Shioaji in UTC (or
    naive — the SDK is inconsistent across endpoints). The portfolio's
    canonical "trade date" is the TPE business date, so a UTC late-evening
    timestamp must roll into the next TPE day to match what the user sees
    in their statement.
    """
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(_TPE).date().isoformat()


# --- Session factory (patched in tests) ----------------------------------


def _make_session():
    """Construct a fresh `shioaji.Shioaji()` session.

    Indirection lets unit tests substitute a fake session class without
    touching the import line. Tests monkeypatch this symbol; production
    code never calls anything else to construct the API handle.
    """
    if not _SHIOAJI_AVAILABLE:  # pragma: no cover — covered by no-creds test path
        raise RuntimeError("shioaji module is not installed")
    return _shioaji.Shioaji()  # type: ignore[union-attr]


# --- Cond → portfolio.type mapping ---------------------------------------
#
# StockOrderCond.{Cash, MarginTrading, ShortSelling} → 現股 / 融資 / 融券.
# Mirrors the PDF parser's holdings_detail.type convention so the overlay
# can feed analytics layers (FIFO P&L, 融資 cost-asymmetry handling) without
# a translation step. Unknown conds pass through to '現股' as the safest
# default — the overlay treats unknown == cash for visualization, and the
# audit hook in trade_overlay.merge() will surface the mismatch.

_COND_TO_TYPE: dict[str, str] = {
    "Cash": "現股",
    "MarginTrading": "融資",
    "ShortSelling": "融券",
}


def _enum_value(obj: Any, default: str = "") -> str:
    """Pull `.value` off a Pydantic/enum-shaped attr, or fall back to str()."""
    if obj is None:
        return default
    val = getattr(obj, "value", None)
    if val is not None:
        return str(val)
    return str(obj) or default


# --- Side normalization --------------------------------------------------


def _normalize_side(raw: str) -> str:
    """Map Shioaji's English side strings → portfolio.json convention.

    The PDF parser writes 普買/普賣 for TW cash trades. The overlay must
    write the same strings so trade_overlay.merge() can compare records
    by structural equality without translating mid-comparison.
    """
    if not raw:
        return ""
    s = raw.lower()
    if s.startswith("buy") or s == "b":
        return "普買"
    if s.startswith("sell") or s == "s":
        return "普賣"
    return raw  # already-localized — pass through


# --- Fill extraction (handles both real SDK Trade objects and dict mocks)


def _extract_fills(
    trade: Any, start_date: str, end_date: str,
) -> list[dict[str, Any]]:
    """Translate one Trade-like object into 0+ project-shape fill records.

    Two input shapes are supported:

    1. shioaji 1.3.x Trade — has `.contract` (code, currency), `.order`
       (action enum), and `.status.deals[]` where each Deal has
       (ts: float epoch, quantity, price). One Trade with two partial
       fills emits two records.

    2. Legacy dict — {ts, code, side, qty, price, ccy}. Used by older
       unit tests that pre-date the 1.3.x signature change. Treated as
       a single fill record.

    Records outside [start_date, end_date] in TPE date are dropped.
    Malformed records (missing ts/qty/price) are skipped silently — the
    overlay is best-effort and must never crash the data layer.
    """
    if isinstance(trade, dict):
        ts = trade.get("ts")
        if not ts:
            return []
        if isinstance(ts, str):
            ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        else:
            ts_dt = ts
        tpe_date = to_taipei_date(ts_dt)
        if not (start_date <= tpe_date <= end_date):
            return []
        qty = trade.get("qty")
        price = trade.get("price")
        if qty is None or price is None:
            return []
        return [{
            "date": tpe_date,
            "code": str(trade.get("code") or ""),
            "side": _normalize_side(str(trade.get("side") or "")),
            "qty": float(qty),
            "price": float(price),
            "ccy": trade.get("ccy") or "TWD",
            "venue": "TW",
        }]

    contract = getattr(trade, "contract", None)
    order = getattr(trade, "order", None)
    status = getattr(trade, "status", None)
    if contract is None or order is None or status is None:
        return []

    code = getattr(contract, "code", None) or ""
    ccy = getattr(contract, "currency", None) or "TWD"
    action = getattr(order, "action", None)
    side_str = getattr(action, "value", None) or str(action) if action is not None else ""

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


# --- Lot extraction (StockPositionDetail → project record) ---------------


def _extract_lot(
    lot: Any,
    close_resolver: Callable[[str, str], float | None] | None,
) -> dict[str, Any] | None:
    """Translate one StockPositionDetail-shaped object → project record.

    The SDK's `quantity` field is unreliable (always 0 for 零股 — see plan
    §"Quantity derivation"). We derive qty from MV/close on the lot's
    entry date, where close comes from the injected resolver (typically a
    DailyStore-backed lookup). When the close is unavailable, we skip the
    lot entirely and let the caller log it — this is safer than emitting
    qty=None which would crash downstream cumulative-qty math.

    Returns None when the lot is unusable (missing core attrs or no close).
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


# --- Realized-pair extraction (StockProfitLoss + StockProfitDetail) ------


def _qty_from_leg(leg: Any) -> float:
    """Derive qty for one buy leg from cost/price.

    SDK quirk: leg.quantity is always 0 for 零股 (per Phase 0 probe). We
    recover it from cost (total NTD) ÷ price (per-share NTD). round() is
    used because the broker's cost field already includes fees that
    sub-cent rounding errors can introduce — but the share count is
    integer-shaped (TW shares are whole numbers, including odd-lot 零股
    which is integer < 1000).
    """
    cost = getattr(leg, "cost", None)
    price = getattr(leg, "price", None)
    if cost is None or price is None or float(price) == 0:
        return 0.0
    return float(round(float(cost) / float(price)))


def _extract_realized_pair(
    pl: Any, legs: list[Any],
) -> list[dict[str, Any]]:
    """Translate one (summary, [legs]) pair → 0+ project records.

    Emits one record per buy leg (side='普買') plus one sell summary
    (side='普賣') with qty=sum-of-leg-qtys. When `legs` is empty (the
    degenerate C-fallback case from plan §Path A), the sell summary is
    still emitted with qty=0 so the merge layer can fire a reconcile
    event.
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


# --- The client ----------------------------------------------------------


class ShioajiClient:
    """Lazy-login read-only wrapper.

    Construction is cheap and side-effect-free: no network call until
    .lazy_login() or .list_trades() is invoked. That lets the Flask
    factory instantiate one of these unconditionally and only pay the
    login cost on the rare overlay-refresh path.
    """

    def __init__(
        self,
        api_key: str | None = None,
        secret_key: str | None = None,
    ) -> None:
        self._api_key = api_key if api_key is not None else os.environ.get("SINOPAC_API_KEY", "")
        self._secret_key = (
            secret_key if secret_key is not None else os.environ.get("SINOPAC_SECRET_KEY", "")
        )
        self._api: Any = None
        self._unconfigured_logged = False

    # --- public surface ---------------------------------------------------

    @property
    def configured(self) -> bool:
        """True only if creds AND the shioaji package are both present."""
        return bool(self._api_key and self._secret_key) and _SHIOAJI_AVAILABLE

    def lazy_login(self) -> bool:
        """Bring the session up if needed. Returns True on success.

        Idempotent: a successful login leaves the session cached on the
        instance; subsequent calls are no-ops. The "disabled" line is
        written exactly once per client lifetime, with a reason tag so
        operators can tell "creds missing" apart from "package missing".
        """
        if not self.configured:
            if not self._unconfigured_logged:
                if not _SHIOAJI_AVAILABLE:
                    log.info(
                        "Shioaji package not installed (pip install 'shioaji>=1.2'); "
                        "trade overlay disabled"
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
        """Return fills within [start_date, end_date], inclusive, in the
        project-standard record shape:

            {date: 'YYYY-MM-DD', code, side: 普買|普賣, qty, price, ccy, venue}

        Returns [] if not configured, login fails, or both fetch attempts
        (initial + reconnect-once) fail. Never raises.

        SDK note (shioaji 1.3.x): the underlying api.list_trades() takes
        no arguments and returns only the *current session*'s trades —
        typically today's. We still accept (start_date, end_date) so the
        callsite contract in trade_overlay doesn't change, and we filter
        client-side, but be aware that for any date earlier than "today"
        in TPE, the SDK simply has nothing to return. Multi-day broker
        backfill needs a persistent broker-deals table fed daily, which
        this client does not own.
        """
        if not self.lazy_login():
            return []
        try:
            return self._fetch(start_date, end_date)
        except Exception as exc:
            log.warning("shioaji list_trades failed (%s); reconnecting once", exc)
            self._api = None
            if not self.lazy_login():
                return []
            try:
                return self._fetch(start_date, end_date)
            except Exception:
                log.exception("shioaji list_trades failed after reconnect")
                return []

    def list_realized_pairs(
        self, begin_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        """Return closed-pair fills (buy legs + sell summary) where the
        SELL date is in [begin_date, end_date], inclusive.

        Two-step SDK call:
          1. ``api.list_profit_loss(stock_account, begin, end)`` returns
             one summary row per closed pair.
          2. ``api.list_profit_loss_detail(stock_account, pl.id)`` returns
             the BUY-leg tranches for that pair. Per the Phase 0 probe
             (plan §"Confirmed data model"), detail rows are buy legs
             only — `trade_type` does NOT distinguish buy/sell.

        Each pair yields N+1 records (N = leg count): one '普買' record
        per buy leg with qty=round(cost/price), plus one '普賣' summary
        record with qty=sum-of-leg-qtys, price=pl.price, pnl=pl.pnl.

        IMPORTANT: per locked decision #1 (option C), buy legs may
        pre-date begin_date. The window filters SELL dates only, never
        BUY dates — this gives the merge layer's audit hook full visibility
        into broker-vs-PDF disagreements regardless of when the buy
        happened.

        Returns []: not configured / login fails / both attempts fail.
        Never raises. If ``list_profit_loss_detail(id)`` returns empty for
        any id (rate-limit / partial response — the C-fallback degenerate
        case), the sell summary still emits with qty=0 so the merge layer
        can fire the C-fallback reconcile event.
        """
        if not self.lazy_login():
            return []
        try:
            return self._fetch_realized_pairs(begin_date, end_date)
        except Exception as exc:
            log.warning(
                "shioaji list_realized_pairs failed (%s); reconnecting once",
                exc,
            )
            self._api = None
            if not self.lazy_login():
                return []
            try:
                return self._fetch_realized_pairs(begin_date, end_date)
            except Exception:
                log.exception(
                    "shioaji list_realized_pairs failed after reconnect"
                )
                return []

    def list_open_lots(
        self,
        close_resolver: Callable[[str, str], float | None] | None = None,
    ) -> list[dict[str, Any]]:
        """Return currently-held TW lots as project-shape records.

        Calls ``api.list_position_detail(stock_account)``. Each row becomes
        one record:

            {date, code, qty, cost_twd, mv_twd, type, ccy, venue}

        Where qty is derived from ``round(lot.last_price / close_resolver(
        code, date))`` — the SDK's lot.quantity is unreliable for 零股
        (see plan §"Quantity derivation").

        ``close_resolver`` is a callable ``(code, iso_date) → float | None``.
        Production callers wire in a DailyStore-backed lookup; tests pass a
        dict-stub. When None or returns None for any lot, that lot is
        skipped (logged as a warning) — the merge layer can't do anything
        useful with an unknown qty, so silent filtering is safer than
        partial data.

        Returns []: not configured / login fails / both attempts fail.
        Never raises.
        """
        if not self.lazy_login():
            return []
        try:
            return self._fetch_open_lots(close_resolver)
        except Exception as exc:
            log.warning(
                "shioaji list_open_lots failed (%s); reconnecting once", exc
            )
            self._api = None
            if not self.lazy_login():
                return []
            try:
                return self._fetch_open_lots(close_resolver)
            except Exception:
                log.exception("shioaji list_open_lots failed after reconnect")
                return []

    # --- internals --------------------------------------------------------

    def _fetch_realized_pairs(
        self, begin_date: str, end_date: str,
    ) -> list[dict[str, Any]]:
        """One round-trip pair: list_profit_loss + N × list_profit_loss_detail.

        N is bounded by the number of closed pairs in the window — typically
        single digits per refresh, so the per-id drill-down isn't a hot
        path. If a per-id detail call raises, that pair is skipped (logged)
        and the rest of the window still returns; failure of the *summary*
        call propagates so list_realized_pairs() can reconnect.
        """
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

    def _fetch_open_lots(
        self,
        close_resolver: Callable[[str, str], float | None] | None,
    ) -> list[dict[str, Any]]:
        """One API round-trip for list_position_detail. Caller catches."""
        raw = self._api.list_position_detail(self._api.stock_account)
        out: list[dict[str, Any]] = []
        for lot in raw or []:
            rec = _extract_lot(lot, close_resolver)
            if rec is not None:
                out.append(rec)
        return out

    def _fetch(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """One API round-trip. Caller catches exceptions for retry logic.

        Calls api.list_trades() (no args in 1.3.x) and walks the returned
        Trade objects' .status.deals[] for individual fill records.
        Filters by [start_date, end_date] client-side. Also handles a
        legacy dict shape for tests that pre-date the 1.3.x signature.
        """
        raw = self._api.list_trades()
        out: list[dict[str, Any]] = []
        for trade in raw or []:
            out.extend(_extract_fills(trade, start_date, end_date))
        return out
