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
from typing import Any
from zoneinfo import ZoneInfo

log = logging.getLogger(__name__)

_TPE = ZoneInfo("Asia/Taipei")

# Optional dependency — never fail the import path if unavailable.
try:
    import shioaji as _shioaji  # noqa: F401  (only the module handle, no symbols)
    _SHIOAJI_AVAILABLE = True
except Exception:  # noqa: BLE001 — any failure means "not installed", treat as off
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
        instance; subsequent calls are no-ops. The "credentials not
        configured; trade overlay disabled" line is written exactly once
        per client lifetime.
        """
        if not self.configured:
            if not self._unconfigured_logged:
                log.info("Shioaji credentials not configured; trade overlay disabled")
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
        """Return trades within [start_date, end_date], inclusive, in the
        project-standard record shape:

            {date: 'YYYY-MM-DD', code, side: 普買|普賣, qty, price, ccy, venue}

        Returns [] if not configured, login fails, or both fetch attempts
        (initial + reconnect-once) fail. Never raises.
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

    # --- internals --------------------------------------------------------

    def _fetch(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        """One API round-trip. Caller catches exceptions for retry logic.

        Translates whatever the Shioaji SDK returns into the common dict
        shape. The SDK's exact surface varies by version; the common
        contract we rely on is `api.list_trades(start_date, end_date)`
        returning records with `ts`, `code`, `side`, `qty`, `price`,
        `ccy`. Any divergence becomes a translator change here, not a
        callsite change in trade_overlay.
        """
        raw = self._api.list_trades(start_date, end_date)
        out: list[dict[str, Any]] = []
        for r in raw or []:
            ts = r.get("ts") if isinstance(r, dict) else getattr(r, "ts", None)
            code = r.get("code") if isinstance(r, dict) else getattr(r, "code", None)
            side = r.get("side") if isinstance(r, dict) else getattr(r, "side", None)
            qty = r.get("qty") if isinstance(r, dict) else getattr(r, "qty", None)
            price = r.get("price") if isinstance(r, dict) else getattr(r, "price", None)
            ccy = (r.get("ccy") if isinstance(r, dict) else getattr(r, "ccy", "TWD")) or "TWD"
            if not (ts and code and side and qty is not None and price is not None):
                continue
            # ts may be a string ISO timestamp or a datetime; normalize both
            if isinstance(ts, str):
                # Strip a trailing "Z" so fromisoformat accepts it on Py<3.11
                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            else:
                ts_dt = ts
            out.append({
                "date": to_taipei_date(ts_dt),
                "code": str(code),
                "side": _normalize_side(str(side)),
                "qty": float(qty),
                "price": float(price),
                "ccy": ccy,
                "venue": "TW",
            })
        return out
