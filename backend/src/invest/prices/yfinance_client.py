"""yfinance HTTP wrapper for foreign equities + FX rates.

Two operations:
  - fetch_prices(symbol, start, end)  -> [{date, close, volume}]
  - fetch_fx(ccy, start, end)         -> [{date, rate}]   (rate = ccy -> TWD)

Both return rows in the same shape so an upstream router can consume
them uniformly across TW (.TW / .TWO suffix) and foreign paths.
yfinance is imported lazily so the test suite doesn't pay the import
cost on every run, and the module attribute `_yf` is monkeypatchable
for unit tests.

Date contract:
  - `start` and `end` are ISO YYYY-MM-DD strings; window is inclusive.
  - yfinance's `end` parameter is exclusive, so we add one day before
    passing through.

NaN handling:
  - yfinance can emit NaN for the close on non-trading rows that
    occasionally slip into the response. We drop those rows here so
    downstream code never has to special-case None/NaN closes.
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

# Lazy import surface; tests replace this with a SimpleNamespace.
_yf: Any = None
_yf_lock = threading.Lock()


def _ensure_yf() -> Any:
    """Double-checked locking so concurrent backfill threads don't race
    the import. Tests monkeypatch `_yf` directly; production runs hit
    this lock exactly once per process."""
    global _yf
    if _yf is None:  # pragma: no cover - exercised in real runs only
        with _yf_lock:
            if _yf is None:
                import yfinance as yf

                _yf = yf
    return _yf


def _is_nan(x: Any) -> bool:
    return isinstance(x, float) and x != x


def _next_day_iso(d: str) -> str:
    return (date.fromisoformat(d) + timedelta(days=1)).isoformat()


def _iter_days(start: str, end: str) -> Iterator[str]:
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    cur = s
    while cur <= e:
        yield cur.isoformat()
        cur += timedelta(days=1)


def _download_daily(symbol: str, start: str, end: str):
    yf = _ensure_yf()
    df = yf.download(
        symbol,
        start=start,
        end=_next_day_iso(end),
        interval="1d",
        progress=False,
        auto_adjust=True,
    )
    cols = getattr(df, "columns", None)
    if cols is not None and getattr(cols, "nlevels", 1) > 1:
        df = df.droplevel(1, axis=1)
    return df


def fetch_prices(symbol: str, start: str, end: str) -> list[dict]:
    """Return [{date, close, volume}, ...] for the inclusive window."""
    df = _download_daily(symbol, start, end)
    if df is None or getattr(df, "empty", False) or len(df) == 0:
        return []
    out: list[dict] = []
    for ts, row in df.iterrows():
        close_val = row["Close"] if "Close" in row else None
        if close_val is None or _is_nan(close_val):
            continue
        try:
            close_f = float(close_val)
        except (TypeError, ValueError):
            continue
        volume_val = (
            row.get("Volume", 0) if hasattr(row, "get") else row["Volume"]
        )
        try:
            volume_i = int(volume_val) if not _is_nan(volume_val) else 0
        except (TypeError, ValueError):
            volume_i = 0
        out.append(
            {
                "date": ts.strftime("%Y-%m-%d"),
                "close": close_f,
                "volume": volume_i,
            }
        )
    return out


def fetch_fx(ccy: str, start: str, end: str) -> list[dict]:
    """Return [{date, rate}, ...] where rate is ccy -> TWD.

    TWD is the identity case (rate=1.0 for every calendar day) so we
    don't waste a yfinance request fetching it.

    Yahoo's quoting convention: `<CCY>=X` returns "1 USD in <CCY>", so
    `TWD=X` is the USD->TWD rate (units: TWD per USD), which is what
    we want for USD positions held in a TWD-functional account. For
    HKD/JPY etc. we use the explicit pair (e.g. `HKDTWD=X` =
    "1 HKD in TWD").
    """
    if ccy == "TWD":
        return [{"date": d, "rate": 1.0} for d in _iter_days(start, end)]

    pair = "TWD=X" if ccy == "USD" else f"{ccy}TWD=X"
    df = _download_daily(pair, start, end)
    if df is None or getattr(df, "empty", False) or len(df) == 0:
        return []
    out: list[dict] = []
    for ts, row in df.iterrows():
        close_val = row["Close"] if "Close" in row else None
        if close_val is None or _is_nan(close_val):
            continue
        try:
            rate_f = float(close_val)
        except (TypeError, ValueError):
            continue
        out.append({"date": ts.strftime("%Y-%m-%d"), "rate": rate_f})
    return out
