"""yfinance HTTP wrapper for foreign equities + FX rates.

Two operations:
  - fetch_prices(symbol, start, end)  → [{date, close, volume}]
  - fetch_fx(ccy, start, end)         → [{date, rate}]   (rate = ccy → TWD)

Both return rows in the same shape as twse_client / tpex_client so the
router can treat all three uniformly. yfinance is imported lazily so the
test suite doesn't pay the import cost on every run, and the module
attribute `_yf` is monkeypatchable for unit tests.

Date contract:
  - `start` and `end` are ISO YYYY-MM-DD strings; window is inclusive.
  - yfinance's `end` parameter is exclusive, so we add one day before
    passing through.

NaN handling:
  - yfinance can emit NaN for the close on non-trading rows that occasionally
    slip into the response. We drop those rows here so downstream code never
    has to special-case None/NaN closes.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

log = logging.getLogger(__name__)

# Lazy import surface; tests replace this with a SimpleNamespace.
_yf: Any = None


def _ensure_yf() -> Any:
    global _yf
    if _yf is None:  # pragma: no cover — exercised in real runs only
        import yfinance as yf
        _yf = yf
    return _yf


def _is_nan(x: Any) -> bool:
    return isinstance(x, float) and x != x


def _next_day_iso(d: str) -> str:
    return (date.fromisoformat(d) + timedelta(days=1)).isoformat()


def _iter_days(start: str, end: str):
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    cur = s
    while cur <= e:
        yield cur.isoformat()
        cur += timedelta(days=1)


def _download_daily(symbol: str, start: str, end: str):
    yf = _ensure_yf()
    # `end` is exclusive in yfinance, so bump by one day to make the
    # window inclusive on both ends.
    return yf.download(
        symbol,
        start=start,
        end=_next_day_iso(end),
        interval="1d",
        progress=False,
        auto_adjust=True,
    )


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
        volume_val = row.get("Volume", 0) if hasattr(row, "get") else row["Volume"]
        try:
            volume_i = int(volume_val) if not _is_nan(volume_val) else 0
        except (TypeError, ValueError):
            volume_i = 0
        out.append({
            "date": ts.strftime("%Y-%m-%d"),
            "close": close_f,
            "volume": volume_i,
        })
    return out


def fetch_fx(ccy: str, start: str, end: str) -> list[dict]:
    """Return [{date, rate}, ...] where rate is ccy → TWD.

    TWD is the identity case (rate=1.0 for every calendar day) so we
    don't waste a yfinance request fetching it. For all other currencies
    we ask Yahoo for `<CCY>=X` (which is the inverse direction — USD per
    TWD or similar — wait, Yahoo's `TWD=X` is actually USD-per-TWD…).

    Actually Yahoo's quoting is: `<CCY>=X` is "1 USD in <CCY>". So
    `TWD=X` is the USD→TWD rate, which is exactly what we want for
    USD positions held in a TWD-functional account. For HKD/JPY we'd
    use `HKDTWD=X` (e.g., "1 HKD in TWD") if we ever wire those in.
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
