"""Phase 6 acceptance tests for app/yfinance_client.py.

The wrapper exposes two operations:
  - fetch_prices(symbol, start, end) → [{date, close, volume}]
  - fetch_fx(ccy, start, end)        → [{date, rate}]

Both are pure I/O wrappers around yfinance — the router consumes them
generically. Tests inject a fake `yfinance` module via monkeypatch so the
real network is never touched.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app import yfinance_client


class _FakeDataFrame:
    """A minimal stand-in for the pandas DataFrame returned by yfinance.

    yfinance's real DataFrame indexes on Timestamp. We just need .iterrows
    to yield (Timestamp-like, row) tuples where row['Close'] is the value.
    """

    def __init__(self, rows: list[tuple]):
        # rows is [(date_iso, close, volume), ...]
        self._rows = rows

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def empty(self) -> bool:
        return len(self._rows) == 0

    def iterrows(self):
        for d, close, volume in self._rows:
            ts = SimpleNamespace(strftime=lambda fmt, _d=d: _d)
            row = {"Close": close, "Volume": volume}
            yield ts, row


@pytest.fixture()
def fake_yf(monkeypatch):
    """Replace yfinance.download with a programmable stub."""
    download_calls: list[dict] = []
    response_queue: list[_FakeDataFrame] = []

    def fake_download(symbol, start=None, end=None, interval=None, **kwargs):
        download_calls.append({
            "symbol": symbol, "start": start, "end": end, "interval": interval
        })
        if not response_queue:
            return _FakeDataFrame([])
        return response_queue.pop(0)

    fake_module = SimpleNamespace(download=fake_download)
    monkeypatch.setattr("app.yfinance_client._yf", fake_module)
    return download_calls, response_queue


# --- fetch_prices --------------------------------------------------------


def test_fetch_prices_calls_yfinance_with_daily_interval(fake_yf) -> None:
    calls, queue = fake_yf
    queue.append(_FakeDataFrame([
        ("2026-04-01", 150.0, 1_000_000),
        ("2026-04-02", 152.5, 1_200_000),
    ]))
    rows = yfinance_client.fetch_prices("SNDK", "2026-04-01", "2026-04-30")
    assert len(rows) == 2
    assert rows[0] == {"date": "2026-04-01", "close": 150.0, "volume": 1_000_000}
    # yfinance's `end` is exclusive — bump to next day for inclusivity
    assert calls[0]["interval"] == "1d"
    assert calls[0]["symbol"] == "SNDK"


def test_fetch_prices_returns_empty_on_no_data(fake_yf) -> None:
    """Empty DataFrame → []. Don't blow up on delisted/unknown tickers."""
    _, _ = fake_yf  # no responses queued; default returns empty
    assert yfinance_client.fetch_prices("UNKNOWN", "2026-04-01", "2026-04-30") == []


def test_fetch_prices_skips_nan_closes(fake_yf) -> None:
    """yfinance occasionally returns NaN for non-trading days that slip
    into the response. Drop those rows."""
    _, queue = fake_yf
    queue.append(_FakeDataFrame([
        ("2026-04-01", 150.0, 1_000),
        ("2026-04-02", float("nan"), 0),
        ("2026-04-03", 153.0, 2_000),
    ]))
    rows = yfinance_client.fetch_prices("SNDK", "2026-04-01", "2026-04-30")
    assert [r["date"] for r in rows] == ["2026-04-01", "2026-04-03"]


# --- fetch_fx ------------------------------------------------------------


def test_fetch_fx_uses_correct_yahoo_pair(fake_yf) -> None:
    """USD → TWD uses Yahoo's TWD=X ticker; the `=X` is the FX-pair suffix."""
    calls, queue = fake_yf
    queue.append(_FakeDataFrame([
        ("2026-04-01", 32.5, 0),
        ("2026-04-02", 32.6, 0),
    ]))
    rows = yfinance_client.fetch_fx("USD", "2026-04-01", "2026-04-30")
    assert len(rows) == 2
    assert rows[0] == {"date": "2026-04-01", "rate": 32.5}
    assert calls[0]["symbol"] == "TWD=X"


def test_fetch_fx_handles_empty_response(fake_yf) -> None:
    assert yfinance_client.fetch_fx("USD", "2026-04-01", "2026-04-02") == []


def test_fetch_fx_twd_returns_unit_rate() -> None:
    """TWD→TWD is identity — return a single synthetic row per day rather
    than calling yfinance (saves a request and avoids a stale TWD=X rate).
    """
    rows = yfinance_client.fetch_fx("TWD", "2026-04-01", "2026-04-03")
    # Identity rate of 1.0; one row per calendar day in window
    assert all(r["rate"] == 1.0 for r in rows)
    assert {r["date"] for r in rows} == {"2026-04-01", "2026-04-02", "2026-04-03"}
