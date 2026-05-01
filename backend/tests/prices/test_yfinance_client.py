"""Phase 2 reproducer for invest.prices.yfinance_client.

The wrapper is a thin port of app/yfinance_client.py — two operations:
  - fetch_prices(symbol, start, end) -> [{date, close, volume}]
  - fetch_fx(ccy, start, end)        -> [{date, rate}]

Tests inject a fake `yfinance` module via monkeypatch on the lazy-loaded
`_yf` attribute, so the real network is never touched and the import cost
of yfinance/pandas/numpy is paid only once across the whole suite.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from invest.prices import yfinance_client


class _FakeDataFrame:
    """Stand-in for the pandas DataFrame returned by yfinance.

    yfinance's real DataFrame indexes on Timestamp; we just need
    `iterrows()` to yield (Timestamp-like, row) tuples where
    `row["Close"]` is the value. The Timestamp-like only needs to
    answer `.strftime(...)`.
    """

    def __init__(self, rows: list[tuple]):
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
    """Replace yfinance.download with a programmable stub.

    Returns (download_calls, response_queue) so each test can pre-load
    the response and then assert what symbol/dates the wrapper sent.
    """
    download_calls: list[dict] = []
    response_queue: list[_FakeDataFrame] = []

    def fake_download(symbol, start=None, end=None, interval=None, **kwargs):
        download_calls.append(
            {"symbol": symbol, "start": start, "end": end, "interval": interval}
        )
        if not response_queue:
            return _FakeDataFrame([])
        return response_queue.pop(0)

    fake_module = SimpleNamespace(download=fake_download)
    monkeypatch.setattr(
        "invest.prices.yfinance_client._yf", fake_module
    )
    return download_calls, response_queue


# --- fetch_prices --------------------------------------------------------


class TestFetchPrices:
    def test_returns_rows_with_date_close_volume(self, fake_yf):
        calls, queue = fake_yf
        queue.append(_FakeDataFrame([
            ("2026-04-01", 150.0, 1_000_000),
            ("2026-04-02", 152.5, 1_200_000),
        ]))
        rows = yfinance_client.fetch_prices(
            "SNDK", "2026-04-01", "2026-04-30"
        )
        assert rows == [
            {"date": "2026-04-01", "close": 150.0, "volume": 1_000_000},
            {"date": "2026-04-02", "close": 152.5, "volume": 1_200_000},
        ]
        assert calls[0]["symbol"] == "SNDK"
        assert calls[0]["interval"] == "1d"

    def test_bumps_end_by_one_day_for_inclusive_window(self, fake_yf):
        """yfinance's `end` is exclusive; our API is inclusive. Confirm
        the wrapper passes end+1 day so the caller's `end` still gets
        included in the result."""
        calls, queue = fake_yf
        queue.append(_FakeDataFrame([]))
        yfinance_client.fetch_prices("SNDK", "2026-04-01", "2026-04-30")
        assert calls[0]["start"] == "2026-04-01"
        assert calls[0]["end"] == "2026-05-01"

    def test_returns_empty_on_no_data(self, fake_yf):
        """Empty DataFrame -> []. Don't blow up on delisted/unknown."""
        assert (
            yfinance_client.fetch_prices(
                "UNKNOWN", "2026-04-01", "2026-04-30"
            )
            == []
        )

    def test_skips_nan_closes(self, fake_yf):
        """yfinance occasionally emits NaN closes on non-trading rows
        that slip into the response. Drop them silently."""
        _, queue = fake_yf
        queue.append(_FakeDataFrame([
            ("2026-04-01", 150.0, 1_000),
            ("2026-04-02", float("nan"), 0),
            ("2026-04-03", 153.0, 2_000),
        ]))
        rows = yfinance_client.fetch_prices(
            "SNDK", "2026-04-01", "2026-04-30"
        )
        assert [r["date"] for r in rows] == ["2026-04-01", "2026-04-03"]


# --- fetch_fx ------------------------------------------------------------


class TestFetchFx:
    def test_usd_uses_twd_x_pair(self, fake_yf):
        """Yahoo's TWD=X means '1 USD in TWD' — the right rate for
        USD-denominated positions held in a TWD-functional account."""
        calls, queue = fake_yf
        queue.append(_FakeDataFrame([
            ("2026-04-01", 32.5, 0),
            ("2026-04-02", 32.6, 0),
        ]))
        rows = yfinance_client.fetch_fx("USD", "2026-04-01", "2026-04-30")
        assert rows == [
            {"date": "2026-04-01", "rate": 32.5},
            {"date": "2026-04-02", "rate": 32.6},
        ]
        assert calls[0]["symbol"] == "TWD=X"

    def test_hkd_uses_hkdtwd_x_pair(self, fake_yf):
        """Non-USD foreign currencies use the explicit pair form."""
        calls, queue = fake_yf
        queue.append(_FakeDataFrame([("2026-04-01", 4.18, 0)]))
        yfinance_client.fetch_fx("HKD", "2026-04-01", "2026-04-01")
        assert calls[0]["symbol"] == "HKDTWD=X"

    def test_empty_response(self, fake_yf):
        assert (
            yfinance_client.fetch_fx("USD", "2026-04-01", "2026-04-02") == []
        )

    def test_twd_returns_synthetic_unit_rate(self):
        """TWD->TWD is identity. Skip yfinance entirely (saves a request
        and avoids a stale TWD=X rate)."""
        rows = yfinance_client.fetch_fx("TWD", "2026-04-01", "2026-04-03")
        assert all(r["rate"] == 1.0 for r in rows)
        assert {r["date"] for r in rows} == {
            "2026-04-01",
            "2026-04-02",
            "2026-04-03",
        }


# --- MultiIndex column flattening (yfinance >=0.2.40) -----------------------


class _MultiIndexColumns:
    def __init__(self, levels: int):
        self.nlevels = levels


class _MultiIndexFakeDataFrame:
    """Mimics yfinance >=0.2.40's single-symbol response: columns
    become a MultiIndex like ('Close', 'LITE') instead of bare 'Close'.
    `droplevel(1, axis=1)` returns a flat-shape df."""

    def __init__(self, rows: list[tuple], levels: int = 2):
        self._rows = rows
        self._levels = levels
        self.columns = _MultiIndexColumns(levels)
        self.droplevel_calls: list[tuple[int, int]] = []

    def __len__(self) -> int:
        return len(self._rows)

    @property
    def empty(self) -> bool:
        return len(self._rows) == 0

    def droplevel(self, level: int, axis: int = 0):
        self.droplevel_calls.append((level, axis))
        flat = _MultiIndexFakeDataFrame(self._rows, levels=1)
        flat.droplevel_calls = self.droplevel_calls
        return flat

    def iterrows(self):
        for d, close, volume in self._rows:
            ts = SimpleNamespace(strftime=lambda fmt, _d=d: _d)
            row = {"Close": close, "Volume": volume}
            yield ts, row


class TestMultiIndexFlattening:
    def test_multi_level_columns_are_flattened(self, monkeypatch):
        """yfinance >=0.2.40 returns single-symbol MultiIndex columns;
        the wrapper must detect nlevels > 1 and flatten via
        droplevel(1, axis=1). Without this, downstream `row["Close"]`
        would receive a tuple."""
        multi_df = _MultiIndexFakeDataFrame(
            [
                ("2026-04-01", 150.0, 1_000),
                ("2026-04-02", 152.0, 2_000),
            ],
            levels=2,
        )

        def fake_download(symbol, **kwargs):
            return multi_df

        fake_module = SimpleNamespace(download=fake_download)
        monkeypatch.setattr(
            "invest.prices.yfinance_client._yf", fake_module
        )

        rows = yfinance_client.fetch_prices(
            "SNDK", "2026-04-01", "2026-04-02"
        )
        assert multi_df.droplevel_calls == [(1, 1)]
        assert rows[0]["close"] == 150.0

    def test_flat_columns_are_not_droplevelled(self, monkeypatch):
        """Already-flat (nlevels == 1) must not trigger droplevel —
        that would crash with 'no level to drop' on a normal index.
        Cover the no-op branch explicitly."""
        flat_df = _MultiIndexFakeDataFrame(
            [("2026-04-01", 150.0, 1_000)], levels=1
        )

        def fake_download(symbol, **kwargs):
            return flat_df

        fake_module = SimpleNamespace(download=fake_download)
        monkeypatch.setattr(
            "invest.prices.yfinance_client._yf", fake_module
        )

        rows = yfinance_client.fetch_prices(
            "SNDK", "2026-04-01", "2026-04-01"
        )
        assert flat_df.droplevel_calls == []
        assert rows[0]["close"] == 150.0
