"""symbol_market caching tests.

Once a TW symbol is resolved to a market verdict ('twse' or 'tpex'),
re-runs must NOT re-probe the other suffix. The verdict labels still
read 'twse' / 'tpex' for the market the symbol is *listed* on, even
though both fetch via yfinance now.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.daily_store import DailyStore
from app.price_sources import get_prices


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "dashboard.db")
    s.init_schema()
    return s


def _yf_factory(rows_by_suffix: dict[str, list[dict]]):
    """Build a fake yfinance_fetch_prices that responds based on the
    Yahoo suffix on the queried symbol. The default for any suffix not
    in the map is `[]` (i.e. "not on this exchange")."""
    calls: list[str] = []

    def fake_yf(symbol, start, end):
        calls.append(symbol)
        for suffix, rows in rows_by_suffix.items():
            if symbol.endswith(suffix):
                return list(rows)
        return []

    return fake_yf, calls


# --- .TWO fallback when .TW empty ----------------------------------------


def test_tw_falls_back_to_otc_when_listed_empty(
    monkeypatch, store: DailyStore
) -> None:
    """If `.TW` returns no rows, the router probes `.TWO`."""
    fake, calls = _yf_factory({
        ".TWO": [{"date": "2025-06-15", "close": 100.0, "volume": 1}],
        # .TW unlisted: implicit empty
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)

    rows = get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert len(rows) == 1
    assert rows[0]["source"] == "yfinance"
    assert rows[0]["symbol"] == "5483"
    # Both suffixes were probed in order
    assert calls == ["5483.TW", "5483.TWO"]


def test_tw_uses_listed_when_data_present(monkeypatch, store: DailyStore) -> None:
    """If `.TW` returns rows, the router does NOT probe `.TWO` (cost-saver)."""
    fake, calls = _yf_factory({
        ".TW": [{"date": "2025-06-15", "close": 200.0, "volume": 9}],
        ".TWO": [{"date": "2025-06-15", "close": 999.0, "volume": 9}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)

    rows = get_prices("2330", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert all(r["source"] == "yfinance" for r in rows)
    assert calls == ["2330.TW"]  # .TWO never probed


# --- Verdict persistence -------------------------------------------------


def test_market_verdict_persisted_for_tpex_symbol(
    monkeypatch, store: DailyStore
) -> None:
    """After a successful `.TWO` fetch, symbol_market has market='tpex'."""
    fake, _ = _yf_factory({
        ".TWO": [{"date": "2025-06-15", "close": 80.0, "volume": 1}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)
    get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("5483",)
        ).fetchone()
        assert row is not None
        assert row["market"] == "tpex"


def test_market_verdict_persisted_for_twse_symbol(
    monkeypatch, store: DailyStore
) -> None:
    fake, _ = _yf_factory({
        ".TW": [{"date": "2025-06-15", "close": 200.0, "volume": 9}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)
    get_prices("2330", "TWD", "2025-06-01", "2025-06-30", store=store)

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("2330",)
        ).fetchone()
        assert row is not None
        assert row["market"] == "twse"


def test_market_verdict_unknown_when_neither_responds(
    monkeypatch, store: DailyStore
) -> None:
    """If both suffixes return empty, mark 'unknown' so the next
    backfill doesn't re-probe."""
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", lambda *_: [])

    rows = get_prices("9999", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert rows == []
    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("9999",)
        ).fetchone()
        assert row is not None
        assert row["market"] == "unknown"


# --- Cache hit: re-run skips the other-suffix probe ----------------------


def test_cached_twse_symbol_skips_tpex_suffix_on_rerun(
    monkeypatch, store: DailyStore
) -> None:
    """Once cached as 'twse', a re-run for the same symbol must not probe
    `.TWO` even if `.TW` happens to return empty (e.g. a holiday-only
    month).
    """
    # First run: .TW returns rows → cache as 'twse'
    fake1, _ = _yf_factory({
        ".TW": [{"date": "2025-06-15", "close": 200.0, "volume": 9}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake1)
    get_prices("2330", "TWD", "2025-06-01", "2025-06-30", store=store)

    # Second run: .TW returns empty (e.g. a holiday-only month). .TWO
    # must still NOT be probed — cached as 'twse'.
    queried_second: list[str] = []

    def fake2(symbol, start, end):
        queried_second.append(symbol)
        return []  # nothing this month

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake2)
    rows = get_prices("2330", "TWD", "2025-07-01", "2025-07-31", store=store)
    # Only `.TW` was queried — the cache verdict pinned the suffix.
    assert all(s.endswith(".TW") for s in queried_second), queried_second
    assert not any(s.endswith(".TWO") for s in queried_second)
    assert rows == []


def test_cached_tpex_symbol_skips_listed_suffix_on_rerun(
    monkeypatch, store: DailyStore
) -> None:
    """Mirror case: cached as 'tpex' → never probe `.TW` again."""
    fake1, _ = _yf_factory({
        ".TWO": [{"date": "2025-06-15", "close": 80.0, "volume": 1}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake1)
    get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)

    queried_second: list[str] = []
    fake2, _ = _yf_factory({
        ".TWO": [{"date": "2025-07-15", "close": 81.0, "volume": 1}],
    })

    def wrapped(symbol, start, end):
        queried_second.append(symbol)
        return fake2(symbol, start, end)

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", wrapped)
    rows = get_prices("5483", "TWD", "2025-07-01", "2025-07-31", store=store)
    # `.TW` never re-probed
    assert not any(s.endswith(".TW") and not s.endswith(".TWO") for s in queried_second)
    assert all(r["source"] == "yfinance" for r in rows)


def test_cached_unknown_symbol_skips_both_on_rerun(
    monkeypatch, store: DailyStore
) -> None:
    """Cached as 'unknown' → skip both probes (until manual re-resolution)."""
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", lambda *_: [])
    get_prices("9999", "TWD", "2025-06-01", "2025-06-30", store=store)

    queried: list[str] = []

    def fake(symbol, start, end):
        queried.append(symbol)
        return []

    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)
    rows = get_prices("9999", "TWD", "2025-07-01", "2025-07-31", store=store)
    assert queried == []  # neither .TW nor .TWO probed
    assert rows == []


# --- Backwards compatibility ---------------------------------------------


def test_call_without_store_still_works(monkeypatch) -> None:
    """get_prices() without `store=` still works — backwards-compatible."""
    fake, _ = _yf_factory({
        ".TW": [{"date": "2025-06-15", "close": 200.0, "volume": 9}],
    })
    monkeypatch.setattr("app.price_sources.yfinance_fetch_prices", fake)
    rows = get_prices("2330", "TWD", "2025-06-01", "2025-06-30")
    assert all(r["source"] == "yfinance" for r in rows)
    assert all(r["symbol"] == "2330" for r in rows)
