"""Phase 5 acceptance tests for app/price_sources.py.

Phase 5 adds:
  - TPEX as a TWSE fallback for TW symbols
  - symbol_market caching: once a symbol is resolved to "twse" or "tpex",
    re-runs of get_prices() must NOT probe the other exchange

Phase 2 tests (test_price_sources.py) still pass — the router stays
backwards-compatible when no DailyStore is passed.
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


# --- TPEX fallback when TWSE empty ----------------------------------------


def test_tw_falls_back_to_tpex_when_twse_empty(monkeypatch, store: DailyStore) -> None:
    """If TWSE returns no rows for the symbol, the router probes TPEX."""
    twse_calls: list[tuple] = []
    tpex_calls: list[tuple] = []

    def fake_twse(stockNo, year, month):
        twse_calls.append((stockNo, year, month))
        return []  # not on TWSE

    def fake_tpex(stockNo, year, month):
        tpex_calls.append((stockNo, year, month))
        return [{"date": f"{year}-{month:02d}-15", "close": 100.0, "volume": 1}]

    monkeypatch.setattr("app.price_sources.twse_fetch_month", fake_twse)
    monkeypatch.setattr("app.price_sources.tpex_fetch_month", fake_tpex)

    rows = get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert len(rows) == 1
    assert rows[0]["source"] == "tpex"
    assert rows[0]["symbol"] == "5483"
    # TWSE was probed once before falling back
    assert len(twse_calls) >= 1
    assert len(tpex_calls) >= 1


def test_tw_uses_twse_when_data_present(monkeypatch, store: DailyStore) -> None:
    """If TWSE returns rows, the router does NOT probe TPEX (cost-saver)."""
    tpex_calls: list[tuple] = []

    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 200.0, "volume": 9}],
    )
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda *args, **kwargs: tpex_calls.append(args) or [],
    )

    rows = get_prices("2330", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert all(r["source"] == "twse" for r in rows)
    assert tpex_calls == []  # never reached


def test_market_verdict_persisted_to_symbol_market(monkeypatch, store: DailyStore) -> None:
    """After a successful TPEX fetch, symbol_market has market='tpex'."""
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 80.0, "volume": 1}],
    )
    get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("5483",)
        ).fetchone()
        assert row is not None
        assert row["market"] == "tpex"


def test_market_verdict_persisted_for_twse(monkeypatch, store: DailyStore) -> None:
    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 200.0, "volume": 9}],
    )
    monkeypatch.setattr("app.price_sources.tpex_fetch_month", lambda *_: [])
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
    """If both TWSE and TPEX return empty, mark as 'unknown' so the next
    backfill doesn't re-probe."""
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    monkeypatch.setattr("app.price_sources.tpex_fetch_month", lambda *_: [])

    rows = get_prices("9999", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert rows == []
    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT market FROM symbol_market WHERE symbol = ?", ("9999",)
        ).fetchone()
        assert row is not None
        assert row["market"] == "unknown"


# --- Cache hit: re-run skips the probe -----------------------------------


def test_cached_symbol_skips_probe_of_other_exchange(
    monkeypatch, store: DailyStore
) -> None:
    """Once symbol is resolved to TWSE, a re-run for the same symbol must
    not probe TPEX even if TWSE returns empty (e.g., delisted month).

    This is the spec acceptance criterion: 're-running backfill_daily.py
    does not re-probe cached symbols'.
    """
    # First run: resolve as twse via populated month
    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 200.0, "volume": 9}],
    )
    tpex_calls: list[tuple] = []
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda *args: (tpex_calls.append(args), [])[1],
    )
    get_prices("2330", "TWD", "2025-06-01", "2025-06-30", store=store)
    assert tpex_calls == []

    # Second run: TWSE returns empty (e.g., a holiday-only month). TPEX
    # must still NOT be probed — the symbol is cached as twse.
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    rows = get_prices("2330", "TWD", "2025-07-01", "2025-07-31", store=store)
    assert tpex_calls == []  # still no TPEX probes
    assert rows == []


def test_cached_tpex_symbol_skips_twse_on_rerun(
    monkeypatch, store: DailyStore
) -> None:
    """The mirror case: cached as tpex → skip TWSE probe on re-run."""
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 80.0, "volume": 1}],
    )
    get_prices("5483", "TWD", "2025-06-01", "2025-06-30", store=store)

    twse_calls: list[tuple] = []
    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda *args: (twse_calls.append(args), [])[1],
    )
    rows = get_prices("5483", "TWD", "2025-07-01", "2025-07-31", store=store)
    assert twse_calls == []  # never re-probed TWSE
    assert all(r["source"] == "tpex" for r in rows)


def test_cached_unknown_symbol_skips_both_on_rerun(
    monkeypatch, store: DailyStore
) -> None:
    """Cached as 'unknown' → skip both probes (until manual re-resolution)."""
    monkeypatch.setattr("app.price_sources.twse_fetch_month", lambda *_: [])
    monkeypatch.setattr("app.price_sources.tpex_fetch_month", lambda *_: [])
    get_prices("9999", "TWD", "2025-06-01", "2025-06-30", store=store)

    twse_calls: list[tuple] = []
    tpex_calls: list[tuple] = []
    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda *args: (twse_calls.append(args), [])[1],
    )
    monkeypatch.setattr(
        "app.price_sources.tpex_fetch_month",
        lambda *args: (tpex_calls.append(args), [])[1],
    )
    rows = get_prices("9999", "TWD", "2025-07-01", "2025-07-31", store=store)
    assert twse_calls == []
    assert tpex_calls == []
    assert rows == []


# --- Backwards compatibility ---------------------------------------------


def test_phase2_call_without_store_still_works(monkeypatch) -> None:
    """get_prices() without `store=` still works — backwards-compatible."""
    monkeypatch.setattr(
        "app.price_sources.twse_fetch_month",
        lambda s, y, m: [{"date": f"{y}-{m:02d}-15", "close": 200.0, "volume": 9}],
    )
    rows = get_prices("2330", "TWD", "2025-06-01", "2025-06-30")
    assert all(r["source"] == "twse" for r in rows)
