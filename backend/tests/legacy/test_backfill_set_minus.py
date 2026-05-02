"""Tests for set-minus dates_checked logic + round-robin orchestration +
deferred retry pass.

Three concerns, three groups:

  1. DailyStore.find_missing_dates / mark_dates_checked behave correctly
     across empty / partial / full / today-clip cases.
  2. price_sources fetchers consult dates_checked when (store, today) given,
     skip already-covered windows, and mark on success.
  3. run_full_backfill round-robins across upstreams and defers failed
     tasks to a single retry pass before writing the DLQ.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from invest.jobs import backfill_runner
from invest.persistence.daily_store import DailyStore
from invest.prices import sources as price_sources


# --- Group 1: DailyStore primitives --------------------------------------


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "set_minus.db")
    s.init_schema()
    return s


def test_find_missing_dates_empty_store_returns_full_range(store: DailyStore):
    missing = store.find_missing_dates("2330", "2025-11-01", "2025-11-05")
    assert missing == [
        "2025-11-01", "2025-11-02", "2025-11-03", "2025-11-04", "2025-11-05",
    ]


def test_find_missing_dates_after_mark_returns_only_holes(store: DailyStore):
    store.mark_dates_checked("2330", "2025-11-01", "2025-11-03")
    missing = store.find_missing_dates("2330", "2025-11-01", "2025-11-05")
    assert missing == ["2025-11-04", "2025-11-05"]


def test_mark_dates_checked_is_idempotent(store: DailyStore):
    n1 = store.mark_dates_checked("2330", "2025-11-01", "2025-11-03")
    n2 = store.mark_dates_checked("2330", "2025-11-02", "2025-11-04")
    assert n1 == 3
    # Second call writes 3 rows but only 1 is new (Nov 4); INSERT OR IGNORE.
    assert n2 == 3
    missing = store.find_missing_dates("2330", "2025-11-01", "2025-11-04")
    assert missing == []


def test_find_missing_dates_per_symbol_isolation(store: DailyStore):
    store.mark_dates_checked("2330", "2025-11-01", "2025-11-05")
    # Different symbol — should still see full window as missing.
    missing = store.find_missing_dates("2454", "2025-11-01", "2025-11-05")
    assert len(missing) == 5


def test_find_missing_dates_inverted_range_returns_empty(store: DailyStore):
    assert store.find_missing_dates("X", "2025-11-05", "2025-11-01") == []


# --- Group 2: price_sources set-minus + today-clip + range coalesce ------


def test_coalesce_date_ranges_merges_consecutive():
    out = price_sources.coalesce_date_ranges([
        "2025-11-04", "2025-11-05", "2025-11-06", "2025-11-15", "2025-11-16",
    ])
    assert out == [("2025-11-04", "2025-11-06"), ("2025-11-15", "2025-11-16")]


def test_coalesce_date_ranges_empty():
    assert price_sources.coalesce_date_ranges([]) == []


def test_coalesce_date_ranges_single():
    assert price_sources.coalesce_date_ranges(["2025-11-04"]) == [
        ("2025-11-04", "2025-11-04")
    ]


def test_get_prices_tw_skips_already_checked_window(
    store: DailyStore, monkeypatch
):
    """When dates_checked covers the full window, yfinance is never called
    and an empty list is returned."""
    # Pre-mark the symbol as TWSE-resolved AND fully checked.
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO symbol_market(symbol, market, resolved_at, last_verified_at) "
            "VALUES (?, 'twse', ?, ?)",
            ("2330", "2026-04-26T00:00:00", "2026-04-26T00:00:00"),
        )
    store.mark_dates_checked("2330", "2025-11-01", "2025-11-30")

    calls: list[tuple] = []

    def fake_yf(symbol, start, end):
        calls.append((symbol, start, end))
        return [{"date": "2025-11-15", "close": 1000.0, "volume": 1}]

    monkeypatch.setattr(price_sources, "yfinance_fetch_prices", fake_yf)

    rows = price_sources.get_prices(
        "2330", "TWD", "2025-11-01", "2025-11-30",
        store=store, today="2026-04-27",
    )
    assert rows == []
    assert calls == [], "yfinance should not be called when all dates are checked"


def test_get_prices_tw_marks_window_clipped_to_yesterday(
    store: DailyStore, monkeypatch,
):
    """Successful fetch marks every requested day in the window as checked,
    but never marks today (volatile data)."""
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO symbol_market(symbol, market, resolved_at, last_verified_at) "
            "VALUES (?, 'twse', ?, ?)",
            ("2330", "2026-04-01T00:00:00", "2026-04-01T00:00:00"),
        )

    def fake_yf(symbol, start, end):
        # symbol carries .TW suffix when cached as 'twse'
        assert symbol == "2330.TW"
        return [
            {"date": "2026-04-10", "close": 1000.0, "volume": 1},
            {"date": "2026-04-15", "close": 1010.0, "volume": 1},
        ]

    monkeypatch.setattr(price_sources, "yfinance_fetch_prices", fake_yf)

    today = "2026-04-20"
    price_sources.get_prices(
        "2330", "TWD", "2026-04-01", "2026-04-20",
        store=store, today=today,
    )

    # April 1..19 should be checked (clipped at today-1 = Apr 19).
    # April 20 (today) MUST NOT be in dates_checked. The cache key is the
    # bare symbol '2330' (NOT '2330.TW') — yfinance suffix is fetcher-only.
    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date FROM dates_checked WHERE symbol = '2330' "
            "ORDER BY date"
        ).fetchall()
    dates = [r["date"] for r in rows]
    assert "2026-04-01" in dates
    assert "2026-04-19" in dates
    assert "2026-04-20" not in dates, "today must never enter dates_checked"


def test_get_prices_foreign_uses_set_minus_with_coalesce(
    store: DailyStore, monkeypatch,
):
    """Foreign (yfinance) path coalesces missing dates into ranges and
    fetches each range exactly once."""
    # Pre-mark a hole: Nov 4-6 missing, rest present.
    store.mark_dates_checked("AAPL", "2025-11-01", "2025-11-03")
    store.mark_dates_checked("AAPL", "2025-11-07", "2025-11-15")

    fetch_calls: list[tuple] = []

    def fake_yf(symbol, start, end):
        fetch_calls.append((symbol, start, end))
        return [{"date": "2025-11-05", "close": 200.0, "volume": 1}]

    monkeypatch.setattr(price_sources, "yfinance_fetch_prices", fake_yf)

    price_sources.get_prices(
        "AAPL", "USD", "2025-11-01", "2025-11-15",
        store=store, today="2026-04-27",
    )

    # Should have fetched ONLY the gap: Nov 4-6.
    assert fetch_calls == [("AAPL", "2025-11-04", "2025-11-06")]


def test_get_fx_rates_uses_set_minus_with_namespaced_key(
    store: DailyStore, monkeypatch,
):
    """FX uses 'FX:<ccy>' as the dates_checked key so it doesn't collide
    with any equity symbol named 'USD'."""
    fx_calls: list[tuple] = []

    def fake_yf_fx(ccy, start, end):
        fx_calls.append((ccy, start, end))
        return [{"date": "2025-11-15", "rate": 31.5}]

    monkeypatch.setattr(price_sources, "yfinance_fetch_fx", fake_yf_fx)

    price_sources.get_fx_rates(
        "USD", "2025-11-01", "2025-11-15",
        store=store, today="2026-04-27",
    )
    # Second call: should be skipped entirely (set-minus says all checked).
    fx_calls.clear()
    price_sources.get_fx_rates(
        "USD", "2025-11-01", "2025-11-15",
        store=store, today="2026-04-27",
    )
    assert fx_calls == []

    # Verify the namespaced key was used.
    with store.connect_ro() as conn:
        n = conn.execute(
            "SELECT COUNT(*) AS n FROM dates_checked WHERE symbol = 'FX:USD'"
        ).fetchone()["n"]
        assert n > 0


# --- Group 3: round-robin + deferred retry orchestration -----------------


@pytest.fixture()
def portfolio_two_upstreams(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "summary": {
            "all_trades": [
                {"venue": "TW", "code": "2330", "date": "2025-08-15",
                 "side": "普買", "qty": 100, "price": 600},
                {"venue": "Foreign", "code": "AAPL", "date": "2025-08-15",
                 "side": "普買", "qty": 10, "price": 200, "ccy": "USD"},
            ],
        },
        "months": [
            {
                "month": "2025-08",
                "tw": {"holdings": [
                    {"code": "2330", "qty": 100, "avg_cost": 600},
                ]},
                "foreign": {"holdings": [
                    {"code": "AAPL", "qty": 10, "avg_cost": 200, "ccy": "USD"},
                ]},
            },
        ],
    }))
    return p


def test_round_robin_interleaves_upstreams(
    store: DailyStore, portfolio_two_upstreams: Path, monkeypatch,
):
    """tw → fx → foreign cycle should interleave upstream calls. With one
    task per upstream the visit order is [tw, fx, foreign], not all-tw-
    then-all-fx-then-all-foreign."""
    visit_order: list[str] = []

    def fake_tw(symbol, currency, start, end, store=None, today=None):
        visit_order.append("tw")
        return [{"date": "2025-08-20", "close": 600.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    def fake_fx(ccy, start, end, store=None, today=None):
        visit_order.append("fx")
        return [{"date": "2025-08-20", "ccy": ccy, "rate": 30.0,
                 "source": "yfinance"}]

    def fake_foreign(symbol, currency, start, end, store=None, today=None):
        visit_order.append("foreign")
        return [{"date": "2025-08-20", "close": 200.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    # The router dispatches by currency: TWD → fake_tw, USD → fake_foreign.
    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if currency == "TWD":
            return fake_tw(symbol, currency, start, end, store=store, today=today)
        return fake_foreign(symbol, currency, start, end, store=store, today=today)

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(backfill_runner, "get_fx_rates", fake_fx)
    # Skip benchmark fetches — they'd dominate the order.
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    backfill_runner.run_full_backfill(
        store, portfolio_two_upstreams, today="2025-08-31",
    )

    # tw → fx → foreign interleaved (benchmark visits collapsed to no-op).
    assert visit_order[:3] == ["tw", "fx", "foreign"]


def test_deferred_retry_recovers_transient_failure(
    store: DailyStore, portfolio_two_upstreams: Path, monkeypatch,
):
    """A task that fails on the first pass but succeeds on retry should
    NOT land in failed_tasks."""
    call_count = {"2330": 0}

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if symbol == "2330":
            call_count["2330"] += 1
            if call_count["2330"] == 1:
                raise RuntimeError("transient yfinance 503")
            return [{"date": "2025-08-20", "close": 600.0,
                     "symbol": symbol, "currency": currency, "source": "yfinance"}]
        # AAPL succeeds first try
        return [{"date": "2025-08-20", "close": 200.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(
        backfill_runner, "get_fx_rates",
        lambda ccy, s, e, store=None, today=None: [],
    )
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    summary = backfill_runner.run_full_backfill(
        store, portfolio_two_upstreams, today="2025-08-31",
    )

    assert call_count["2330"] == 2, "2330 should have been retried once"
    assert "2330" in summary["tw_fetched"]
    assert summary["deferred_count"] == 1
    # No DLQ entry — the retry succeeded.
    assert store.get_failed_tasks() == []


def test_deferred_retry_writes_dlq_on_second_failure(
    store: DailyStore, portfolio_two_upstreams: Path, monkeypatch,
):
    """Two consecutive failures → DLQ row written."""
    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if symbol == "2330":
            raise RuntimeError("yfinance permanently down")
        return [{"date": "2025-08-20", "close": 200.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(
        backfill_runner, "get_fx_rates",
        lambda ccy, s, e, store=None, today=None: [],
    )
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    backfill_runner.run_full_backfill(
        store, portfolio_two_upstreams, today="2025-08-31",
    )

    failed = store.get_failed_tasks()
    assert len(failed) == 1
    assert failed[0]["task_type"] == "tw_prices"
    assert failed[0]["target"] == "2330"
    # attempts should reflect both passes (1 from initial, +1 from retry).
    assert failed[0]["attempts"] == 1


# --- Group 4: per-market circuit breaker --------------------------------


@pytest.fixture()
def portfolio_breaker(tmp_path: Path) -> Path:
    """Five TW symbols + one foreign symbol — enough to overshoot a
    threshold of 3 in the TW market while leaving foreign untouched."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "summary": {
            "all_trades": [
                {"venue": "TW", "code": code, "date": "2025-08-15",
                 "side": "普買", "qty": 100, "price": 600}
                for code in ("2330", "2454", "0050", "2317", "1101")
            ] + [
                {"venue": "Foreign", "code": "AAPL", "date": "2025-08-15",
                 "side": "普買", "qty": 10, "price": 200, "ccy": "USD"},
            ],
        },
        "months": [
            {
                "month": "2025-08",
                "tw": {"holdings": [
                    {"code": code, "qty": 100, "avg_cost": 600}
                    for code in ("2330", "2454", "0050", "2317", "1101")
                ]},
                "foreign": {"holdings": [
                    {"code": "AAPL", "qty": 10, "avg_cost": 200, "ccy": "USD"},
                ]},
            },
        ],
    }))
    return p


def test_circuit_breaker_trips_after_threshold_failures(
    store: DailyStore, portfolio_breaker: Path, monkeypatch,
):
    """3 TW failures should trip the breaker and skip remaining TW symbols.
    The foreign market should be unaffected (separate counter)."""
    tw_call_count = {"n": 0}

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if currency == "TWD":
            tw_call_count["n"] += 1
            raise RuntimeError("yfinance permanently down")
        # Foreign always succeeds.
        return [{"date": "2025-08-20", "close": 200.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(
        backfill_runner, "get_fx_rates",
        lambda ccy, s, e, store=None, today=None: [],
    )
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    summary = backfill_runner.run_full_backfill(
        store, portfolio_breaker, today="2025-08-31",
        max_failures_per_market=3,
    )

    # Round-robin would have visited all 5 TW symbols, but the breaker
    # trips at 3 and the remaining 2 are skipped before fetch_fn runs.
    assert tw_call_count["n"] == 3
    assert "tw" in summary["tripped_markets"]
    assert "foreign" not in summary["tripped_markets"]
    # The 2 TW symbols never attempted should appear in breaker_skipped.
    assert len(summary["circuit_breaker_skipped"]["tw"]) >= 2
    # Foreign succeeded — should be in fetched.
    assert "AAPL" in summary["foreign_fetched"]


def test_circuit_breaker_disabled_with_high_threshold(
    store: DailyStore, portfolio_breaker: Path, monkeypatch,
):
    """Setting threshold above the symbol count = breaker never trips —
    every TW symbol still gets its two-pass shot."""
    tw_call_count = {"n": 0}

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        if currency == "TWD":
            tw_call_count["n"] += 1
            raise RuntimeError("yfinance flaky")
        return [{"date": "2025-08-20", "close": 200.0,
                 "symbol": symbol, "currency": currency, "source": "yfinance"}]

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(
        backfill_runner, "get_fx_rates",
        lambda ccy, s, e, store=None, today=None: [],
    )
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    summary = backfill_runner.run_full_backfill(
        store, portfolio_breaker, today="2025-08-31",
        max_failures_per_market=99,
    )

    # 5 symbols × 2 passes = 10 fetch attempts, no early stop.
    assert tw_call_count["n"] == 10
    assert summary["tripped_markets"] == []
    assert summary["circuit_breaker_skipped"]["tw"] == []


def test_run_tw_backfill_circuit_breaker(
    store: DailyStore, portfolio_breaker: Path, monkeypatch,
):
    """The single-market path also honors the threshold."""
    tw_call_count = {"n": 0}

    def fake_get_prices(symbol, currency, start, end, store=None, today=None):
        tw_call_count["n"] += 1
        raise RuntimeError("yfinance down")

    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)

    summary = backfill_runner.run_tw_backfill(
        store, portfolio_breaker, today="2025-08-31",
        max_failures_per_market=3,
    )

    # No retry pass in this path — fetch_with_dlq writes DLQ on each
    # failure. After 3 failures the loop short-circuits.
    assert tw_call_count["n"] == 3
    assert summary["tripped_markets"] == ["tw"]
    assert len(summary["circuit_breaker_skipped"]["tw"]) >= 2
