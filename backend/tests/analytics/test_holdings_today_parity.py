"""Parity tests: legacy app.holdings_today ≡ invest.analytics.holdings_today.

Same transitional pattern as test_monthly_parity.py — locks the port
faithful, deletes itself in Phase 9 alongside app/.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


@pytest.fixture(scope="module")
def legacy():
    from app import holdings_today as legacy_module
    return legacy_module


@pytest.fixture(scope="module")
def new():
    from invest.analytics import holdings_today as new_module
    return new_module


# Synthetic month — exercises both TW and Foreign holdings, the FX
# rate path, the per-symbol metadata index, and the PDF cold-path
# fallback inside current_holdings().
@pytest.fixture
def month() -> dict:
    return {
        "month": "2024-12",
        "fx_usd_twd": 31.5,
        "tw": {
            "holdings": [
                {"code": "2330", "name": "TSMC", "type": "現股",
                 "qty": 1000, "avg_cost": 600, "cost": 600_000,
                 "ref_price": 650, "mkt_value": 650_000,
                 "unrealized_pnl": 50_000},
                {"code": "0050", "name": "ETF", "type": "融資",
                 "qty": 500, "avg_cost": 130, "cost": 65_000,
                 "ref_price": 140, "mkt_value": 70_000,
                 "unrealized_pnl": 5_000},
            ]
        },
        "foreign": {
            "holdings": [
                {"code": "AAPL", "name": "Apple", "market": "USA",
                 "ccy": "USD", "qty": 100, "cost": 15_000,
                 "close": 180, "mkt_value": 18_000,
                 "unrealized_pnl": 3_000},
            ]
        },
    }


# --- Stub daily_store -----------------------------------------------


class _Store:
    """Minimal stand-in for app.data_store.DataStore. Only `.months`
    is read by current_holdings()."""
    def __init__(self, months: list[dict]) -> None:
        self.months = months


class _DailyStore:
    """Minimal stand-in for app.daily_store.DailyStore.

    Configurable per-test: pass `snapshot=None` to exercise the cold
    path; `snapshot={...}` + `positions=[...]` to exercise the warm
    path.
    """

    def __init__(
        self,
        snapshot: dict | None = None,
        positions: list[dict] | None = None,
        closes: dict[str, dict] | None = None,
    ) -> None:
        self._snapshot = snapshot
        self._positions = positions or []
        self._closes = closes or {}

    def get_today_snapshot(self) -> dict | None:
        return self._snapshot

    def get_positions_snapshot(self, _date: str) -> list[dict]:
        return self._positions

    def get_latest_closes(self, _codes: list[str]) -> dict[str, dict]:
        return self._closes

    def get_latest_close(self, code: str) -> dict | None:
        return self._closes.get(code)


# --- Tests ----------------------------------------------------------


class TestHoldingsForMonth:
    def test_matches(self, legacy, new, month) -> None:
        assert legacy.holdings_for_month(month) == new.holdings_for_month(month)


class TestColdPath:
    """No daily snapshot → PDF month-end + per-symbol reprice fallback."""

    def test_no_snapshot_returns_pdf_rows_unchanged(self, legacy, new, month) -> None:
        store = _Store([month])
        ds = _DailyStore(snapshot=None)
        assert legacy.current_holdings(store, ds) == new.current_holdings(store, ds)

    def test_with_snapshot_no_positions_uses_pdf_reprice(self, legacy, new, month) -> None:
        # Snapshot present but positions table empty — reprice each
        # PDF row with today's close.
        store = _Store([month])
        ds = _DailyStore(
            snapshot={"date": "2026-05-02", "fx_usd_twd": 31.8},
            positions=[],
            closes={
                "2330": {"date": "2026-05-02", "close": 700.0, "currency": "TWD"},
                "AAPL": {"date": "2026-05-02", "close": 200.0, "currency": "USD"},
            },
        )
        # The "warm path" condition is `if snapshot:` — when snapshot
        # exists but positions is empty, current_holdings returns []
        # (positions are authoritative). Both legacy and new agree.
        assert legacy.current_holdings(store, ds) == new.current_holdings(store, ds)


class TestWarmPath:
    """Daily snapshot + positions → return positions enriched with PDF metadata."""

    def test_matches(self, legacy, new, month) -> None:
        store = _Store([month])
        ds = _DailyStore(
            snapshot={"date": "2026-05-02", "fx_usd_twd": 31.8},
            positions=[
                {"symbol": "2330", "qty": 1000, "cost_local": 600_000,
                 "mv_local": 700_000, "mv_twd": 700_000,
                 "type": "現股", "source": "pdf"},
                {"symbol": "AAPL", "qty": 100, "cost_local": 15_000,
                 "mv_local": 20_000, "mv_twd": 636_000,
                 "type": "USA", "source": "pdf"},
            ],
        )
        assert legacy.current_holdings(store, ds) == new.current_holdings(store, ds)

    def test_overlay_only_ticker_uses_default_meta(self, legacy, new, month) -> None:
        # positions_daily has a row for a ticker that's NOT in the
        # latest PDF. Falls back to _default_meta (best-effort).
        store = _Store([month])
        ds = _DailyStore(
            snapshot={"date": "2026-05-02", "fx_usd_twd": 31.8},
            positions=[
                {"symbol": "2330", "qty": 1000, "cost_local": 600_000,
                 "mv_local": 700_000, "mv_twd": 700_000,
                 "type": "現股", "source": "pdf"},
                {"symbol": "1234", "qty": 500, "cost_local": 50_000,
                 "mv_local": 55_000, "mv_twd": 55_000,
                 "type": "現股", "source": "overlay"},
            ],
        )
        assert legacy.current_holdings(store, ds) == new.current_holdings(store, ds)


class TestEmpty:
    def test_no_months(self, legacy, new) -> None:
        store = _Store([])
        ds = _DailyStore(snapshot={"date": "2026-05-02"}, positions=[])
        assert legacy.current_holdings(store, ds) == new.current_holdings(store, ds) == []


class TestHelpers:
    def test_normalize_tw(self, legacy, new) -> None:
        h = {"code": "2330", "name": "TSMC", "type": "現股",
             "qty": 1000, "avg_cost": 600, "cost": 600_000,
             "ref_price": 650, "mkt_value": 650_000,
             "unrealized_pnl": 50_000}
        assert legacy._normalize_tw(h) == new._normalize_tw(h)

    def test_normalize_foreign(self, legacy, new) -> None:
        h = {"code": "AAPL", "name": "Apple", "market": "USA",
             "ccy": "USD", "qty": 100, "cost": 15_000,
             "close": 180, "mkt_value": 18_000, "unrealized_pnl": 3_000}
        assert legacy._normalize_foreign(h, 31.5) == new._normalize_foreign(h, 31.5)

    def test_build_pdf_metadata(self, legacy, new, month) -> None:
        assert legacy._build_pdf_metadata(month) == new._build_pdf_metadata(month)

    def test_default_meta_tw(self, legacy, new) -> None:
        assert legacy._default_meta("2330", "現股") == new._default_meta("2330", "現股")

    def test_default_meta_foreign(self, legacy, new) -> None:
        assert legacy._default_meta("AAPL", None) == new._default_meta("AAPL", None)

    def test_to_api_row(self, legacy, new) -> None:
        position = {"symbol": "2330", "qty": 1000, "cost_local": 600_000,
                    "mv_local": 700_000, "mv_twd": 700_000,
                    "type": "現股", "source": "pdf"}
        pdf_meta = {"2330": {"venue": "TW", "ccy": "TWD", "name": "TSMC",
                              "avg_cost": 600, "type": "現股"}}
        as_of = "2026-05-02"
        assert legacy._to_api_row(position, pdf_meta, as_of) == \
               new._to_api_row(position, pdf_meta, as_of)
