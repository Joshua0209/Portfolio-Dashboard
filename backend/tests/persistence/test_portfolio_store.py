"""Tests for PortfolioStore — the JSON-backed monthly aggregate bridge.

Phase 6.5 prereq: legacy app/data_store.py:DataStore is the source of
truth for monthly aggregates today. The new backend will read the
same data/portfolio.json until the Shioaji-canonical Trade-table
aggregator lands (Phase 10+). Until then, PortfolioStore is the
in-process facade: same property surface, same mtime-driven reload,
parallel test coverage so the legacy can be deleted in Phase 9.

Why mtime reload matters: scripts/parse_statements.py overwrites
data/portfolio.json while the FastAPI process is running. Without
the reload-on-mtime-change check, the running app keeps serving
stale data until restart. The legacy implementation (89 LOC) was
explicit about this — same invariant ports here verbatim.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from invest.persistence.portfolio_store import PortfolioStore


# Shape of the real data/portfolio.json (pinned 2026-05-02). Tests
# build a fixture that mirrors this so the assertions match what
# downstream routers will actually see.
_FIXTURE = {
    "months": [
        {
            "month": "2024-01",
            "equity_twd": 1_000_000.0,
            "tw_market_value_twd": 600_000.0,
            "foreign_market_value_twd": 300_000.0,
            "bank_twd": 100_000.0,
            "bank_usd_in_twd": 0.0,
            "external_flow_twd": 1_000_000.0,
            "investment_flows_twd": 0.0,
            "fx_usd_twd": 31.5,
            "period_return": 0.0,
            "cum_twr": 0.0,
            "v_start": 0.0,
            "tw": {"holdings": [], "trades": [], "rebates": []},
            "foreign": {"holdings": [], "trades": []},
            "bank": {"flows": []},
            "dividend_events": [],
        },
        {
            "month": "2024-02",
            "equity_twd": 1_050_000.0,
            "tw_market_value_twd": 630_000.0,
            "foreign_market_value_twd": 315_000.0,
            "bank_twd": 105_000.0,
            "bank_usd_in_twd": 0.0,
            "external_flow_twd": 0.0,
            "investment_flows_twd": 0.0,
            "fx_usd_twd": 31.6,
            "period_return": 0.05,
            "cum_twr": 0.05,
            "v_start": 1_000_000.0,
            "tw": {"holdings": [], "trades": [], "rebates": []},
            "foreign": {"holdings": [], "trades": []},
            "bank": {"flows": []},
            "dividend_events": [],
        },
    ],
    "summary": {
        "kpis": {
            "as_of": "2024-02-29",
            "real_now_twd": 1_050_000.0,
            "profit_twd": 50_000.0,
            "counterfactual_twd": 1_000_000.0,
        },
        "by_ticker": {
            "2330": {
                "code": "2330",
                "name": "TSMC",
                "venue": "tw",
                "trades": [],
                "fees_twd": 100.0,
                "tax_twd": 50.0,
            },
        },
        "all_trades": [
            {"date": "2024-01-15", "code": "2330", "side": "buy",
             "qty": 1000, "price": 600.0},
        ],
        "dividends": [
            {"date": "2024-02-10", "code": "2330", "amount_twd": 1000.0},
        ],
        "venue_flows_twd": [
            {"month": "2024-01", "tw": 600_000.0, "foreign": 300_000.0},
        ],
        "holdings_total_return": [
            {"code": "2330", "total_return_pct": 0.05},
        ],
        "cumulative_flows": {
            "real_curve": [{"month": "2024-01", "value": 1_000_000.0}],
            "counterfactual_curve": [{"month": "2024-01", "value": 1_000_000.0}],
        },
    },
}


@pytest.fixture
def fixture_path(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps(_FIXTURE), encoding="utf-8")
    return p


# --- Construction --------------------------------------------------


class TestConstruction:
    def test_existing_file_loads(self, fixture_path: Path) -> None:
        store = PortfolioStore(fixture_path)
        assert store.months  # non-empty list

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        # parse_statements.py hasn't run yet → file doesn't exist.
        # Legacy invariant: returns empty containers, never raises.
        store = PortfolioStore(tmp_path / "does_not_exist.json")
        assert store.months == []
        assert store.summary == {}
        assert store.kpis == {}
        assert store.by_ticker == {}


# --- Property surface ----------------------------------------------


class TestPropertySurface:
    """Every property the legacy DataStore exposes must port verbatim.

    The legacy app/api/_helpers.py + the 13 blueprint files reach
    through these properties; downstream router parity depends on
    every one being present with the same return shape.
    """

    @pytest.fixture
    def store(self, fixture_path: Path) -> PortfolioStore:
        return PortfolioStore(fixture_path)

    def test_months_returns_list(self, store: PortfolioStore) -> None:
        assert isinstance(store.months, list)
        assert len(store.months) == 2
        assert store.months[0]["month"] == "2024-01"

    def test_summary_returns_dict(self, store: PortfolioStore) -> None:
        assert isinstance(store.summary, dict)
        assert "kpis" in store.summary

    def test_kpis(self, store: PortfolioStore) -> None:
        assert store.kpis["real_now_twd"] == 1_050_000.0

    def test_by_ticker(self, store: PortfolioStore) -> None:
        assert "2330" in store.by_ticker
        assert store.by_ticker["2330"]["name"] == "TSMC"

    def test_all_trades(self, store: PortfolioStore) -> None:
        assert isinstance(store.all_trades, list)
        assert store.all_trades[0]["code"] == "2330"

    def test_dividends(self, store: PortfolioStore) -> None:
        assert isinstance(store.dividends, list)
        assert store.dividends[0]["code"] == "2330"

    def test_venue_flows_twd(self, store: PortfolioStore) -> None:
        assert isinstance(store.venue_flows_twd, list)
        assert store.venue_flows_twd[0]["tw"] == 600_000.0

    def test_holdings_total_return(self, store: PortfolioStore) -> None:
        assert isinstance(store.holdings_total_return, list)
        assert store.holdings_total_return[0]["code"] == "2330"

    def test_cumulative_flows(self, store: PortfolioStore) -> None:
        assert "real_curve" in store.cumulative_flows

    def test_latest_month(self, store: PortfolioStore) -> None:
        assert store.latest_month["month"] == "2024-02"

    def test_as_of(self, store: PortfolioStore) -> None:
        assert store.as_of == "2024-02-29"

    def test_raw_returns_full_payload(self, store: PortfolioStore) -> None:
        # raw is the escape hatch for callers that need a key not yet
        # exposed as a property — must return the entire JSON dict.
        assert "months" in store.raw
        assert "summary" in store.raw


# --- Empty-file edge case ------------------------------------------


class TestEmptyFile:
    def test_empty_months_no_latest(self, tmp_path: Path) -> None:
        # File present but no months yet — first run before any
        # statement is decoded.
        p = tmp_path / "portfolio.json"
        p.write_text(json.dumps({"months": [], "summary": {}}), encoding="utf-8")
        store = PortfolioStore(p)
        assert store.months == []
        assert store.latest_month == {}
        assert store.as_of is None


# --- Mtime-watching reload -----------------------------------------


class TestReloadOnMtimeChange:
    """The reason DataStore exists at all.

    parse_statements.py rewrites portfolio.json while the server
    is running. Without mtime-driven reload, /api/* endpoints serve
    stale data forever. This test exercises both the no-change
    short-circuit and the mtime-changed reload path.
    """

    def test_reload_picks_up_overwritten_file(self, fixture_path: Path) -> None:
        store = PortfolioStore(fixture_path)
        assert len(store.months) == 2

        # Overwrite with a different payload. Bump mtime explicitly
        # to defeat filesystem second-resolution timestamps in CI.
        new_payload = {
            "months": [{"month": "2024-03", "equity_twd": 2_000_000.0,
                        "tw": {}, "foreign": {}, "bank": {}}],
            "summary": {"kpis": {"as_of": "2024-03-31"}},
        }
        fixture_path.write_text(json.dumps(new_payload), encoding="utf-8")
        future = time.time() + 5
        os.utime(fixture_path, (future, future))

        # Property access triggers mtime check → reload.
        assert len(store.months) == 1
        assert store.months[0]["month"] == "2024-03"
        assert store.as_of == "2024-03-31"

    def test_no_reload_when_mtime_unchanged(self, fixture_path: Path) -> None:
        store = PortfolioStore(fixture_path)
        first_raw_id = id(store.raw)
        # Second access without any file change must reuse the cached
        # dict. id() check confirms no reload happened.
        assert id(store.raw) == first_raw_id
