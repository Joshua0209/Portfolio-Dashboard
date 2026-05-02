"""Shared HTTP test fixtures — Phase 6.5 onwards.

Phase 6.5 wires the routers to PortfolioStore + DailyStore. The
existing tests (Phase 6 contract tests) only override get_session.
This conftest provides a consistent way to also override
get_portfolio_store + get_daily_store with empty fakes so the
empty-state envelope contract still holds.

Tests that need populated stores construct their own and pass them
to the appropriate fixture.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest


class _FakePortfolio:
    """Minimal PortfolioStore with overridable backing dict.

    Default constructs to "empty portfolio" (no months, no summary)
    which exercises the legacy empty-state branches in routers.
    """

    def __init__(self, raw: dict | None = None) -> None:
        self._raw = raw or {"months": [], "summary": {}}

    @property
    def raw(self) -> dict[str, Any]:
        return self._raw

    @property
    def months(self) -> list[dict[str, Any]]:
        return self._raw.get("months", [])

    @property
    def summary(self) -> dict[str, Any]:
        return self._raw.get("summary", {})

    @property
    def kpis(self) -> dict[str, Any]:
        return self.summary.get("kpis", {})

    @property
    def by_ticker(self) -> dict[str, Any]:
        return self.summary.get("by_ticker", {})

    @property
    def all_trades(self) -> list[dict[str, Any]]:
        return self.summary.get("all_trades", [])

    @property
    def cumulative_flows(self) -> dict[str, Any]:
        return self.summary.get("cumulative_flows", {})

    @property
    def dividends(self) -> list[dict[str, Any]]:
        return self.summary.get("dividends", [])

    @property
    def venue_flows_twd(self) -> list[dict[str, Any]]:
        return self.summary.get("venue_flows_twd", [])

    @property
    def holdings_total_return(self) -> list[dict[str, Any]]:
        return self.summary.get("holdings_total_return", [])

    @property
    def latest_month(self) -> dict[str, Any]:
        m = self.months
        return m[-1] if m else {}

    @property
    def as_of(self) -> str | None:
        return self.kpis.get("as_of")


class _FakeDaily:
    """Minimal DailyStore — every method returns empty/None by default.

    Tests that need specific daily-layer state set instance attributes
    directly (e.g. fake_daily.snapshot = {"date": "...", ...}).
    """

    def __init__(self) -> None:
        self.snapshot: dict | None = None
        self.positions: list[dict] = []
        self.closes: dict[str, dict] = {}
        self.equity_curve: list[dict] = []
        self.drawdown_series: list[dict] = []
        self.fx_series: list[dict] = []
        self.usd_exposure_series: list[dict] = []
        self.allocation_series: list[dict] = []
        self.failed_tasks: list[dict] = []
        self.ticker_history: dict[str, list[dict]] = {}
        self.positions_for_ticker: dict[str, list[dict]] = {}

    def get_today_snapshot(self) -> dict | None:
        return self.snapshot

    def get_positions_snapshot(self, _date: str) -> list[dict]:
        return list(self.positions)

    def get_latest_close(self, code: str) -> dict | None:
        return self.closes.get(code)

    def get_latest_closes(self, codes: list[str]) -> dict[str, dict]:
        return {c: self.closes[c] for c in codes if c in self.closes}

    def get_equity_curve(self, **_kwargs) -> list[dict]:
        return list(self.equity_curve)

    def get_drawdown_series(self, **_kwargs) -> list[dict]:
        return list(self.drawdown_series)

    def get_fx_series(self, **_kwargs) -> list[dict]:
        return list(self.fx_series)

    def get_usd_exposure_series(self, **_kwargs) -> list[dict]:
        return list(self.usd_exposure_series)

    def get_allocation_timeseries(self, **_kwargs) -> list[dict]:
        return list(self.allocation_series)

    def get_failed_tasks(self) -> list[dict]:
        return list(self.failed_tasks)

    def get_ticker_history(self, code: str, **_kwargs) -> list[dict]:
        return list(self.ticker_history.get(code, []))

    def get_positions_for_ticker(self, code: str, **_kwargs) -> list[dict]:
        return list(self.positions_for_ticker.get(code, []))


@pytest.fixture
def fake_portfolio() -> _FakePortfolio:
    """Empty portfolio by default. Tests populate via fake_portfolio._raw."""
    return _FakePortfolio()


@pytest.fixture
def fake_daily() -> _FakeDaily:
    """Empty daily store by default. Tests set attributes for population."""
    return _FakeDaily()


def install_store_overrides(app, *, portfolio: _FakePortfolio, daily: _FakeDaily) -> None:
    """Wire fake stores into a FastAPI app's dependency_overrides.

    Tests call this after constructing their TestClient to swap the
    real singletons for empty (or populated) fakes. Putting the
    helper here keeps the import path consistent and avoids the 5
    existing test files each spelling out the same boilerplate.
    """
    from invest.http.deps import get_portfolio_store, get_daily_store

    app.dependency_overrides[get_portfolio_store] = lambda: portfolio
    app.dependency_overrides[get_daily_store] = lambda: daily
