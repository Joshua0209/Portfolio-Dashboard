"""Reproducer for Phase 6 Cycle 43 — benchmarks + daily routers.

RED: invest.http.routers.{benchmarks,daily} do not exist.

Cycle 43 ports the two yfinance-touching surfaces. Per the same Phase 6
contract, no yfinance is invoked — these are shells that pin URL shape,
envelope, query-param validation, and (for /daily/*) the daily-state
machine gating from Cycle 42.

  benchmarks/strategies  static list of strategy configs (Phase 7 ports
                         the full STRATEGIES catalogue from app/benchmarks
                         alongside the yfinance fetcher; for now this
                         endpoint returns an empty list — the frontend
                         can render the page heading + an empty grid).
  benchmarks/compare     empty envelope (analytics layer + benchmarks
                         catalogue are both Phase 7+ ports).
  daily/equity           state-gated; validates start/end ISO dates;
                         400 on malformed.
  daily/prices/{symbol}  state-gated; validates start/end ISO dates.

Date validation is the only routing-level invariant that we MUST get
right today: legacy uses a strict regex (rejects 2026-99-99 instead of
crashing in fromisoformat). FastAPI's typed query param doesn't reject
bad-month-day combinations, so we keep the regex check.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.portfolio_daily import PortfolioDaily


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng)
    return eng


@pytest.fixture
def client(engine, fake_portfolio, fake_daily):
    from invest.app import create_app
    from invest.http.deps import get_session
    from .conftest import install_store_overrides

    app = create_app()

    def _override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _override
    install_store_overrides(app, portfolio=fake_portfolio, daily=fake_daily)
    return TestClient(app)


def _data(r) -> dict:
    body = r.json()
    assert body["ok"] is True
    return body["data"]


def _portfolio_row(d: date) -> PortfolioDaily:
    return PortfolioDaily(
        date=d, equity=Decimal("1000000"), cost_basis=Decimal("900000"),
        currency="TWD", source="snapshot",
    )


# --- /api/benchmarks ----------------------------------------------------


class TestBenchmarks:
    def test_strategies_full_catalogue(self, client):
        d = _data(client.get("/api/benchmarks/strategies"))
        # Phase 6.5: full STRATEGIES catalogue ported. List, not empty.
        assert isinstance(d, list)
        assert len(d) > 0
        # Each entry has the required keys.
        for s in d:
            assert {"key", "name", "market", "weights", "description"} <= set(s.keys())

    def test_compare_empty_envelope(self, client):
        d = _data(client.get("/api/benchmarks/compare"))
        # Legacy returns {"empty": True} when months is empty.
        assert d.get("empty") is True

    def test_compare_accepts_keys_param(self, client):
        # Even on empty store, the keys parameter shouldn't error.
        r = client.get("/api/benchmarks/compare?keys=tw_passive,us_passive")
        assert r.status_code == 200


# --- /api/daily/equity --------------------------------------------------


class TestDailyEquity:
    def test_returns_202_when_portfolio_daily_empty(self, client):
        r = client.get("/api/daily/equity")
        assert r.status_code == 202
        assert r.json()["data"]["state"] == "INITIALIZING"

    def test_returns_200_when_ready(self, client, fake_daily):
        # Phase 6.5: state-gate keys on DailyStore.last_known_date or
        # equity_curve presence (both legacy signals).
        fake_daily.equity_curve = [
            {"date": "2026-04-30", "equity_twd": 1_000_000,
             "n_positions": 5, "fx_usd_twd": 31.5, "has_overlay": False,
             "cash_twd": 0},
        ]
        r = client.get("/api/daily/equity")
        assert r.status_code == 200
        d = _data(r)
        assert "points" in d
        assert isinstance(d["points"], list)

    def test_400_on_malformed_start(self, client, fake_daily):
        fake_daily.equity_curve = [
            {"date": "2026-04-30", "equity_twd": 1_000_000,
             "n_positions": 5, "fx_usd_twd": 31.5, "has_overlay": False,
             "cash_twd": 0},
        ]
        r = client.get("/api/daily/equity?start=2026-99-99")
        assert r.status_code == 400
        assert r.json()["ok"] is False

    def test_400_on_malformed_end(self, client, fake_daily):
        fake_daily.equity_curve = [
            {"date": "2026-04-30", "equity_twd": 1_000_000,
             "n_positions": 5, "fx_usd_twd": 31.5, "has_overlay": False,
             "cash_twd": 0},
        ]
        r = client.get("/api/daily/equity?end=not-a-date")
        assert r.status_code == 400


# --- /api/daily/prices/{symbol} -----------------------------------------


class TestDailyPrices:
    def test_returns_202_when_portfolio_daily_empty(self, client):
        r = client.get("/api/daily/prices/2330")
        assert r.status_code == 202

    def test_returns_200_when_ready(self, client, fake_daily):
        fake_daily.equity_curve = [
            {"date": "2026-04-30", "equity_twd": 1_000_000,
             "n_positions": 5, "fx_usd_twd": 31.5, "has_overlay": False,
             "cash_twd": 0},
        ]
        r = client.get("/api/daily/prices/2330")
        assert r.status_code == 200
        d = _data(r)
        assert d["symbol"] == "2330"
        assert isinstance(d["points"], list)
        assert isinstance(d["trades"], list)

    def test_400_on_malformed_start(self, client, fake_daily):
        fake_daily.equity_curve = [
            {"date": "2026-04-30", "equity_twd": 1_000_000,
             "n_positions": 5, "fx_usd_twd": 31.5, "has_overlay": False,
             "cash_twd": 0},
        ]
        r = client.get("/api/daily/prices/2330?start=2026-13-01")
        assert r.status_code == 400
