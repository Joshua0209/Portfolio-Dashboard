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
def client(engine):
    from invest.app import create_app
    from invest.http.deps import get_session

    app = create_app()

    def _override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _override
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
    def test_strategies_empty_list(self, client):
        d = _data(client.get("/api/benchmarks/strategies"))
        # Phase 6 baseline: empty catalogue. Phase 7 ports STRATEGIES
        # alongside the yfinance fetcher.
        assert d == []

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

    def test_returns_200_when_ready(self, client, engine):
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        r = client.get("/api/daily/equity")
        assert r.status_code == 200
        d = _data(r)
        assert "points" in d
        assert isinstance(d["points"], list)

    def test_400_on_malformed_start(self, client, engine):
        # State-machine pass first (so we get to validation, not 202).
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        r = client.get("/api/daily/equity?start=2026-99-99")
        assert r.status_code == 400
        assert r.json()["ok"] is False

    def test_400_on_malformed_end(self, client, engine):
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        r = client.get("/api/daily/equity?end=not-a-date")
        assert r.status_code == 400


# --- /api/daily/prices/{symbol} -----------------------------------------


class TestDailyPrices:
    def test_returns_202_when_portfolio_daily_empty(self, client):
        r = client.get("/api/daily/prices/2330")
        assert r.status_code == 202

    def test_returns_200_when_ready(self, client, engine):
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        r = client.get("/api/daily/prices/2330")
        assert r.status_code == 200
        d = _data(r)
        assert d["symbol"] == "2330"
        assert isinstance(d["points"], list)
        assert isinstance(d["trades"], list)

    def test_400_on_malformed_start(self, client, engine):
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        r = client.get("/api/daily/prices/2330?start=2026-13-01")
        assert r.status_code == 400
