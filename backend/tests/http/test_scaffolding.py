"""Reproducer for HTTP layer scaffolding — Phase 6 Cycle 39.

RED: invest.http and invest.app do not exist.

Cycle 39 establishes the FastAPI foundation for Phase 6:
  invest.core.config        — pydantic Settings (env vars)
  invest.http.envelope      — {ok, data} response wrapping
  invest.http.deps          — get_session, require_admin
  invest.http.routers.health — /api/health
  invest.app                — FastAPI() factory

Why a separate Cycle for scaffolding before any actual router logic:
  Every router in Cycles 40-44 will Depends on get_session and use
  envelope.success(). Pinning these contracts NOW means router code
  stays uniform and the per-router tests focus on business logic, not
  on rediscovering the DI shape.

Health endpoint contract (forward-compatible with Phase 7 jobs/state):
  months_loaded     — count of distinct YYYY-MM in Trade.date.
                      Real signal even before jobs/snapshot.py lands.
  as_of             — max(Trade.date) ISO string, null if empty.
  daily_state       — "READY" if PortfolioDaily has rows, else
                      "INITIALIZING". "FAILED" deferred to Phase 7
                      (no backfill_state machine ported yet).
  daily_last_known  — max(PortfolioDaily.date), null if empty.
  daily_progress    — {} (placeholder for Phase 7 progress dict).
  daily_error       — null (placeholder).

Why we don't use the legacy data_store / portfolio.json:
  The new design has SQLite as the single store. Trade rows feed
  every projection; PortfolioDaily is the materialized equity-curve
  cache. Reading JSON would re-introduce the dual-source problem
  the rewrite is supposed to eliminate.

require_admin semantics (preserved verbatim from legacy):
  ADMIN_TOKEN env unset       → no gate (localhost dev mode)
  ADMIN_TOKEN env set, header missing/wrong → 401
  ADMIN_TOKEN env set, header matches       → allow
  Reads stay open by design — the gate is only on write endpoints
  that opt in via Depends(require_admin).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.domain.trade import Side
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.trade import Trade


# --- Fixtures -------------------------------------------------------------


@pytest.fixture
def engine():
    # StaticPool + check_same_thread=False so the in-memory DB is shared
    # across the TestClient's worker thread and the test thread. Without
    # this, each FastAPI request lands on a fresh empty in-memory DB.
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng)
    return eng


@pytest.fixture
def client(engine):
    """FastAPI TestClient with get_session overridden to the in-memory
    engine. Each test gets a fresh DB."""
    from invest.app import create_app
    from invest.http.deps import get_session

    app = create_app()

    def _override():
        with Session(engine) as s:
            yield s

    app.dependency_overrides[get_session] = _override
    return TestClient(app)


def _trade(d: date, code: str = "2330") -> Trade:
    return Trade(
        date=d, code=code, side=int(Side.CASH_BUY), qty=1000,
        price=Decimal("100"), currency="TWD",
        fee=Decimal("0"), tax=Decimal("0"), rebate=Decimal("0"),
        source="pdf", venue="TW",
    )


def _portfolio_row(d: date) -> PortfolioDaily:
    return PortfolioDaily(
        date=d, equity=Decimal("1000000"), cost_basis=Decimal("900000"),
        currency="TWD", source="snapshot",
    )


# --- Envelope -------------------------------------------------------------


class TestEnvelope:
    def test_success_wraps_data(self):
        from invest.http.envelope import success

        out = success({"foo": 1})
        assert out == {"ok": True, "data": {"foo": 1}}

    def test_success_with_list(self):
        from invest.http.envelope import success

        out = success([1, 2, 3])
        assert out == {"ok": True, "data": [1, 2, 3]}


# --- App factory ----------------------------------------------------------


class TestAppFactory:
    def test_create_app_returns_fastapi_instance(self):
        """create_app() must produce a FastAPI app without I/O.
        No DB connections at construction time — get_session is the
        per-request seam; constructing the app is environment-clean."""
        from fastapi import FastAPI

        from invest.app import create_app

        app = create_app()
        assert isinstance(app, FastAPI)


# --- Health endpoint ------------------------------------------------------


class TestHealth:
    def test_returns_200_with_envelope(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200
        body = r.json()
        assert body["ok"] is True
        assert "data" in body

    def test_keys_match_legacy_contract(self, client):
        body = client.get("/api/health").json()["data"]
        # INVARIANT: same keys as legacy /api/health so the existing
        # frontend can hit either backend without changes.
        assert set(body.keys()) >= {
            "months_loaded", "as_of",
            "daily_state", "daily_last_known",
            "daily_progress", "daily_error",
        }

    def test_empty_db_initializing_state(self, client):
        body = client.get("/api/health").json()["data"]
        # No Trade, no PortfolioDaily -> nothing to show yet.
        assert body["months_loaded"] == 0
        assert body["as_of"] is None
        assert body["daily_state"] == "INITIALIZING"
        assert body["daily_last_known"] is None
        assert body["daily_progress"] == {}
        assert body["daily_error"] is None

    def test_months_loaded_counts_distinct_yyyy_mm(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 1, 10)))
            s.add(_trade(date(2026, 1, 20)))  # same month
            s.add(_trade(date(2026, 2, 5)))
            s.add(_trade(date(2026, 3, 5), code="2454"))
            s.commit()
        body = client.get("/api/health").json()["data"]
        assert body["months_loaded"] == 3
        assert body["as_of"] == "2026-03-05"

    def test_ready_state_when_portfolio_daily_has_rows(
        self, client, engine,
    ):
        """daily_state = READY iff PortfolioDaily has at least one row.
        Mirrors the legacy 'snapshot is not None' branch."""
        with Session(engine) as s:
            s.add(_portfolio_row(date(2026, 4, 30)))
            s.commit()
        body = client.get("/api/health").json()["data"]
        assert body["daily_state"] == "READY"
        assert body["daily_last_known"] == "2026-04-30"


# --- require_admin --------------------------------------------------------


class TestRequireAdmin:
    """Tests use a probe endpoint (/admin-probe) wired in app factory
    so we don't have to mount a router-under-test for every dep.

    Legacy contract preserved verbatim: token unset = no gate;
    token set = enforce on this endpoint."""

    def test_allows_when_admin_token_unset(self, client, monkeypatch):
        monkeypatch.delenv("ADMIN_TOKEN", raising=False)
        r = client.post("/api/admin/_probe")
        # Probe endpoint in scaffolding returns 204; gate is off.
        assert r.status_code == 204

    def test_returns_401_when_token_set_and_header_missing(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        r = client.post("/api/admin/_probe")
        assert r.status_code == 401

    def test_returns_401_when_token_set_and_header_wrong(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        r = client.post("/api/admin/_probe", headers={"X-Admin-Token": "wrong"})
        assert r.status_code == 401

    def test_allows_when_token_set_and_header_matches(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        r = client.post(
            "/api/admin/_probe", headers={"X-Admin-Token": "secret-xyz"},
        )
        assert r.status_code == 204
