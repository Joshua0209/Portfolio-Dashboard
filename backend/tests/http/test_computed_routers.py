"""Reproducer for Phase 6 Cycle 41 — computed routers.

RED: invest.http.routers.{performance,risk,cashflows} do not exist.

Cycle 41 ports the three analytics-touching blueprints. Per the same
Phase 6 contract pinned in Cycle 40, these routers are shells:
empty-state envelopes match legacy shapes, query params (?method=,
?resolution=daily) are validated, but the actual TWR / drawdown /
attribution math doesn't run until the analytics layer ports in
Phase 7 (PLAN section 6 explicitly lists "Phase 3 — Analytics module"
which hasn't happened yet — `analytics.py` is still 995 lines in
app/).

What's pinned here:
  performance  3 endpoints + ?method= validation (the only routers
               with a non-trivial typed query param contract)
  risk         single endpoint, empty-state envelope
  cashflows    3 endpoints (monthly returns a list; cumulative + bank
               are dicts)
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine


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


# --- /api/performance ----------------------------------------------------


class TestPerformance:
    def test_timeseries_empty_envelope_keys(self, client):
        d = _data(client.get("/api/performance/timeseries"))
        # Pinned legacy empty-state keys (matches app/api/performance.py
        # `not months` branch).
        assert d["empty"] is True
        assert d["monthly"] == []
        assert d["twr_total"] == 0
        assert d["xirr"] is None
        assert d["max_drawdown"] == 0
        assert d["sharpe_annualized"] == 0

    def test_timeseries_method_default_is_day_weighted(self, client):
        d = _data(client.get("/api/performance/timeseries"))
        # Even on empty payload, the method echo should be the default.
        # INVARIANT: matches /api/summary's default_weighting so the two
        # surfaces never disagree on cum_twr.
        assert d.get("method") == "day_weighted"

    def test_timeseries_accepts_known_methods(self, client):
        for m in ("day_weighted", "mid_month", "eom"):
            r = client.get(f"/api/performance/timeseries?method={m}")
            assert r.status_code == 200
            assert _data(r)["method"] == m

    def test_timeseries_rejects_unknown_method(self, client):
        r = client.get("/api/performance/timeseries?method=bogus")
        # FastAPI Query enum / Literal type validation produces 422.
        assert r.status_code == 422

    def test_rolling_empty_envelope(self, client):
        d = _data(client.get("/api/performance/rolling"))
        # Legacy returns rolling_3m/6m/12m/sharpe_6m keyed dict, even on empty.
        # Empty months → empty lists in each rolling key.
        assert d.get("rolling_3m", []) == []
        assert d.get("rolling_6m", []) == []
        assert d.get("rolling_12m", []) == []

    def test_attribution_empty_returns_list(self, client):
        d = _data(client.get("/api/performance/attribution"))
        # Legacy: empty months → envelope([]) — literal empty list.
        assert d == []


# --- /api/risk -----------------------------------------------------------


class TestRisk:
    def test_empty_envelope(self, client):
        d = _data(client.get("/api/risk"))
        # Legacy keys: drawdown_curve, hhi, top5_share, top10_share,
        # leverage_exposure, ratios.
        assert isinstance(d, dict)
        assert d.get("drawdown_curve", []) == []
        assert d.get("hhi", 0) == 0


# --- /api/cashflows ------------------------------------------------------


class TestCashflows:
    def test_monthly_empty_list(self, client):
        # Legacy returns a list of monthly flow rows.
        d = _data(client.get("/api/cashflows/monthly"))
        assert d == []

    def test_cumulative_empty_envelope(self, client):
        d = _data(client.get("/api/cashflows/cumulative"))
        # Legacy: real_curve + counterfactual_curve.
        assert isinstance(d, dict)
        assert d.get("real_curve", []) == []
        assert d.get("counterfactual_curve", []) == []

    def test_bank_empty_envelope(self, client):
        d = _data(client.get("/api/cashflows/bank"))
        # Legacy returns a flat list of bank ledger rows (one per tx).
        # Empty months → empty list.
        assert d == []
