"""Reproducer for Phase 6 Cycle 42 — today + admin routers.

RED: invest.http.routers.today does not exist; create_app() does not
mount the /api/today/* or /api/admin/* surfaces.

Cycle 42 is the cohesion-preserving cycle: legacy app/api/today.py
mounts BOTH /api/today/* (read) and /api/admin/* (write) on the same
blueprint because they're operationally entangled — every admin button
on the /today page lives there. The new design preserves that file-
level cohesion via TWO APIRouter instances in one module file:

  invest.http.routers.today.read_router   (/api/today/* reads)
  invest.http.routers.today.admin_router  (/api/admin/* writes,
                                           gated by Depends(require_admin))

Both mounted from create_app() — the user gets one place to read the
operational surface.

Phase 6 baseline: today reads return EMPTY-state envelopes when the
daily store has data, 202 INITIALIZING when it doesn't (mirrors legacy
require_ready_or_warming decorator). 'FAILED' is deferred to Phase 7
(no backfill_state machine ported yet).

Admin write endpoints:
  POST /api/admin/refresh                  — empty summary
  POST /api/admin/retry-failed             — empty summary
  POST /api/admin/reconcile?month=YYYY-MM  — month regex pinned (400 on bad)
  POST /api/admin/reconcile/{id}/dismiss   — uses ReconcileRepo
  GET  /api/admin/failed-tasks             — empty list (read endpoint
                                              under /admin namespace; legacy
                                              has it open, we keep it open
                                              since reads stay unauthed
                                              by design — only POSTs gate)

Today reads pinned (one representative each — exhaustive parity is
Phase 7's job):
  GET /api/today/snapshot       — 202 empty / 200 with stub data
  GET /api/today/freshness      — always 200 (it's the staleness probe)
  GET /api/today/reconcile      — open ReconcileEvent rows projection

The freshness endpoint is intentionally NOT gated by the daily-state
machine — its WHOLE PURPOSE is to surface "no data yet" to the
operator. Legacy routes around `require_ready_or_warming` for this
exact reason; we preserve.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.reconcile_event import ReconcileEvent


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
def client(engine, monkeypatch, fake_portfolio, fake_daily):
    monkeypatch.delenv("ADMIN_TOKEN", raising=False)
    from invest.app import create_app
    from invest.http.deps import get_session
    from invest.jobs import snapshot_workflow
    from .conftest import install_store_overrides

    # Phase 11: /api/admin/refresh is wired to snapshot_workflow.run,
    # which fetches yfinance + Shioaji on the real path. Stub it out
    # in tests — endpoint contract here is "200 + envelope with
    # new_rows or skipped_reason key".
    monkeypatch.setattr(
        snapshot_workflow,
        "run",
        lambda store, portfolio: {
            "skipped_reason": "stubbed_in_test",
            "new_rows": 0,
            "new_dates": 0,
            "overlay": {"overlay_trades": 0, "dates_written": 0,
                        "skipped_reason": "no_gap"},
            "window": None,
        },
    )

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


# --- Today reads --------------------------------------------------------


class TestTodayReads:
    def test_snapshot_returns_202_when_portfolio_daily_empty(self, client):
        """INITIALIZING contract: daily-state-gated reads return 202 +
        progress envelope when the daily store has no rows yet. Legacy
        require_ready_or_warming decorator behavior."""
        r = client.get("/api/today/snapshot")
        assert r.status_code == 202
        body = r.json()
        assert body["ok"] is True
        assert body["data"]["state"] == "INITIALIZING"

    def test_snapshot_returns_200_when_portfolio_daily_has_rows(
        self, client, fake_daily,
    ):
        # Phase 6.5: today_snapshot reads from DailyStore.get_today_snapshot()
        # (legacy schema). _FakeDaily.snapshot is the seam.
        fake_daily.snapshot = {
            "date": "2026-04-30",
            "equity_twd": 1_000_000.0,
            "fx_usd_twd": 31.5,
            "n_positions": 5,
            "has_overlay": False,
        }
        r = client.get("/api/today/snapshot")
        assert r.status_code == 200
        d = _data(r)
        assert d.get("date") == "2026-04-30"

    def test_freshness_always_200_even_when_empty(self, client):
        """Freshness is the 'no data yet' probe — must NOT be gated by
        the daily-state machine. INVARIANT: always 200, with band='red'
        when no data."""
        r = client.get("/api/today/freshness")
        assert r.status_code == 200
        d = _data(r)
        assert d["data_date"] is None
        assert d["band"] == "red"

    def test_reconcile_open_events_empty(self, client):
        r = client.get("/api/today/reconcile")
        assert r.status_code == 200
        d = _data(r)
        assert d["events"] == []
        assert d["count"] == 0

    def test_reconcile_open_events_returns_open_only(self, client, engine):
        with Session(engine) as s:
            s.add(ReconcileEvent(
                pdf_month="2026-04",
                event_type="broker_pdf_buy_leg_mismatch",
                detail={"code": "2330", "sell_date": "2026-04-10"},
                status="open",
            ))
            s.add(ReconcileEvent(
                pdf_month="2026-03",
                event_type="broker_pdf_buy_leg_mismatch",
                detail={"code": "2454"},
                status="dismissed",
            ))
            s.commit()
        d = _data(client.get("/api/today/reconcile"))
        # Dismissed events are filtered out — same banner contract as legacy.
        assert d["count"] == 1
        assert d["events"][0]["pdf_month"] == "2026-04"
        assert d["events"][0]["event_type"] == "broker_pdf_buy_leg_mismatch"


# --- Admin reads (open by design) ---------------------------------------


class TestAdminReads:
    def test_failed_tasks_empty(self, client):
        # FailedTask repo with empty store. Reads stay open (no admin gate).
        r = client.get("/api/admin/failed-tasks")
        assert r.status_code == 200
        d = _data(r)
        assert d["tasks"] == []
        assert d["count"] == 0


# --- Admin writes (gated) -----------------------------------------------


class TestAdminWritesUngated:
    """ADMIN_TOKEN unset: legacy localhost-friendly default, all POSTs allow."""

    def test_refresh_returns_summary(self, client):
        r = client.post("/api/admin/refresh")
        assert r.status_code == 200
        d = _data(r)
        # Phase 6 baseline returns no-op summary (no snapshot daemon yet).
        assert "new_rows" in d or "skipped_reason" in d

    def test_retry_failed_returns_summary(self, client):
        r = client.post("/api/admin/retry-failed")
        assert r.status_code == 200
        d = _data(r)
        # Empty DLQ → resolved 0, still_failing 0.
        assert d.get("resolved", 0) == 0

    def test_reconcile_dismiss_marks_event(self, client, engine):
        with Session(engine) as s:
            ev = ReconcileEvent(
                pdf_month="2026-04",
                event_type="broker_pdf_buy_leg_mismatch",
                detail={"code": "2330"},
                status="open",
            )
            s.add(ev)
            s.commit()
            event_id = ev.id
        r = client.post(f"/api/admin/reconcile/{event_id}/dismiss")
        assert r.status_code == 200
        d = _data(r)
        assert d["event_id"] == event_id
        assert d["dismissed"] is True
        # Round-trip: subsequent reads should not see this event.
        d2 = _data(client.get("/api/today/reconcile"))
        assert d2["count"] == 0


class TestAdminReconcileMonthValidation:
    def test_reconcile_400_on_invalid_month(self, client):
        r = client.post("/api/admin/reconcile?month=2026-13")
        assert r.status_code == 400
        body = r.json()
        assert body["ok"] is False

    def test_reconcile_400_on_missing_month(self, client):
        r = client.post("/api/admin/reconcile")
        assert r.status_code == 400

    def test_reconcile_accepts_valid_month(self, client):
        r = client.post("/api/admin/reconcile?month=2026-04")
        # Phase 6 baseline returns empty diff summary; analysis logic is
        # already in invest.reconciliation.shioaji_audit (Cycle 38) but the
        # full PDF-vs-overlay diff is Phase 7's port. 200 + envelope is the
        # contract today.
        assert r.status_code == 200
        d = _data(r)
        assert isinstance(d, dict)


# --- Admin gating (token enforcement) -----------------------------------


class TestAdminGating:
    """When ADMIN_TOKEN is set, all admin POSTs require the matching
    X-Admin-Token header. Legacy contract preserved.

    Reads (failed-tasks GET, today/reconcile GET) stay UNGATED by design
    — only writes go through Depends(require_admin)."""

    def test_admin_post_rejected_without_token_when_set(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        for path in (
            "/api/admin/refresh",
            "/api/admin/retry-failed",
            "/api/admin/reconcile?month=2026-04",
        ):
            r = client.post(path)
            assert r.status_code == 401, f"{path} should be 401"

    def test_admin_post_accepted_with_token_when_set(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        r = client.post(
            "/api/admin/refresh", headers={"X-Admin-Token": "secret-xyz"},
        )
        assert r.status_code == 200

    def test_failed_tasks_read_stays_open_with_token_set(
        self, client, monkeypatch,
    ):
        """INVARIANT: only writes are gated. Reads (including those under
        /api/admin/) stay open."""
        monkeypatch.setenv("ADMIN_TOKEN", "secret-xyz")
        r = client.get("/api/admin/failed-tasks")
        assert r.status_code == 200
