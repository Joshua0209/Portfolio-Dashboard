"""FastAPI factory — `create_app()` returns the wired-up app.

Every router lands here. The factory is environment-clean: no DB
connection, no env-var read, no I/O. Tests instantiate freely.

Phase 6 progress per PLAN section 6:
  Cycle 39 — scaffolding: factory + envelope + deps + health
  Cycle 40 — read-only data routers
  Cycle 41 — computed routers
  Cycle 42 — today + admin routers (replaces the Cycle 39 _probe)
  Cycle 43 — benchmarks + daily routers
  Cycle 44 — OpenAPI export + parity smoke

The /api/admin/_probe endpoint from Cycle 39 is gone — Cycle 42's real
admin router exercises require_admin via the actual endpoints
(refresh, retry-failed, reconcile, reconcile/dismiss) and the test
fixture in test_scaffolding.py was already a probe-style harness, so
no test breakage. The `_probe` URL itself is freed up — if a future
non-admin endpoint wants /_probe it's available.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlmodel import SQLModel

from invest.http.routers.benchmarks import router as benchmarks_router
from invest.http.routers.cashflows import router as cashflows_router
from invest.http.routers.daily import router as daily_router
from invest.http.routers.dividends import router as dividends_router
from invest.http.routers.fx import router as fx_router
from invest.http.routers.health import router as health_router
from invest.http.routers.holdings import router as holdings_router
from invest.http.routers.performance import router as performance_router
from invest.http.routers.risk import router as risk_router
from invest.http.routers.summary import router as summary_router
from invest.http.routers.tax import router as tax_router
from invest.http.routers.tickers import router as tickers_router
from invest.http.routers.today import (
    admin_router as today_admin_router,
    read_router as today_read_router,
)
from invest.http.routers.transactions import router as transactions_router


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    # Bootstrap schema on first start. create_all is non-destructive —
    # it only creates missing tables, never alters existing ones.
    # During the Phase 0→9 transition the new backend points at its own
    # DAILY_DB_PATH (see .env) so it doesn't collide with the legacy
    # dashboard.db schema. Phase 0's Alembic plan supersedes this once set up.
    from invest.http.deps import _get_engine
    from invest.persistence.models import (  # noqa: F401  (register tables)
        failed_task,
        fx_rate,
        portfolio_daily,
        position_daily,
        price,
        reconcile_event,
        symbol_market,
        trade,
    )

    SQLModel.metadata.create_all(_get_engine())
    yield


def create_app() -> FastAPI:
    app = FastAPI(title="invest backend", version="0.1.0", lifespan=_lifespan)

    app.include_router(health_router)
    app.include_router(summary_router)
    app.include_router(holdings_router)
    app.include_router(transactions_router)
    app.include_router(dividends_router)
    app.include_router(fx_router)
    app.include_router(tax_router)
    app.include_router(tickers_router)
    app.include_router(performance_router)
    app.include_router(risk_router)
    app.include_router(cashflows_router)
    app.include_router(today_read_router)
    app.include_router(today_admin_router)
    app.include_router(benchmarks_router)
    app.include_router(daily_router)

    return app


# Module-level instance so `uvicorn invest.app:app` works without --factory.
# create_app() is environment-clean (no DB I/O at construction), so importing
# this module is safe in tests too — the lifespan hook is the seam where
# DB I/O happens, and it only fires on real app startup, not on import.
app = create_app()
