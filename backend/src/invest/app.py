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

from fastapi import FastAPI

from invest.http.routers.cashflows import router as cashflows_router
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


def create_app() -> FastAPI:
    app = FastAPI(title="invest backend", version="0.1.0")

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

    return app
