"""FastAPI factory — `create_app()` returns the wired-up app.

Every router lands here. The factory is environment-clean: no DB
connection, no env-var read, no I/O. Tests instantiate freely.

Phase 6 progress per PLAN section 6:
  Cycle 39 (this) — scaffolding: factory + envelope + deps + health
  Cycle 40-44     — routers wired in incrementally

The /api/admin/_probe endpoint is a Cycle 39 fixture for exercising
require_admin without coupling tests to a real admin router. Cycle 42
will replace it with the actual admin endpoints (refresh, reconcile,
retry-failed, dismiss).
"""
from __future__ import annotations

from fastapi import Depends, FastAPI
from fastapi.responses import Response

from invest.http.deps import require_admin
from invest.http.routers.dividends import router as dividends_router
from invest.http.routers.fx import router as fx_router
from invest.http.routers.health import router as health_router
from invest.http.routers.holdings import router as holdings_router
from invest.http.routers.summary import router as summary_router
from invest.http.routers.tax import router as tax_router
from invest.http.routers.tickers import router as tickers_router
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

    # Cycle 39 admin probe — wired to exercise require_admin in tests.
    # Replaced by the real admin router in Cycle 42.
    @app.post(
        "/api/admin/_probe",
        status_code=204,
        dependencies=[Depends(require_admin)],
    )
    def _admin_probe() -> Response:
        return Response(status_code=204)

    return app
