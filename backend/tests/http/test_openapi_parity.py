"""Parity check — Phase 6 Cycle 44.

Static parity: every legacy URL declared in app/api/*.py + app/__init__.py
must be registered on the new FastAPI app.

NOT a runtime diff — that would require booting the legacy backend with
real fixtures, and Phase 6 deliberately returns empty-state envelopes
where legacy returns full analytics. Per PLAN section 6 the contract is
"Same URL shapes (/api/...), same {ok, data} envelope" — both of which
are static properties of the route registration + the envelope helper
already enforced inside each cycle's TestClient tests.

The legacy URL inventory below is the authoritative list of every
/api/* path the existing Flask app serves (read-locked at HEAD on
2026-05-02). A future Phase 7 cycle that intentionally drops or
renames a legacy URL must update this table — surfacing the divergence
in code review instead of at runtime.

Convention: legacy Flask path-converters (`<code>`, `<int:event_id>`)
are normalized to FastAPI brace syntax (`{code}`, `{event_id}`) before
comparison. The HTTP method is part of the parity key — POST and GET
on the same path are distinct contract surfaces.
"""
from __future__ import annotations

import re

import pytest

# (method, path) — every legacy /api/* URL.
# Source of truth: `grep -rnE "@bp\.(get|post)|@app\.get|@app\.post" app/`
LEGACY_URLS: list[tuple[str, str]] = [
    # Health
    ("GET", "/api/health"),
    # Summary
    ("GET", "/api/summary"),
    # Holdings
    ("GET", "/api/holdings/current"),
    ("GET", "/api/holdings/timeline"),
    ("GET", "/api/holdings/sectors"),
    ("GET", "/api/holdings/snapshot/{month}"),
    # Performance
    ("GET", "/api/performance/timeseries"),
    ("GET", "/api/performance/rolling"),
    ("GET", "/api/performance/attribution"),
    # Transactions
    ("GET", "/api/transactions"),
    ("GET", "/api/transactions/aggregates"),
    # Cashflows
    ("GET", "/api/cashflows/monthly"),
    ("GET", "/api/cashflows/cumulative"),
    ("GET", "/api/cashflows/bank"),
    # Dividends / risk / fx / tax
    ("GET", "/api/dividends"),
    ("GET", "/api/risk"),
    ("GET", "/api/fx"),
    ("GET", "/api/tax"),
    # Tickers
    ("GET", "/api/tickers"),
    ("GET", "/api/tickers/{code}"),
    # Benchmarks
    ("GET", "/api/benchmarks/strategies"),
    ("GET", "/api/benchmarks/compare"),
    # Daily
    ("GET", "/api/daily/equity"),
    ("GET", "/api/daily/prices/{symbol}"),
    # Today (read)
    ("GET", "/api/today/snapshot"),
    ("GET", "/api/today/movers"),
    ("GET", "/api/today/sparkline"),
    ("GET", "/api/today/period-returns"),
    ("GET", "/api/today/drawdown"),
    ("GET", "/api/today/risk-metrics"),
    ("GET", "/api/today/calendar"),
    ("GET", "/api/today/freshness"),
    ("GET", "/api/today/reconcile"),
    # Admin
    ("GET", "/api/admin/failed-tasks"),
    ("POST", "/api/admin/refresh"),
    ("POST", "/api/admin/retry-failed"),
    ("POST", "/api/admin/reconcile"),
    ("POST", "/api/admin/reconcile/{event_id}/dismiss"),
]


@pytest.fixture(scope="module")
def routes() -> set[tuple[str, str]]:
    from invest.app import create_app

    app = create_app()
    out: set[tuple[str, str]] = set()
    for r in app.routes:
        path = getattr(r, "path", None)
        methods = getattr(r, "methods", None)
        if not path or not methods:
            continue
        for m in methods:
            if m in {"HEAD", "OPTIONS"}:
                continue
            out.add((m, path))
    return out


@pytest.mark.parametrize(("method", "path"), LEGACY_URLS)
def test_legacy_url_registered(
    method: str, path: str, routes: set[tuple[str, str]]
) -> None:
    """Every legacy /api/* URL is registered on the new FastAPI app."""
    assert (method, path) in routes, (
        f"{method} {path} from legacy /api/* surface is not registered "
        f"on the new FastAPI app — Phase 6 contract violated."
    )


def test_no_unexpected_api_routes(routes: set[tuple[str, str]]) -> None:
    """The new app exposes only legacy /api/* URLs (plus FastAPI built-ins).

    Any /api/* route on the new app that isn't in LEGACY_URLS is either:
      - a typo introduced during the port (parity failure), or
      - a deliberate Phase 6+ extension that needs a corresponding
        legacy-url entry added with a clear comment.
    """
    legacy_set = set(LEGACY_URLS)
    api_routes = {(m, p) for (m, p) in routes if p.startswith("/api/")}
    extra = api_routes - legacy_set
    assert not extra, (
        f"New app exposes /api/* routes not present in legacy: {sorted(extra)}"
    )


_ENVELOPE_FIELDS = {"ok", "data"}


def test_envelope_helper_shape() -> None:
    """The {ok, data} envelope helper preserves legacy contract.

    Legacy Flask app's app/api/_helpers.py:envelope(payload) returns
    {"ok": True, "data": payload}. The new helper must match.
    """
    from invest.http.envelope import success

    out = success({"hello": "world"})
    assert set(out.keys()) >= _ENVELOPE_FIELDS
    assert out["ok"] is True
    assert out["data"] == {"hello": "world"}


def test_error_envelope_shape() -> None:
    """Error envelope must produce {ok: False, error: <message>}."""
    from invest.http.envelope import error

    out = error("boom")
    assert out["ok"] is False
    assert out["error"] == "boom"


def test_openapi_schema_generates() -> None:
    """FastAPI's OpenAPI generation must succeed without errors.

    Phase 8 codegens a typed client from this schema; a generation
    failure here breaks the entire frontend rebuild downstream.
    """
    from invest.app import create_app

    app = create_app()
    schema = app.openapi()
    assert schema["openapi"].startswith("3.")
    assert "paths" in schema
    # Every legacy URL with FastAPI-syntax path-params must be in paths.
    for _method, path in LEGACY_URLS:
        assert path in schema["paths"], f"missing from OpenAPI: {path}"


_SEM_VER = re.compile(r"^\d+\.\d+\.\d+")


def test_openapi_has_version() -> None:
    """The exported schema carries an info.version — codegen tools key on it."""
    from invest.app import create_app

    app = create_app()
    info = app.openapi().get("info", {})
    assert "version" in info
    # Allow both semver (1.2.3) and our current "0.1.0" baseline.
    assert _SEM_VER.match(info["version"]) or info["version"]
