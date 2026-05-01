"""Reproducer for Phase 6 Cycle 40 — read-only data routers.

RED: invest.http.routers.{summary,holdings,transactions,dividends,fx,tax,tickers}
do not exist; create_app() does not mount them yet.

Cycle 40 ports the seven read-only blueprints from app/api/ to FastAPI routers.
Pinned here is the **contract**, not byte-for-byte data parity — Phase 6 stops
at "every legacy URL responds with the right envelope and validates its query
params"; the data backing those responses comes online progressively as the
analytics + jobs layers port in Phase 7.

Empty-state shapes mirror the legacy empty-state envelopes (CLAUDE.md "API
surface" section + the literal `not s.months` branches in app/api/*.py) so
the existing frontend keeps rendering against the new backend during the
cutover with no client-side changes for empty panels.

What the new backend can compute right now (without a ported analytics layer):
  - transactions/list      Trade rows (the new schema's authoritative source)
  - transactions/aggregates trade COUNT + per-venue counts (totals require
                            currency-converted gross_twd which the schema
                            doesn't store; full totals are Phase 7)
  - tickers/list           DISTINCT(code) from Trade
  - tickers/<code>         404 unless code appears in Trade

Everything else (summary KPIs, holdings, dividends, fx P&L, tax basis,
sectors, snapshots) requires the monthly analytics aggregator — empty-state
envelope is the honest answer until Phase 7 wires it.

Contract pins:
  Envelope  every endpoint returns {ok: True, data: ...}; admin gating off
            because reads are open.
  URL shape /api/<resource>[/<sub>] matches legacy 1:1.
  Empty    when no data is loaded, the data field has the legacy empty-state
            keys (so the frontend's chart-empty branches still fire).
  404 path  /holdings/snapshot/<month> with no match; /tickers/<code> with no
            match. Both return JSON envelope with {ok: False, error: ...}.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine

from invest.domain.trade import Side
from invest.persistence.models.trade import Trade


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


def _trade(
    d: date,
    code: str = "2330",
    side: Side = Side.CASH_BUY,
    qty: int = 1000,
    price: str = "100",
    venue: str = "TW",
    source: str = "pdf",
) -> Trade:
    return Trade(
        date=d, code=code, side=int(side), qty=qty,
        price=Decimal(price), currency="TWD",
        fee=Decimal("0"), tax=Decimal("0"), rebate=Decimal("0"),
        source=source, venue=venue,
    )


def _envelope(r) -> dict:
    body = r.json()
    assert body["ok"] is True
    assert "data" in body
    return body["data"]


# --- /api/summary --------------------------------------------------------


class TestSummary:
    """Single GET. Empty-state envelope mirrors app/api/summary.py:_monthly_summary
    when months is empty: keys = {empty, kpis, twr, xirr, profit_twd,
    invested_twd, equity_curve, allocation, first_month, last_month,
    months_covered}.

    INVARIANT: data must include the empty-state keys even with no Trade rows
    so the frontend's chart-empty branches still activate.
    """

    def test_returns_200_envelope(self, client):
        r = client.get("/api/summary")
        assert r.status_code == 200
        d = _envelope(r)
        assert isinstance(d, dict)

    def test_empty_state_keys(self, client):
        d = _envelope(client.get("/api/summary"))
        assert set(d.keys()) >= {
            "empty", "kpis", "twr", "xirr", "profit_twd",
            "invested_twd", "equity_curve", "allocation",
            "first_month", "last_month", "months_covered",
        }
        assert d["empty"] is True
        assert d["equity_curve"] == []
        assert d["months_covered"] == 0


# --- /api/holdings -------------------------------------------------------


class TestHoldings:
    """4 endpoints: /current, /timeline, /sectors, /snapshot/<month>.
    Empty store returns the legacy empty-state envelopes.
    """

    def test_current_empty_state(self, client):
        d = _envelope(client.get("/api/holdings/current"))
        # legacy returns {"holdings": [], "total_twd": 0} when months is empty
        assert d["holdings"] == []
        assert "total_twd" in d

    def test_timeline_empty_returns_monthly_resolution(self, client):
        # ?resolution=daily falls back to monthly when daily store is empty.
        d = _envelope(client.get("/api/holdings/timeline"))
        assert d == {"resolution": "monthly", "rows": []}

        d2 = _envelope(client.get("/api/holdings/timeline?resolution=daily"))
        # INVARIANT: never 404 — fall back to monthly empty.
        assert d2["resolution"] == "monthly"

    def test_sectors_empty_is_empty_list(self, client):
        d = _envelope(client.get("/api/holdings/sectors"))
        assert d == []

    def test_snapshot_404_when_month_not_found(self, client):
        r = client.get("/api/holdings/snapshot/2026-04")
        assert r.status_code == 404
        body = r.json()
        assert body["ok"] is False


# --- /api/transactions ---------------------------------------------------


class TestTransactions:
    """Two endpoints: list "" and /aggregates.

    The new schema lets us compute these for real — Trade rows are the
    authoritative source. The legacy field encoding (Chinese side strings,
    name fields, gross_twd/fee_twd) is replaced by the new schema's shape;
    this is intentional — Phase 8 frontend regenerates types from OpenAPI.
    """

    def test_list_empty(self, client):
        d = _envelope(client.get("/api/transactions"))
        assert d == [] or d == {"trades": [], "count": 0}

    def test_list_returns_trade_rows(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 4, 10)))
            s.add(_trade(date(2026, 4, 20), code="2454"))
            s.commit()
        d = _envelope(client.get("/api/transactions"))
        rows = d if isinstance(d, list) else d["trades"]
        assert len(rows) == 2
        codes = {r["code"] for r in rows}
        assert codes == {"2330", "2454"}

    def test_list_filter_by_code(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 4, 10)))
            s.add(_trade(date(2026, 4, 20), code="2454"))
            s.commit()
        d = _envelope(client.get("/api/transactions?code=2330"))
        rows = d if isinstance(d, list) else d["trades"]
        assert len(rows) == 1
        assert rows[0]["code"] == "2330"

    def test_list_sorted_descending_by_date(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 1, 10), code="A"))
            s.add(_trade(date(2026, 4, 5), code="B"))
            s.add(_trade(date(2026, 2, 15), code="C"))
            s.commit()
        d = _envelope(client.get("/api/transactions"))
        rows = d if isinstance(d, list) else d["trades"]
        # Most recent first — same convention as legacy.
        assert rows[0]["code"] == "B"
        assert rows[-1]["code"] == "A"

    def test_aggregates_empty(self, client):
        d = _envelope(client.get("/api/transactions/aggregates"))
        # INVARIANT: legacy empty-state has totals + by_venue + monthly + venues.
        assert "totals" in d
        assert d["totals"]["trades"] == 0
        assert d["monthly"] == []
        assert d["venues"] == []

    def test_aggregates_counts_real_trades(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 4, 10), venue="TW"))
            s.add(_trade(date(2026, 4, 20), venue="TW", side=Side.CASH_SELL))
            s.add(_trade(date(2026, 4, 25), venue="Foreign", code="AAPL"))
            s.commit()
        d = _envelope(client.get("/api/transactions/aggregates"))
        assert d["totals"]["trades"] == 3
        # Both venues represented.
        assert set(d["venues"]) == {"TW", "Foreign"}


# --- /api/dividends ------------------------------------------------------


class TestDividends:
    def test_empty_envelope(self, client):
        d = _envelope(client.get("/api/dividends"))
        # Legacy returns dict with rows + by_ticker + monthly_by_venue + by_ccy + totals.
        # Empty: rows is [], totals are zeroed.
        assert isinstance(d, dict)
        assert d.get("rows", []) == []


# --- /api/fx -------------------------------------------------------------


class TestFx:
    def test_empty_returns_empty_curve(self, client):
        d = _envelope(client.get("/api/fx"))
        # Legacy empty-state keys.
        assert d.get("empty") is True
        assert d["rate_curve"] == []
        assert d["current_rate"] is None


# --- /api/tax ------------------------------------------------------------


class TestTax:
    def test_empty_envelope(self, client):
        d = _envelope(client.get("/api/tax"))
        # Legacy returns a list (or dict with 'rows'); empty either way.
        assert d == [] or d.get("rows", []) == []


# --- /api/tickers --------------------------------------------------------


class TestTickers:
    def test_list_empty(self, client):
        d = _envelope(client.get("/api/tickers"))
        assert d == []

    def test_list_distinct_codes(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 1, 10), code="2330"))
            s.add(_trade(date(2026, 2, 10), code="2330"))  # duplicate code
            s.add(_trade(date(2026, 3, 10), code="2454"))
            s.commit()
        d = _envelope(client.get("/api/tickers"))
        codes = {row["code"] for row in d}
        assert codes == {"2330", "2454"}

    def test_detail_404_when_unknown(self, client):
        r = client.get("/api/tickers/9999")
        assert r.status_code == 404
        assert r.json()["ok"] is False

    def test_detail_returns_envelope_when_known(self, client, engine):
        with Session(engine) as s:
            s.add(_trade(date(2026, 4, 10), code="2330"))
            s.commit()
        r = client.get("/api/tickers/2330")
        assert r.status_code == 200
        d = _envelope(r)
        assert d["code"] == "2330"
        # trades is the only universally-derivable field; richer fields
        # (held timeline, dividends) come in Phase 7.
        assert isinstance(d.get("trades", []), list)
