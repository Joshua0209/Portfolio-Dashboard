"""Cycle 47 RED — pin invest.jobs._positions contract.

Phase 1 redesigned the daily-layer schemas. Legacy positions_daily had
(qty, mv_local, mv_twd, cost_local, type, source) with a custom UPSERT
guard for PDF-vs-overlay; new PositionDaily has (qty, close, currency,
market_value, source). Legacy portfolio_daily had
(equity_twd, cash_twd, fx_usd_twd, n_positions, has_overlay); new
PortfolioDaily has (equity, cost_basis, currency, source).

So Cycle 47 is NOT a verbatim port — it builds a new positions
builder against the new schema using the same algorithmic skeleton:

  1. qty_trajectory(trades, dates) — running qty per (date, code)
     from the trade ledger. Buys add; sells subtract.
  2. forward_fill(rows, dates) — generic helper. Most-recent value
     wins; pre-first-row dates get the earliest value (so an early
     dashboard date still has a price/FX even if the upstream
     fetcher returned nothing for that day).
  3. build_daily(session, start, end) — orchestrator that walks
     priced dates, writes PositionDaily for each held (date, code),
     and aggregates to a single PortfolioDaily row per date.

Deferred from legacy (intentionally TODO, not lost):
  - Stock split detection (legacy used PDF anchors; the new world has
    Trade rows only, so split detection becomes a different
    algorithm — close-ratio inspection, no anchor signal).
  - Overlay merging (no trades_overlay table in the new schema —
    Phase 5 made Trade the single source of truth).
  - ref_price fallback (band-aid for yfinance gaps; revisit if a
    real price-source-down regression appears in tests).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.jobs import _positions
from invest.persistence.models.fx_rate import FxRate
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.position_daily import PositionDaily
from invest.persistence.models.price import Price
from invest.persistence.models.trade import Trade


# Trade.side encoding — invest.domain.trade.Side
CASH_BUY = 1
CASH_SELL = 2


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _add_trade(
    session,
    *,
    d: date,
    code: str,
    side: int,
    qty: int,
    price: Decimal,
    currency: str = "TWD",
    venue: str = "TW",
    source: str = "shioaji",
):
    session.add(
        Trade(
            date=d,
            code=code,
            side=side,
            qty=qty,
            price=price,
            currency=currency,
            source=source,
            venue=venue,
        )
    )
    session.commit()


def _add_price(
    session,
    *,
    d: date,
    symbol: str,
    close: Decimal,
    currency: str = "TWD",
    source: str = "yfinance",
):
    session.add(
        Price(
            date=d,
            symbol=symbol,
            close=close,
            currency=currency,
            source=source,
        )
    )
    session.commit()


def _add_fx(
    session,
    *,
    d: date,
    base: str,
    quote: str,
    rate: Decimal,
    source: str = "yfinance",
):
    session.add(
        FxRate(date=d, base=base, quote=quote, rate=rate, source=source)
    )
    session.commit()


# ---------------------------------------------------------------------------
# qty_trajectory — pure helper
# ---------------------------------------------------------------------------


class TestQtyTrajectory:
    def test_empty_trades_empty_dates(self):
        result = _positions.qty_trajectory(trades=[], dates=[])
        assert result == {}

    def test_single_buy_carries_forward(self):
        d1, d2, d3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
        trades = [
            _trade_tuple(d=d1, code="2330", side=CASH_BUY, qty=100),
        ]
        result = _positions.qty_trajectory(trades, [d1, d2, d3])
        assert result[(d1, "2330")] == 100
        assert result[(d2, "2330")] == 100
        assert result[(d3, "2330")] == 100

    def test_buy_then_sell(self):
        d1, d2, d3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
        trades = [
            _trade_tuple(d=d1, code="2330", side=CASH_BUY, qty=100),
            _trade_tuple(d=d2, code="2330", side=CASH_SELL, qty=40),
        ]
        result = _positions.qty_trajectory(trades, [d1, d2, d3])
        assert result[(d1, "2330")] == 100
        assert result[(d2, "2330")] == 60
        assert result[(d3, "2330")] == 60

    def test_full_exit_yields_zero_qty(self):
        d1, d2 = date(2026, 1, 1), date(2026, 1, 2)
        trades = [
            _trade_tuple(d=d1, code="2330", side=CASH_BUY, qty=100),
            _trade_tuple(d=d2, code="2330", side=CASH_SELL, qty=100),
        ]
        result = _positions.qty_trajectory(trades, [d1, d2])
        assert result[(d1, "2330")] == 100
        assert result[(d2, "2330")] == 0

    def test_dates_before_first_trade_have_no_position(self):
        d1, d2 = date(2026, 1, 1), date(2026, 1, 2)
        trades = [_trade_tuple(d=d2, code="2330", side=CASH_BUY, qty=100)]
        result = _positions.qty_trajectory(trades, [d1, d2])
        assert (d1, "2330") not in result
        assert result[(d2, "2330")] == 100

    def test_multiple_codes_independent(self):
        d = date(2026, 1, 1)
        trades = [
            _trade_tuple(d=d, code="2330", side=CASH_BUY, qty=100),
            _trade_tuple(d=d, code="2317", side=CASH_BUY, qty=50),
        ]
        result = _positions.qty_trajectory(trades, [d])
        assert result[(d, "2330")] == 100
        assert result[(d, "2317")] == 50


# ---------------------------------------------------------------------------
# forward_fill — pure helper
# ---------------------------------------------------------------------------


class TestForwardFill:
    def test_empty_rows_returns_empty(self):
        assert _positions.forward_fill([], [date(2026, 1, 1)]) == {}

    def test_carries_recent_value_forward(self):
        d1, d2, d3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
        rows = [(d1, Decimal("100"))]
        result = _positions.forward_fill(rows, [d1, d2, d3])
        assert result == {d1: Decimal("100"), d2: Decimal("100"), d3: Decimal("100")}

    def test_pre_first_row_dates_use_earliest(self):
        # If a target date precedes every row, fall back to the
        # earliest-known value (better than dropping the position).
        d0, d1 = date(2025, 12, 31), date(2026, 1, 1)
        rows = [(d1, Decimal("100"))]
        result = _positions.forward_fill(rows, [d0, d1])
        assert result[d0] == Decimal("100")
        assert result[d1] == Decimal("100")

    def test_rows_outside_target_window_ignored(self):
        d1, d2 = date(2026, 1, 1), date(2026, 1, 2)
        rows = [
            (d1, Decimal("100")),
            (date(2026, 6, 1), Decimal("999")),
        ]
        result = _positions.forward_fill(rows, [d1, d2])
        assert result == {d1: Decimal("100"), d2: Decimal("100")}

    def test_value_changes_at_each_row(self):
        d1, d2, d3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
        rows = [(d1, Decimal("100")), (d3, Decimal("200"))]
        result = _positions.forward_fill(rows, [d1, d2, d3])
        assert result[d1] == Decimal("100")
        assert result[d2] == Decimal("100")
        assert result[d3] == Decimal("200")


# ---------------------------------------------------------------------------
# build_daily — orchestrator + persistence
# ---------------------------------------------------------------------------


class TestBuildDailyEmpty:
    def test_no_trades_no_prices_writes_nothing(self, session):
        result = _positions.build_daily(
            session, start=date(2026, 1, 1), end=date(2026, 1, 31)
        )
        assert result == {"positions_rows": 0, "portfolio_rows": 0}

    def test_trades_but_no_prices_writes_nothing(self, session):
        _add_trade(
            session,
            d=date(2026, 1, 1),
            code="2330",
            side=CASH_BUY,
            qty=100,
            price=Decimal("500"),
        )
        result = _positions.build_daily(
            session, start=date(2026, 1, 1), end=date(2026, 1, 31)
        )
        # No priced dates → no rows. Engine doesn't synthesize a curve
        # from trades alone; without prices we don't know MV.
        assert result == {"positions_rows": 0, "portfolio_rows": 0}


class TestBuildDailySingleTW:
    def test_one_buy_one_priced_day(self, session):
        d = date(2026, 1, 5)
        _add_trade(
            session, d=d, code="2330", side=CASH_BUY,
            qty=100, price=Decimal("500"),
        )
        _add_price(session, d=d, symbol="2330", close=Decimal("510"))

        result = _positions.build_daily(session, start=d, end=d)
        assert result["positions_rows"] == 1
        assert result["portfolio_rows"] == 1

        rows = session.query(PositionDaily).all()
        assert len(rows) == 1
        assert rows[0].code == "2330"
        assert rows[0].qty == 100
        assert rows[0].close == Decimal("510")
        assert rows[0].market_value == Decimal("51000")
        assert rows[0].currency == "TWD"

        port = session.query(PortfolioDaily).all()
        assert len(port) == 1
        assert port[0].equity == Decimal("51000")
        assert port[0].currency == "TWD"

    def test_zero_qty_dates_skip_position_row(self, session):
        d_buy = date(2026, 1, 5)
        d_sell = date(2026, 1, 6)
        _add_trade(
            session, d=d_buy, code="2330", side=CASH_BUY,
            qty=100, price=Decimal("500"),
        )
        _add_trade(
            session, d=d_sell, code="2330", side=CASH_SELL,
            qty=100, price=Decimal("520"),
        )
        _add_price(session, d=d_buy, symbol="2330", close=Decimal("500"))
        _add_price(session, d=d_sell, symbol="2330", close=Decimal("520"))

        _positions.build_daily(session, start=d_buy, end=d_sell)

        positions = session.query(PositionDaily).order_by(
            PositionDaily.date
        ).all()
        # Only buy day has a position; sell day fully exits.
        assert len(positions) == 1
        assert positions[0].date == d_buy
        assert positions[0].qty == 100


class TestBuildDailyForeign:
    def test_foreign_position_converts_via_fx(self, session):
        d = date(2026, 1, 5)
        _add_trade(
            session, d=d, code="AAPL", side=CASH_BUY,
            qty=10, price=Decimal("180"),
            currency="USD", venue="US",
        )
        _add_price(
            session, d=d, symbol="AAPL", close=Decimal("190"),
            currency="USD",
        )
        _add_fx(
            session, d=d, base="USD", quote="TWD", rate=Decimal("31.5"),
        )

        _positions.build_daily(session, start=d, end=d)

        positions = session.query(PositionDaily).all()
        assert len(positions) == 1
        assert positions[0].currency == "USD"
        assert positions[0].market_value == Decimal("1900")  # 10 * 190 USD

        port = session.query(PortfolioDaily).all()
        assert len(port) == 1
        # 10 * 190 USD * 31.5 TWD/USD = 59850 TWD
        assert port[0].equity == Decimal("59850.00")

    def test_missing_fx_skips_foreign_position(self, session):
        # A foreign position without an FX rate can't be converted;
        # rather than write a wrong TWD equity, exclude it from the
        # PortfolioDaily aggregate. The PositionDaily row still gets
        # written in the local currency for audit visibility.
        d = date(2026, 1, 5)
        _add_trade(
            session, d=d, code="AAPL", side=CASH_BUY,
            qty=10, price=Decimal("180"),
            currency="USD", venue="US",
        )
        _add_price(
            session, d=d, symbol="AAPL", close=Decimal("190"),
            currency="USD",
        )
        # No FX row for USD on this date.

        _positions.build_daily(session, start=d, end=d)

        positions = session.query(PositionDaily).all()
        assert len(positions) == 1  # local-currency row preserved

        port = session.query(PortfolioDaily).all()
        # No portfolio row when the only position can't be FX-converted.
        assert len(port) == 0


class TestBuildDailyMixed:
    def test_tw_plus_foreign_aggregate(self, session):
        d = date(2026, 1, 5)
        # TW: 100 * 500 = 50000 TWD
        _add_trade(
            session, d=d, code="2330", side=CASH_BUY,
            qty=100, price=Decimal("500"),
        )
        _add_price(session, d=d, symbol="2330", close=Decimal("500"))
        # Foreign: 10 * 180 USD * 30 = 54000 TWD
        _add_trade(
            session, d=d, code="AAPL", side=CASH_BUY,
            qty=10, price=Decimal("180"),
            currency="USD", venue="US",
        )
        _add_price(
            session, d=d, symbol="AAPL", close=Decimal("180"),
            currency="USD",
        )
        _add_fx(session, d=d, base="USD", quote="TWD", rate=Decimal("30"))

        _positions.build_daily(session, start=d, end=d)

        port = session.query(PortfolioDaily).all()
        assert len(port) == 1
        assert port[0].equity == Decimal("104000.00")  # 50000 + 54000


class TestBuildDailyForwardFill:
    def test_price_forward_filled_for_symbol_missing_on_some_priced_days(
        self, session
    ):
        # Forward-fill handles per-symbol gaps within priced_dates: if
        # ANY symbol has a Price row on day d, d is iterated; symbols
        # silent that day inherit their last-known close. If NO symbol
        # has a price on d, the day is not iterated at all (legacy
        # semantics — equity curve is undefined on truly-priceless days).
        d1, d2, d3 = date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)
        _add_trade(
            session, d=d1, code="2330", side=CASH_BUY,
            qty=100, price=Decimal("500"),
        )
        _add_trade(
            session, d=d1, code="2317", side=CASH_BUY,
            qty=200, price=Decimal("100"),
        )
        # 2330 priced d1 and d3; silent on d2.
        _add_price(session, d=d1, symbol="2330", close=Decimal("500"))
        _add_price(session, d=d3, symbol="2330", close=Decimal("520"))
        # 2317 priced every day — keeps d2 in priced_dates.
        _add_price(session, d=d1, symbol="2317", close=Decimal("100"))
        _add_price(session, d=d2, symbol="2317", close=Decimal("101"))
        _add_price(session, d=d3, symbol="2317", close=Decimal("102"))

        _positions.build_daily(session, start=d1, end=d3)

        rows_2330 = session.query(PositionDaily).filter_by(code="2330").all()
        # 2330 has a row on every priced day, with d2 close forward-filled.
        assert len(rows_2330) == 3
        rows_2330_sorted = sorted(rows_2330, key=lambda r: r.date)
        assert rows_2330_sorted[0].close == Decimal("500")
        assert rows_2330_sorted[1].close == Decimal("500")  # forward-filled
        assert rows_2330_sorted[2].close == Decimal("520")

    def test_fx_forward_filled_across_weekend(self, session):
        d_fri, d_mon = date(2026, 1, 9), date(2026, 1, 12)
        _add_trade(
            session, d=d_fri, code="AAPL", side=CASH_BUY,
            qty=10, price=Decimal("180"),
            currency="USD", venue="US",
        )
        _add_price(session, d=d_fri, symbol="AAPL", close=Decimal("180"), currency="USD")
        _add_price(session, d=d_mon, symbol="AAPL", close=Decimal("182"), currency="USD")
        _add_fx(session, d=d_fri, base="USD", quote="TWD", rate=Decimal("30"))
        # No FX row for d_mon — yfinance often misses Asia weekends/Mondays.

        _positions.build_daily(session, start=d_fri, end=d_mon)

        port = (
            session.query(PortfolioDaily).order_by(PortfolioDaily.date).all()
        )
        assert len(port) == 2  # both days have a portfolio row
        # Monday's equity uses Friday's FX (forward-filled).
        assert port[1].equity == Decimal("54600.00")  # 10 * 182 * 30


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _trade_tuple(d, code, side, qty):
    """Lightweight stand-in for a Trade row used by the pure
    qty_trajectory helper. The helper only needs (date, code, side, qty)
    — full Trade construction (with currency/venue/source) is overkill
    for trajectory tests."""

    class _T:
        pass

    t = _T()
    t.date = d
    t.code = code
    t.side = side
    t.qty = qty
    return t
