"""Positions and portfolio_daily materializer.

Shared by invest.jobs.backfill (cold-start, full date range) and
invest.jobs.snapshot (incremental gap-fill from last_known_date).
Private to invest.jobs (underscore prefix) — neither analytics/ nor
the HTTP routers should reach across into this writer.

Algorithmic skeleton (logic-equivalent to the legacy
_derive_positions_and_portfolio in app/backfill_runner.py, ported to
the Phase 1 schema):

    1. Walk priced dates (intersection of [start, end] and the dates
       for which any Price row exists).
    2. For each date d:
       - qty(d, code) = running sum of trade qty signed by side, up
         to and including d.
       - close(d, code) = forward-filled Price.close (gap-tolerant
         across yfinance silence days).
       - position MV = qty * close in the symbol's local currency.
       - PortfolioDaily.equity = sum of position MVs converted to
         the reporting currency (TWD by default) via forward-filled
         FX rates.
    3. Persist a PositionDaily per (date, code) where qty > 0 and a
       PortfolioDaily per date where the aggregate is computable.

Deferred from legacy (TODO, not silently dropped):
  - Stock split detection. Legacy used PDF-anchor signal; new world
    has Trade rows only — algorithm shifts to close-ratio inspection.
  - Overlay merge. Phase 5 made Trade the single source of truth.
  - ref_price fallback. Forward-fill below covers most real cases.
"""
from __future__ import annotations

from datetime import date as _date
from decimal import Decimal
from typing import Iterable

from sqlmodel import Session, select

from invest.persistence.models.fx_rate import FxRate
from invest.persistence.models.portfolio_daily import PortfolioDaily
from invest.persistence.models.position_daily import PositionDaily
from invest.persistence.models.price import Price
from invest.persistence.models.trade import Trade

# Side encoding (mirrors invest.domain.trade.Side IntEnum).
_BUY_SIDES = frozenset({1, 11, 22})  # CASH_BUY, MARGIN_BUY, SHORT_COVER


def qty_trajectory(
    trades: Iterable, dates: Iterable[_date]
) -> dict[tuple[_date, str], int]:
    """Per (date, code) running qty from the trade ledger.

    Buys add; sells subtract. Dates that precede the code's first
    trade get no entry.
    """
    deltas_by_code: dict[str, list[tuple[_date, int]]] = {}
    for t in trades:
        sign = 1 if t.side in _BUY_SIDES else -1
        deltas_by_code.setdefault(t.code, []).append((t.date, sign * t.qty))

    sorted_dates = sorted(dates)
    out: dict[tuple[_date, str], int] = {}
    for code, deltas in deltas_by_code.items():
        deltas.sort(key=lambda r: r[0])
        idx = 0
        running = 0
        for d in sorted_dates:
            while idx < len(deltas) and deltas[idx][0] <= d:
                running += deltas[idx][1]
                idx += 1
            if idx == 0:
                continue
            out[(d, code)] = running
    return out


def forward_fill(
    rows: list[tuple[_date, Decimal]],
    dates: Iterable[_date],
) -> dict[_date, Decimal]:
    """Build {date → value} carrying the most-recent value forward.

    Pre-first-row dates fall back to the earliest known value rather
    than dropping the position — the legacy implementation made the
    same call to keep the equity curve continuous through start-of-
    range FX/price gaps.
    """
    if not rows:
        return {}
    sorted_rows = sorted(rows, key=lambda r: r[0])
    sorted_dates = sorted(set(dates))
    out: dict[_date, Decimal] = {}
    idx = 0
    last: Decimal | None = None
    for d in sorted_dates:
        while idx < len(sorted_rows) and sorted_rows[idx][0] <= d:
            last = sorted_rows[idx][1]
            idx += 1
        if last is None:
            last = sorted_rows[0][1]
        out[d] = last
    return out


def build_daily(
    session: Session,
    start: _date,
    end: _date,
    *,
    reporting_currency: str = "TWD",
    source: str = "computed",
) -> dict[str, int]:
    """Compute and persist PositionDaily + PortfolioDaily for every
    priced date in [start, end].

    Returns the row counts written.
    """
    # Trades before `start` still affect qty in the window — pull all.
    earlier_trades = list(
        session.exec(select(Trade).where(Trade.date < start)).all()
    )
    in_window_trades = list(
        session.exec(
            select(Trade).where(Trade.date >= start, Trade.date <= end)
        ).all()
    )
    all_trades = earlier_trades + in_window_trades

    priced_rows = list(
        session.exec(
            select(Price).where(Price.date >= start, Price.date <= end)
        ).all()
    )
    if not priced_rows:
        return {"positions_rows": 0, "portfolio_rows": 0}

    priced_dates = sorted({p.date for p in priced_rows})

    closes_by_symbol: dict[str, list[tuple[_date, Decimal]]] = {}
    currency_by_symbol: dict[str, str] = {}
    for p in priced_rows:
        closes_by_symbol.setdefault(p.symbol, []).append((p.date, p.close))
        currency_by_symbol.setdefault(p.symbol, p.currency)
    closes_filled: dict[str, dict[_date, Decimal]] = {
        sym: forward_fill(rows, priced_dates)
        for sym, rows in closes_by_symbol.items()
    }

    needed_currencies = {
        c for c in currency_by_symbol.values() if c != reporting_currency
    }
    fx_filled: dict[str, dict[_date, Decimal]] = {}
    for ccy in needed_currencies:
        fx_rows = list(
            session.exec(
                select(FxRate)
                .where(
                    FxRate.base == ccy,
                    FxRate.quote == reporting_currency,
                    FxRate.date >= start,
                    FxRate.date <= end,
                )
            ).all()
        )
        fx_filled[ccy] = forward_fill(
            [(r.date, r.rate) for r in fx_rows], priced_dates
        )

    qty_map = qty_trajectory(all_trades, priced_dates)

    n_positions = 0
    n_portfolio = 0
    for d in priced_dates:
        held = {
            code: q
            for (date_key, code), q in qty_map.items()
            if date_key == d and q > 0
        }
        if not held:
            continue

        day_equity_reporting = Decimal("0")
        day_has_convertible = False

        for code, qty in held.items():
            close = closes_filled.get(code, {}).get(d)
            if close is None:
                continue
            ccy = currency_by_symbol.get(code, reporting_currency)
            mv_local = Decimal(qty) * close

            session.add(
                PositionDaily(
                    date=d,
                    code=code,
                    qty=qty,
                    close=close,
                    currency=ccy,
                    market_value=mv_local,
                    source=source,
                )
            )
            n_positions += 1

            if ccy == reporting_currency:
                day_equity_reporting += mv_local
                day_has_convertible = True
                continue

            fx_rate = fx_filled.get(ccy, {}).get(d)
            if fx_rate is None or fx_rate == 0:
                continue
            day_equity_reporting += mv_local * fx_rate
            day_has_convertible = True

        if day_has_convertible:
            session.add(
                PortfolioDaily(
                    date=d,
                    equity=day_equity_reporting,
                    cost_basis=Decimal("0"),  # TODO: FIFO aggregator
                    currency=reporting_currency,
                    source=source,
                )
            )
            n_portfolio += 1

    session.commit()
    return {"positions_rows": n_positions, "portfolio_rows": n_portfolio}
