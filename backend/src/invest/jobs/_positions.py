"""Positions and portfolio_daily materializer.

Shared by invest.jobs.backfill (cold-start, full date range) and
invest.jobs.snapshot (incremental gap-fill from last_known_date).
Private to invest.jobs (underscore prefix) — neither analytics/ nor
the HTTP routers should reach across into this writer.

Algorithmic skeleton (logic-equivalent to the legacy
_derive_positions_and_portfolio in the retired app/backfill_runner.py,
ported to the Phase 1 schema; the production version is co-located
below):

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


# ---------------------------------------------------------------------------
# Production path — DailyStore + portfolio.json (canonical until Phase 14.4+)
# ---------------------------------------------------------------------------
#
# The functions below are the in-production daily walker — ported from
# the retired `app/backfill_runner.py` monolith (Phase 14.3c) byte-
# identically. They consume the PDF-aggregate `portfolio` dict and the
# raw-SQLite `DailyStore`, and write `positions_daily` + `portfolio_daily`
# directly via parameterized SQL. Co-exists with the SQLModel-shape
# `build_daily` above, which is the trade-table aggregator (not yet on
# the request path; see PLAN-modularization §14.4+).


def _qty_history_for_symbol(
    portfolio: dict, code: str, venue: str = "TW"
) -> list[tuple[str, float]]:
    """Return [(date, signed_qty_change), ...] for one symbol on `venue`,
    sorted by date.

    Buys add positive qty; sells subtract. Foreign trades use 買進/賣出
    while TW trades use 普買/普賣 — both contain "買"/"賣" so the same
    sign rule works.
    """
    # Local import to avoid circular dependency with invest.jobs.backfill,
    # which itself uses the production helpers in this module.
    from invest.jobs.backfill import _normalize_trade_date

    out: list[tuple[str, float]] = []
    for t in portfolio.get("summary", {}).get("all_trades", []):
        if t.get("venue") != venue or t.get("code") != code:
            continue
        d = _normalize_trade_date(t["date"])
        side = t.get("side", "")
        qty = float(t.get("qty", 0) or 0)
        sign = 1 if "買" in side else -1
        out.append((d, sign * qty))
    out.sort(key=lambda r: r[0])
    return out


def _qty_per_priced_date_for_symbol(
    portfolio: dict,
    code: str,
    venue: str,
    priced_dates: list[str],
    overlay_deltas: list[tuple[str, float]] | None = None,
) -> dict[str, float]:
    """Map each priced date → qty held that day, anchored to PDF holdings.

    Pure trade summation breaks under stock splits — e.g. 00631L's pre-split
    trade ledger sums to 210 shares while the post-split March holding row
    is 4620 (a ~1:22 split). The PDF holdings table reflects post-split qty,
    so we use it as the anchor in split months and fall back to prior-anchor +
    intra-month trade deltas elsewhere.

    Algorithm per priced date d:
      base_qty = qty_at_(prior_PDF_month_end) + Σ trades in (prior_me, d]
      If d's month has its own PDF anchor:
          expected = qty_at_(prior_PDF_month_end) + Σ all_trades_in_month
          If anchor >> expected (≥1.5×): split was implied; use anchor for
              every day in the month (the price-side handles pre-split
              scaling separately).
          Otherwise: stick with base_qty so mid-month buys don't appear
              prematurely (e.g. 00991A's Feb 5 buy doesn't inflate Feb 2's V).
      No PDF month for d: fall back to base_qty; for dates that precede
      every snapshot (intra-month round-trips that never closed at a
      month-end), sum trades from the start.
    """
    from invest.jobs.backfill import month_end_iso

    venue_key = "tw" if venue == "TW" else "foreign"
    anchors_by_month: dict[str, float] = {}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        if not ym:
            continue
        for h in m.get(venue_key, {}).get("holdings", []):
            if h.get("code") == code:
                qty = float(h.get("qty", 0) or 0)
                if qty > 0:
                    anchors_by_month[ym] = qty
                break

    deltas = _qty_history_for_symbol(portfolio, code, venue)
    if overlay_deltas:
        # Bug 2 follow-up (2026-05-01): fold trades_overlay deltas in so a
        # code held in PDF (e.g. 00981A) reflects post-PDF buys/sells in
        # its daily qty. Without this, mv stays at the PDF anchor while
        # cash correctly tracks the overlay activity, double-counting
        # equity by the inflated mv.
        deltas = sorted(deltas + list(overlay_deltas), key=lambda r: r[0])
    sorted_yms = sorted(anchors_by_month)

    def _prior_ym_for(target_ym: str) -> str | None:
        prior: str | None = None
        for ym in sorted_yms:
            if ym < target_ym:
                prior = ym
            else:
                break
        return prior

    out: dict[str, float] = {}
    for d in priced_dates:
        d_ym = d[:7]
        prior_ym = _prior_ym_for(d_ym)
        if prior_ym is None:
            base_qty = sum(q for td, q in deltas if td <= d)
        else:
            prior_me = month_end_iso(prior_ym)
            base_qty = anchors_by_month[prior_ym] + sum(
                q for td, q in deltas if prior_me < td <= d
            )
        base_qty = max(0.0, base_qty)

        if d_ym in anchors_by_month:
            anchor_qty = anchors_by_month[d_ym]
            month_me = month_end_iso(d_ym)
            prior_me_or_zero = (
                month_end_iso(prior_ym) if prior_ym else "0000-00-00"
            )
            intra_total = sum(
                q for td, q in deltas if prior_me_or_zero < td <= month_me
            )
            expected = (anchors_by_month[prior_ym] if prior_ym else 0.0) + intra_total
            if expected > 1 and anchor_qty > expected * 1.5:
                # Split detected — month-end anchor wins; price-side scales
                # pre-split-day closes elsewhere in the pipeline.
                out[d] = anchor_qty
                continue
        out[d] = base_qty
    return out


def _forward_fill_fx(
    fx_rows: list[tuple[str, float]], dates: Iterable[str]
) -> dict[str, float]:
    """Build a {date → rate} map across `dates` by carrying the most-recent
    rate forward (yfinance returns stale `TWD=X` rows on Asia weekends, so
    a price-day can land on a no-FX date).

    `fx_rows` must be sorted by date asc.
    """
    out: dict[str, float] = {}
    if not fx_rows:
        return out
    sorted_dates = sorted(set(dates))
    fx_idx = 0
    last_rate: float | None = None
    for d in sorted_dates:
        while fx_idx < len(fx_rows) and fx_rows[fx_idx][0] <= d:
            last_rate = fx_rows[fx_idx][1]
            fx_idx += 1
        if last_rate is None:
            # Fall back: scan ahead for the earliest rate (handles dates
            # before the first fx row, e.g. the very start of the curve).
            for fd, fr in fx_rows:
                if fd >= d:
                    last_rate = fr
                    break
        if last_rate is not None:
            out[d] = last_rate
    return out


def _derive_positions_and_portfolio(
    store, portfolio: dict
) -> dict[str, int]:
    """Walk every priced trading day, compute end-of-day qty per symbol
    from the trade ledger, multiply by close → mv_local, convert foreign
    via fx_rates (forward-fill on weekend gaps), and aggregate to
    portfolio_daily.equity_twd.
    """
    from invest.jobs.backfill import (
        _normalize_trade_date,
        iter_foreign_symbols_with_metadata,
        iter_tw_symbols_with_metadata,
        month_end_iso,
    )

    tw_codes = [e["code"] for e in iter_tw_symbols_with_metadata(portfolio)]
    foreign_codes: list[str] = []
    foreign_currency: dict[str, str] = {}
    # Per-share avg cost from latest PDF month that held the code. Used at
    # write time as `cost_local = qty × avg_cost_at[code]` so the stored
    # cost_local matches the schema convention (total cost in local ccy)
    # that every reader (analytics.py, holdings.py, tickers.py) assumes.
    avg_cost_at: dict[str, float] = {}

    for entry in iter_foreign_symbols_with_metadata(portfolio):
        foreign_codes.append(entry["code"])
        foreign_currency[entry["code"]] = entry["currency"]

    # ref_price_by_month_code = month-end MV-per-share from holdings, used
    # as a last-resort price when neither yfinance nor forward-fill produce
    # a close. Without this, symbols whose daily fetch fails (DLQ candidates)
    # silently drop out of daily V — the daily equity curve under-tracks
    # the true portfolio for those holdings.
    ref_price_by_month_code: dict[str, dict[str, float]] = {"tw": {}, "foreign": {}}
    for m in portfolio.get("months", []):
        ym = m.get("month")
        for h in m.get("tw", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = float(h.get("qty", 0) or 0)
            if qty > 0:
                avg = h.get("avg_cost")
                if avg is None:
                    avg = float(h.get("cost", 0) or 0) / qty
                avg_cost_at[code] = float(avg or 0)
            ref = h.get("ref_price")
            if ref and ym:
                ref_price_by_month_code["tw"][f"{code}|{ym}"] = float(ref)
        for h in m.get("foreign", {}).get("holdings", []):
            code = h.get("code")
            if not code:
                continue
            qty = float(h.get("qty", 0) or 0)
            if qty > 0:
                # Foreign rows often have avg_cost_local=None but carry a
                # canonical total `cost` field — derive avg from cost/qty
                # to avoid storing 0 (which would render as 100% unrealized).
                avg = h.get("avg_cost_local")
                if avg is None:
                    avg = float(h.get("cost", 0) or 0) / qty
                avg_cost_at[code] = float(avg or 0)
            close_l = h.get("close")
            if close_l and ym:
                ref_price_by_month_code["foreign"][f"{code}|{ym}"] = float(close_l)

    # Stale-overlay cleanup (single-writer architecture, 2026-05-01).
    # The per-day UPSERT below uses WHERE source='pdf' so PDF writes
    # don't clobber overlay rows during the gap window. The downside:
    # once a new PDF month lands and the gap shrinks, overlay rows for
    # now-covered dates would persist forever. Clear them up-front so
    # PDFs become canonical for any date the latest PDF month-end now
    # reaches.
    months = sorted(
        (m.get("month") for m in portfolio.get("months", []) if m.get("month")),
    )
    if months:
        latest_pdf_month_end = month_end_iso(months[-1])
        with store.connect_rw() as conn:
            conn.execute(
                "DELETE FROM positions_daily "
                "WHERE source = 'overlay' AND date <= ?",
                (latest_pdf_month_end,),
            )

    with store.connect_ro() as conn:
        priced_dates = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT date FROM prices ORDER BY date"
            ).fetchall()
        ]
        all_prices = {
            (r[0], r[1]): r[2]
            for r in conn.execute(
                "SELECT date, symbol, close FROM prices"
            ).fetchall()
        }
        fx_by_ccy: dict[str, list[tuple[str, float]]] = {}
        # Phase 14.3b: read SQLModel-canonical `fx_rates` (base/quote/rate)
        # but alias columns so the in-Python aggregator below keeps using
        # ``ccy`` / ``rate_to_twd``.
        for r in conn.execute(
            "SELECT base AS ccy, date, rate AS rate_to_twd FROM fx_rates "
            "WHERE quote = 'TWD' ORDER BY base, date"
        ).fetchall():
            fx_by_ccy.setdefault(r[0], []).append((r[1], float(r[2])))

    if not priced_dates:
        return {"positions_rows": 0, "portfolio_rows": 0}

    fx_filled: dict[str, dict[str, float]] = {
        ccy: _forward_fill_fx(rows, priced_dates) for ccy, rows in fx_by_ccy.items()
    }

    # Forward-fill closes across priced_dates. Two distinct gap sources:
    #   • foreign symbols: yfinance is silent on US holidays/weekends but
    #     those can still be TW trading days (and vice versa).
    #   • TW symbols: yfinance occasionally returns no rows for a date
    #     (thin-volume NaN, network blip), leaving holes in the window.
    # Without forward-fill, holdings *vanish from V* on every gap day and
    # the daily equity curve gyrates wildly (e.g. r_d = +1274% when 16
    # symbols re-appear on the next priced day). Carrying the most-recent
    # close forward keeps V continuous.
    def _build_filled(codes: list[str]) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for code in codes:
            rows_for_code = sorted(
                (d, c) for (d, sym), c in all_prices.items() if sym == code
            )
            out[code] = _forward_fill_fx(rows_for_code, priced_dates)
        return out

    tw_close_filled = _build_filled(tw_codes)
    foreign_close_filled = _build_filled(foreign_codes)

    # Stock splits cause a sudden mismatch between PDF holdings qty
    # (post-split) and yfinance close (pre-split for days before the
    # split date). e.g. 00631L Feb→Mar 2026 went 220→4620 shares while
    # the underlying price dropped ~22×. Without adjustment, daily MV
    # spikes to (4620 × pre-split 530) ≈ 2.45M, then crashes back at
    # month-end. Detect the split factor from holdings, find the split
    # day from prices, and rescale pre-split closes to the post-split scale.
    def _split_adjusted(
        venue: str, code: str, filled: dict[str, float]
    ) -> dict[str, float]:
        venue_key = "tw" if venue == "TW" else "foreign"
        anchors: list[tuple[str, float]] = []
        for m in portfolio.get("months", []):
            ym = m.get("month")
            for h in m.get(venue_key, {}).get("holdings", []):
                if h.get("code") == code:
                    qty = float(h.get("qty", 0) or 0)
                    if qty > 0 and ym:
                        anchors.append((ym, qty))
                    break
        anchors.sort()
        deltas = _qty_history_for_symbol(portfolio, code, venue)
        adjusted = dict(filled)
        for i in range(1, len(anchors)):
            prev_ym, prev_qty = anchors[i - 1]
            curr_ym, curr_qty = anchors[i]
            prev_me = month_end_iso(prev_ym)
            curr_me = month_end_iso(curr_ym)
            intra = sum(q for td, q in deltas if prev_me < td <= curr_me)
            expected = prev_qty + intra
            if expected <= 1 or curr_qty <= expected * 1.5:
                continue
            split_factor = curr_qty / expected
            month_prices = sorted(
                (d, adjusted[d]) for d in adjusted
                if d[:7] == curr_ym and adjusted[d] > 0
            )
            split_day = None
            target = 1.0 / split_factor
            for j in range(1, len(month_prices)):
                d_prev, c_prev = month_prices[j - 1]
                d_curr, c_curr = month_prices[j]
                if c_prev <= 0:
                    continue
                ratio = c_curr / c_prev
                if abs(ratio - target) / target < 0.15:
                    split_day = d_curr
                    break
            if split_day is None:
                # No price-drop signal — assume the split landed on the
                # month-end (worst case scales the whole month, which still
                # beats letting MV spike 22×).
                split_day = curr_me
            # Only days WITHIN the split month and BEFORE the split day
            # need price-scaling. PDF holdings for prior months carry
            # pre-split qty already, so their MV (pre-split qty × pre-split
            # close) is correct as-is — no scaling needed there.
            for d in list(adjusted.keys()):
                if d[:7] == curr_ym and d < split_day:
                    adjusted[d] = adjusted[d] / split_factor
        return adjusted

    tw_close_filled = {
        code: _split_adjusted("TW", code, filled)
        for code, filled in tw_close_filled.items()
    }
    foreign_close_filled = {
        code: _split_adjusted("Foreign", code, filled)
        for code, filled in foreign_close_filled.items()
    }

    # Pull overlay trade deltas once and group by code so the per-symbol
    # qty walk picks up post-PDF activity for codes that ALSO appear in
    # PDF holdings (e.g., 00981A bought in March, traded heavily in April).
    # Without this, the PDF qty stays as the anchor for every gap date
    # and mv ignores intra-gap rotations.
    overlay_deltas_by_code: dict[str, list[tuple[str, float]]] = {}
    with store.connect_ro() as conn:
        for r in conn.execute(
            "SELECT date, code, side, qty FROM trades_overlay"
        ).fetchall():
            sign = 1 if "買" in (r["side"] or "") else -1
            overlay_deltas_by_code.setdefault(r["code"], []).append(
                (r["date"], sign * float(r["qty"]))
            )

    # Pre-compute per-day qty per symbol once — anchored to PDF holdings so
    # stock splits don't drift the daily share count.
    tw_qty_by_date = {
        code: _qty_per_priced_date_for_symbol(
            portfolio, code, "TW", priced_dates,
            overlay_deltas=overlay_deltas_by_code.get(code),
        )
        for code in tw_codes
    }
    foreign_qty_by_date = {
        code: _qty_per_priced_date_for_symbol(portfolio, code, "Foreign", priced_dates)
        for code in foreign_codes
    }

    # Synthesized broker-cash schedule. The daily layer has no source for
    # broker cash balances, so we approximate: anchor at 0 on the first
    # priced day and accumulate trade.net_twd as we walk forward. Buys make
    # net_twd negative (cash leaves broker → buys position); sells make it
    # positive (cash credited). Without this offset, equity_twd plunges on
    # rotation days because the MV change from a sale isn't matched by the
    # cash credit. With it, mv − net_twd stays conserved across a buy/sell
    # pair (modulo fees, which are real costs).
    #
    # Caveats: external bank↔broker transfers aren't dated daily, so deposit
    # days appear flat instead of jumping. Dividend credits and broker-side
    # fees outside trades are also missing. Within a month with no external
    # flows this is exact for rotations.
    #
    # Bug 2 fix (2026-05-01): post-PDF overlay trades now contribute to
    # running_cash_twd via the trades_overlay table. Without this, an
    # overlay sell debited mv but never credited cash, so the equity curve
    # dropped artificially on rotation days the user sold via broker.
    pdf_trades = [
        (_normalize_trade_date(t["date"]), float(t.get("net_twd") or 0))
        for t in portfolio.get("summary", {}).get("all_trades", [])
    ]
    with store.connect_ro() as conn:
        overlay_trades = [
            (r["date"], float(r["net_twd"] or 0))
            for r in conn.execute(
                "SELECT date, net_twd FROM trades_overlay"
            ).fetchall()
        ]
    trades_chrono: list[tuple[str, float]] = sorted(
        pdf_trades + overlay_trades,
        key=lambda r: r[0],
    )

    n_positions = 0
    n_portfolio = 0
    with store.connect_rw() as conn:
        trade_idx = 0
        running_cash_twd = 0.0
        for d in priced_dates:
            while (
                trade_idx < len(trades_chrono)
                and trades_chrono[trade_idx][0] <= d
            ):
                running_cash_twd += trades_chrono[trade_idx][1]
                trade_idx += 1

            day_fx_usd = fx_filled.get("USD", {}).get(d, 0.0)

            # Single-writer architecture (2026-05-01):
            #   1. Write PDF rows but DO NOT overwrite existing overlay
            #      rows (the WHERE source='pdf' guard on UPDATE). During
            #      the gap window, overlay carries the augmented qty
            #      (e.g. user bought MORE of an existing PDF holding) —
            #      PDF's roll-forward qty is stale by definition there.
            #   2. After all PDF writes, SUM positions_daily for the
            #      day. This sees both PDF rows we just wrote AND any
            #      overlay rows merge persisted earlier in this run.
            #      Single source of truth for portfolio_daily.equity_twd.
            #
            # Stale-overlay cleanup: when a new PDF month lands and the
            # gap shrinks, overlay rows for now-covered dates would
            # persist forever under this guard. Cleared up-front via
            # _clear_stale_overlay_rows() below (called once per derive
            # run, before this loop).

            # TW positions — local == TWD, mv_twd == mv_local
            for code, qty_by_date in tw_qty_by_date.items():
                qty = qty_by_date.get(d, 0.0)
                if qty <= 0:
                    # Position fully exited (e.g., overlay sells exceeded
                    # PDF qty for 042900 — broker sold odd-lot remainder
                    # PDF parser missed). Delete any stale PDF row so the
                    # holdings reader doesn't surface a phantom position.
                    conn.execute(
                        "DELETE FROM positions_daily "
                        "WHERE date = ? AND symbol = ? AND source = 'pdf'",
                        (d, code),
                    )
                    continue
                close = tw_close_filled.get(code, {}).get(d)
                if close is None:
                    # Last resort: PDF-month ref_price for the month of d.
                    # NOTE — same-month-only fallback. If a symbol is held
                    # mid-month but exits before the next month-end, it is
                    # absent from that month's holdings table, so this lookup
                    # returns None and the row is skipped below — silently
                    # under-counting equity_twd for the holding window.
                    # Rare in practice now that TW prices route through
                    # yfinance, but if a regression brings the fallback
                    # back into play, walk back through prior months'
                    # anchors instead of giving up at d[:7].
                    close = ref_price_by_month_code["tw"].get(f"{code}|{d[:7]}")
                if close is None:
                    continue
                mv_local = qty * close
                # Total cost in local ccy (TWD for TW). Convention: every
                # reader treats positions_daily.cost_local as total, not
                # per-share — matching analytics.py:790 / holdings.py:140.
                cost_local = qty * avg_cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    WHERE positions_daily.source = 'pdf'
                    """,
                    (d, code, qty, cost_local, mv_local, mv_local, "現股", "pdf"),
                )
                n_positions += 1

            # Foreign positions — convert mv_local via fx_filled
            for code, qty_by_date in foreign_qty_by_date.items():
                qty = qty_by_date.get(d, 0.0)
                if qty <= 0:
                    conn.execute(
                        "DELETE FROM positions_daily "
                        "WHERE date = ? AND symbol = ? AND source = 'pdf'",
                        (d, code),
                    )
                    continue
                close = foreign_close_filled.get(code, {}).get(d)
                if close is None:
                    close = ref_price_by_month_code["foreign"].get(f"{code}|{d[:7]}")
                if close is None:
                    continue
                ccy = foreign_currency.get(code, "USD")
                fx = fx_filled.get(ccy, {}).get(d)
                if fx is None or fx == 0:
                    # No FX for this day — skip rather than write a wrong
                    # mv_twd. portfolio_daily for this date may end up with
                    # only TW positions, which is the correct degraded state.
                    continue
                mv_local = qty * close
                mv_twd = mv_local * fx
                cost_local = qty * avg_cost_at.get(code, 0.0)
                conn.execute(
                    """
                    INSERT INTO positions_daily(
                        date, symbol, qty, cost_local, mv_local, mv_twd, type, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(date, symbol) DO UPDATE SET
                        qty = excluded.qty,
                        cost_local = excluded.cost_local,
                        mv_local = excluded.mv_local,
                        mv_twd = excluded.mv_twd,
                        type = excluded.type,
                        source = excluded.source
                    WHERE positions_daily.source = 'pdf'
                    """,
                    (d, code, qty, cost_local, mv_local, mv_twd, "foreign", "pdf"),
                )
                n_positions += 1

            # Authoritative day-aggregate: SUM positions_daily.mv_twd
            # over BOTH sources. PRIMARY KEY (date, symbol) ensures we
            # never double-count; the WHERE source='pdf' guard above
            # ensures overlay rows survive intact during the gap window.
            agg = conn.execute(
                "SELECT COALESCE(SUM(mv_twd), 0), COUNT(*), "
                "MAX(CASE WHEN source = 'overlay' THEN 1 ELSE 0 END) "
                "FROM positions_daily WHERE date = ?",
                (d,),
            ).fetchone()
            day_mv_twd = float(agg[0] or 0.0)
            day_n = int(agg[1] or 0)
            day_has_overlay = int(agg[2] or 0)

            if day_n == 0 and running_cash_twd == 0.0:
                continue
            day_equity_twd = day_mv_twd + running_cash_twd
            conn.execute(
                """
                INSERT INTO portfolio_daily(
                    date, equity_twd, cash_twd, fx_usd_twd, n_positions, has_overlay
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date) DO UPDATE SET
                    equity_twd = excluded.equity_twd,
                    cash_twd = excluded.cash_twd,
                    fx_usd_twd = excluded.fx_usd_twd,
                    n_positions = excluded.n_positions,
                    has_overlay = excluded.has_overlay
                """,
                (d, day_equity_twd, running_cash_twd, day_fx_usd, day_n,
                 day_has_overlay),
            )
            n_portfolio += 1

    return {"positions_rows": n_positions, "portfolio_rows": n_portfolio}
