"""Legacy month-dict-shaped analytics facade for the request path.

The router code consumes the month-dict / by-ticker-dict shape produced
by PortfolioStore. This module exposes those shapes while delegating
all math primitives to the principled per-metric modules:

  ratios          : sharpe, sortino, calmar, stdev, downside_stdev
  drawdown        : max_drawdown, drawdown_curve (via underwater_curve)
  concentration   : hhi, top_n_share, effective_n
  sectors         : sector_of, sector_breakdown
  attribution     : fx_pnl (via usd_exposure_walk)
  tax_pnl         : realized_pnl_by_ticker_fifo (via realized_stats_per_position)

The facade owns:
  - month-dict-shape primitives (period_returns, daily_twr, daily_external_flows,
    monthly_flows, reprice_holdings_with_daily, daily_fx_pnl, top_movers,
    recent_activity, monthly_anchored_cum, bank_cash_forward_fill, …)
  - thin float ↔ Decimal adapters at the call sites
  - the legacy 16-key shape for realized_pnl_by_ticker_fifo (Trade VOs +
    bookkeeping happen in tax_pnl.realized_stats_per_position; this module
    flattens the result back into the dashboard shape and merges per-ticker
    metadata)

Float math stays where the input shape is float (period_returns,
daily_twr, monthly_flows, reprice_holdings_with_daily, top_movers,
recent_activity, daily_fx_pnl). Math primitives that round-trip
through Decimal incur ≤ 1e-9 drift — well below dashboard rounding.
"""
from __future__ import annotations

from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Iterable, Literal

from invest.analytics import (
    attribution as _attribution,
    concentration as _concentration,
    drawdown as _drawdown,
    ratios as _ratios,
    sectors as _sectors,
    tax_pnl as _tax_pnl,
)
from invest.domain.money import Money
from invest.domain.trade import Side, Trade, Venue


def _decimals(values: Iterable[Any]) -> list[Decimal]:
    """Coerce floats/None to Decimal via str (shortest-roundtrip)."""
    return [Decimal(str(v or 0)) for v in values]


# ────────────────────────────────────────────────────────────────────────────
# Period returns — choice of flow-timing assumption
# ────────────────────────────────────────────────────────────────────────────
#
# Modified Dietz returns: r = (V_end − V_start − F) / (V_start + Σ Wᵢ·Fᵢ)
#
#   Wᵢ = (D − dᵢ) / D      where D = days in month, dᵢ = day of flow
#
# The legacy parser uses W = 0.5 (mid-month assumption). When all flows
# happen near month-end (e.g. user sells everything on the last day) the
# mid-month assumption inflates the return because it treats half the
# withdrawn capital as having been absent for the whole period.
#
# `day_weighted` is the standard fix and is the dashboard default.
# `eom` (end-of-month) is the most conservative — it assumes ALL flows
# happened on the last day, so denominator is just V_start.
# `mid_month` reproduces the legacy number.

PeriodMethod = Literal["day_weighted", "mid_month", "eom"]


def _parse_trade_day(date_str: str) -> int | None:
    """Pull the day-of-month from a YYYY/MM/DD or YYYY-MM-DD string."""
    if not date_str:
        return None
    try:
        s = date_str.replace("-", "/")
        return int(s.split("/")[2])
    except (ValueError, IndexError):
        return None


def _trade_flow_events(month: dict) -> list[tuple[int, float]]:
    """Per-trade signed flow events as (day_of_month, F_twd).

    Sign convention matches Modified Dietz F: positive = cash INTO the
    investment portfolio (i.e. a buy that uses external bank cash);
    negative = cash OUT of the portfolio (a sell that returns cash to bank).

    For TW: F_i = -net_twd (because trade.net_twd is signed from the
        client side: negative when client paid for a buy).
    For Foreign: F_i = -(net_ccy × month FX), USD-only for now.
    """
    fx = month.get("fx_usd_twd") or 0.0
    events: list[tuple[int, float]] = []
    for t in (month.get("tw") or {}).get("trades", []) or []:
        d = _parse_trade_day(t.get("date"))
        if d is None:
            continue
        events.append((d, -float(t.get("net_twd") or 0)))
    for t in (month.get("foreign") or {}).get("trades", []) or []:
        if t.get("ccy") != "USD" or not fx:
            continue
        d = _parse_trade_day(t.get("date"))
        if d is None:
            continue
        events.append((d, -float(t.get("net_ccy") or 0) * fx))
    return events


def period_returns(months: list[dict], method: PeriodMethod = "day_weighted") -> list[dict]:
    """Compute Modified-Dietz period returns under a chosen flow-timing rule.

    Returns a list of dicts: {month, period_return, v_start, weighted_flow,
    flow_total, equity_end, days_in_month, method}.
    The first month has no prior equity so V_start is back-derived as
    V_end − F and its return is forced to 0 (no info to compare against).
    """
    if not months:
        return []
    out: list[dict] = []
    for i, m in enumerate(months):
        ym = m["month"]
        v_end = float(m.get("equity_twd") or 0)
        f_total = float(m.get("external_flow_twd") or 0)
        v_start = (v_end - f_total) if i == 0 else float(months[i - 1].get("equity_twd") or 0)

        year, mon = (int(x) for x in ym.split("-"))
        days = monthrange(year, mon)[1]

        if i == 0:
            r = 0.0
            wf = 0.0
        else:
            wf = _weighted_flow_sum(m, days, f_total, method)
            denom = v_start + wf
            r = (v_end - v_start - f_total) / denom if denom > 1e-6 else 0.0

        out.append({
            "month": ym,
            "period_return": r,
            "v_start": v_start,
            "equity_end": v_end,
            "flow_total": f_total,
            "weighted_flow": wf,
            "days_in_month": days,
            "method": method,
        })
    return out


def daily_investment_flows(months: list[dict]) -> list[dict]:
    """Per-day broker-side cashflows in TWD (the F for daily TWR).

    "Investment flow" here means cash that crosses the boundary between
    the bank and the brokerage — i.e. the same definition as monthly
    `external_flow_twd`, but resolved per trade day instead of aggregated
    per month. Sign convention matches Modified Dietz F:
        F > 0  →  cash INTO the investments (a buy that drew down bank)
        F < 0  →  cash OUT of investments  (a sell that returned cash)

    Why this and not `daily_external_flows()`? Daily portfolio equity
    (`portfolio_daily.equity_twd`) is *positions-only* market value — it
    does not carry bank cash. Modified Dietz is only correct when V and
    F use consistent definitions: stocks-only V pairs with broker-side F
    (this function); stocks+cash V would pair with deposit-side F
    (`daily_external_flows`). The cashflows page uses the latter for its
    real-vs-counterfactual chart; daily TWR on Performance/Benchmark
    uses this one.

    Source: per-trade `tw.trades[]` and `foreign.trades[]`. Foreign
    trades are USD-only today (HKD/JPY would be added if those venues
    appeared) and are converted via the trade's month FX (same
    convention as monthly `external_flow_twd`).
    """
    by_date: dict[str, float] = defaultdict(float)
    for m in months:
        fx = m.get("fx_usd_twd") or 0.0
        for t in (m.get("tw") or {}).get("trades", []) or []:
            d = _normalize_iso_date(t.get("date"))
            if d is None:
                continue
            by_date[d] += -float(t.get("net_twd") or 0)
        for t in (m.get("foreign") or {}).get("trades", []) or []:
            if t.get("ccy") != "USD" or not fx:
                continue
            d = _normalize_iso_date(t.get("date"))
            if d is None:
                continue
            by_date[d] += -float(t.get("net_ccy") or 0) * fx
    return [{"date": d, "flow_twd": by_date[d]} for d in sorted(by_date)]


# Bank tx categories that are NOT external flows (capital in/out): they
# represent internal portfolio movements (dividends, settlements, FX).
# Module-level constant — recomputing this set on every call to
# daily_external_flows allocated unnecessarily during cold-start backfills
# that walk every month.
_DAILY_FLOW_EXCLUDED_CATEGORIES = frozenset({
    "tw_dividend",
    "foreign_dividend",
    "stock_settle_tw",
    "stock_settle_fx",
    "fx_convert",
})


def daily_external_flows(months: list[dict]) -> list[dict]:
    """Per-day external flow series (TWD) for daily Modified Dietz.

    "External" means: capital that crosses the boundary between you and
    the broker+bank as one system. Stock settlements, FX conversions, and
    dividends are internal rotations and are excluded — they do not change
    your invested capital, only its form.

    Source: bank ledger transactions (`m["bank"]["tx_twd"]` and
    `tx_foreign`), each carrying a `category` tag set by the parser.
    Foreign-account flows are converted to TWD using the same month FX
    that cashflows.py uses today (a per-day FX upgrade can come later via
    daily_store.get_fx_series — not blocking this implementation).

    Returns a sorted list of {date: 'YYYY-MM-DD', flow_twd: float}.
    Sign matches Modified Dietz: positive = inflow into the portfolio
    system (e.g. salary deposit), negative = outflow (e.g. transfer out).
    """
    by_date: dict[str, float] = defaultdict(float)
    for m in months:
        bank = m.get("bank") or {}
        fx = bank.get("fx") or {}
        for tx in bank.get("tx_twd", []) or []:
            if tx.get("category") in _DAILY_FLOW_EXCLUDED_CATEGORIES:
                continue
            d = _normalize_iso_date(tx.get("date"))
            if d:
                by_date[d] += float(tx.get("signed_amount") or 0)
        for tx in bank.get("tx_foreign", []) or []:
            if tx.get("category") in _DAILY_FLOW_EXCLUDED_CATEGORIES:
                continue
            d = _normalize_iso_date(tx.get("date"))
            if not d:
                continue
            ccy = tx.get("ccy") or "USD"
            rate = float(fx.get(ccy) or 0.0)
            by_date[d] += float(tx.get("signed_amount") or 0) * rate
    return [{"date": d, "flow_twd": by_date[d]} for d in sorted(by_date)]


def _normalize_iso_date(s: str | None) -> str | None:
    """'2026/03/15' or '2026-03-15' → '2026-03-15'. Returns None on garbage."""
    if not s:
        return None
    try:
        s = s.replace("/", "-")
        # validate via fromisoformat
        date.fromisoformat(s)
        return s
    except (ValueError, AttributeError):
        return None


def daily_twr(
    equity_series: list[dict],
    flow_series: list[dict],
    weight: float = 0.5,
    anchor_cum_return: float = 0.0,
) -> list[dict]:
    """Per-day Modified Dietz TWR, chained across all priced days.

    The standard daily Modified Dietz formula for one day d is:

        r_d = (V_d − V_{d-1} − F_d) / (V_{d-1} + weight · F_d)

    where:
      V_d        = end-of-day equity (TWD) for trading day d
      V_{d-1}    = end-of-day equity for the prior priced day
      F_d        = net external flow on day d (positive = inflow, signed)
      weight     = within-day flow timing assumption:
                     0.0 → flow at end of day (most conservative)
                     0.5 → flow at mid-day  (default; uniform-within-day)
                     1.0 → flow at start of day (most generous)

    Then chain across days:

        cum_twr_d = ∏_{i ≤ d} (1 + r_i) − 1

    Day 1 has no prior equity to compare against — return 0 for r_1 and
    use V_1 itself as the starting wealth_index baseline.

    `anchor_cum_return` lets callers continue an existing TWR curve into
    the daily window. Pass the monthly cum_twr at the month-end just
    before the first daily date and the day-1 wealth_index will start at
    (1 + anchor) instead of 1.0. period_return on day 1 stays 0 (no
    intra-day comparison available); only the wealth/cum baseline shifts.

    Returns one dict per equity_series row:
        {date, equity_twd, flow_twd, period_return, cum_twr, wealth_index}
    """
    if not equity_series:
        return []

    flow_by_date: dict[str, float] = {f["date"]: float(f["flow_twd"]) for f in flow_series}
    out: list[dict] = []

    # Day 1: no prior equity. Force r_1 = 0; wealth_index seeded with anchor.
    first = equity_series[0]
    running_wealth = 1.0 + anchor_cum_return
    out.append({
        "date": first["date"],
        "equity_twd": float(first["equity_twd"]),
        "flow_twd": flow_by_date.get(first["date"], 0.0),
        "period_return": 0.0,
        "cum_twr": running_wealth - 1.0,
        "wealth_index": running_wealth,
    })

    for i in range(1, len(equity_series)):
        curr = equity_series[i]
        v_start = float(equity_series[i - 1]["equity_twd"])
        v_end = float(curr["equity_twd"])
        f_d = flow_by_date.get(curr["date"], 0.0)
        denom = v_start + weight * f_d
        r_d = (v_end - v_start - f_d) / denom if abs(denom) > 1e-6 else 0.0
        running_wealth *= 1.0 + r_d
        out.append({
            "date": curr["date"],
            "equity_twd": v_end,
            "flow_twd": f_d,
            "period_return": r_d,
            "cum_twr": running_wealth - 1.0,
            "wealth_index": running_wealth,
        })

    return out


def _weighted_flow_sum(month: dict, days: int, f_total: float, method: PeriodMethod) -> float:
    if method == "mid_month":
        return 0.5 * f_total
    if method == "eom":
        return 0.0
    # day_weighted — sum of per-trade Wᵢ·Fᵢ. Falls back to mid-month if
    # no per-trade dates (would only happen for a stub month).
    events = _trade_flow_events(month)
    if not events:
        return 0.5 * f_total
    wf = 0.0
    for day, f_i in events:
        w = max(0.0, (days - day) / days)
        wf += w * f_i
    return wf


# ────────────────────────────────────────────────────────────────────────────
# Time-series stats
# ────────────────────────────────────────────────────────────────────────────


def cumulative_curve(period_returns: Iterable[float]) -> list[float]:
    """Compound monthly returns into a cumulative-return curve starting at 0."""
    cum = []
    running = 1.0
    for r in period_returns:
        running *= 1.0 + (r or 0)
        cum.append(running - 1.0)
    return cum


def drawdown_curve(cum_returns: Iterable[float]) -> list[dict]:
    """For each point, return current drawdown from running peak.

    Wires through `drawdown.underwater_curve` while preserving legacy
    semantics: peak starts at 1.0 (i.e. the implicit pre-period
    starting wealth), so a first-period loss reports `dd = cum[0]`
    rather than the principled module's first-observation `dd = 0`.
    Implemented by prepending a synthetic 1.0 to the equity series.
    """
    cum_list = list(cum_returns)
    equity = [Decimal("1")] + [Decimal(str(1.0 + (r or 0))) for r in cum_list]
    dds = _drawdown.underwater_curve(equity)
    return [
        {"wealth": float(equity[i + 1]), "drawdown": float(dds[i + 1])}
        for i in range(len(cum_list))
    ]


def max_drawdown(cum_returns: list[float]) -> float:
    """Worst peak-to-trough decline. Wires through
    `drawdown.max_drawdown` with the same prepend-1.0 trick used in
    drawdown_curve to preserve legacy semantics."""
    if not cum_returns:
        return 0.0
    equity = [Decimal("1")] + [Decimal(str(1.0 + (r or 0))) for r in cum_returns]
    return float(_drawdown.max_drawdown(equity))


def drawdown_episodes(cum_returns: list[float], months: list[str] | None = None) -> list[dict]:
    """Identify each drawdown episode: peak, trough, depth, duration, recovery.

    A drawdown episode is the path from a new running peak down to its lowest
    trough and back up to the peak (or the present if not yet recovered).
    Returns list sorted by depth (most severe first).
    """
    if not cum_returns:
        return []
    months = months or [str(i) for i in range(len(cum_returns))]
    wealth = [1.0 + (r or 0) for r in cum_returns]

    episodes: list[dict] = []
    peak = wealth[0]
    peak_i = 0
    in_dd = False
    trough = peak
    trough_i = peak_i

    for i, w in enumerate(wealth):
        if w >= peak:
            if in_dd:
                episodes.append(_episode(peak, peak_i, trough, trough_i, w, i, months, recovered=True))
                in_dd = False
            peak = w
            peak_i = i
            trough = w
            trough_i = i
        else:
            if not in_dd:
                in_dd = True
                trough = w
                trough_i = i
            elif w < trough:
                trough = w
                trough_i = i

    if in_dd:
        episodes.append(_episode(peak, peak_i, trough, trough_i, wealth[-1], len(wealth) - 1, months, recovered=False))

    episodes.sort(key=lambda e: e["depth_pct"])
    return episodes


def _episode(peak, peak_i, trough, trough_i, last_w, last_i, months, recovered: bool) -> dict:
    return {
        "peak_month": months[peak_i],
        "peak_wealth": peak,
        "trough_month": months[trough_i],
        "trough_wealth": trough,
        "depth_pct": (trough - peak) / peak if peak else 0.0,
        "duration_months": last_i - peak_i,
        "drawdown_months": trough_i - peak_i,
        "recovery_months": (last_i - trough_i) if recovered else None,
        "recovered": recovered,
    }


def stdev(values: list[float]) -> float:
    """Sample stdev — delegates to ratios.stdev (Decimal)."""
    return float(_ratios.stdev(_decimals(values)))


def downside_stdev(values: list[float], target: float = 0.0) -> float:
    """Sample stdev of below-target observations — delegates to
    ratios.downside_stdev. NOTE: divides by (n − 1) using full series
    count (Sortino convention), not population stdev of negatives.
    Differs numerically from the legacy formula."""
    return float(_ratios.downside_stdev(_decimals(values), Decimal(str(target))))


def annualize_return(period_return: float, periods: int = 12) -> float:
    return (1.0 + period_return) ** periods - 1.0


def cagr_from_cum(cum_return: float, periods: int, periods_per_year: int = 12) -> float:
    """Compound Annual Growth Rate from a cumulative return over N periods."""
    if periods <= 0:
        return 0.0
    years = periods / periods_per_year
    if 1.0 + cum_return <= 0:
        return -1.0
    return (1.0 + cum_return) ** (1.0 / years) - 1.0


def sharpe(period_returns: list[float], rf_period: float = 0.0, periods_per_year: int = 12) -> float:
    """Annualized Sharpe — delegates to ratios.sharpe.

    Legacy `rf_period` is the per-period risk-free rate; `ratios.sharpe`
    accepts an annualized `risk_free` and divides internally by
    `periods_per_year`. We multiply at the boundary to match.
    """
    risk_free_annual = Decimal(str(rf_period)) * Decimal(periods_per_year)
    return float(_ratios.sharpe(_decimals(period_returns), risk_free_annual, periods_per_year))


def sortino(period_returns: list[float], rf_period: float = 0.0, periods_per_year: int = 12) -> float:
    """Annualized Sortino — delegates to ratios.sortino. Sample-stdev
    convention (see downside_stdev note)."""
    risk_free_annual = Decimal(str(rf_period)) * Decimal(periods_per_year)
    return float(_ratios.sortino(_decimals(period_returns), risk_free_annual, periods_per_year))


def calmar(period_returns: list[float], periods_per_year: int = 12) -> float:
    """CAGR divided by |max drawdown| — delegates to ratios.calmar."""
    return float(_ratios.calmar(_decimals(period_returns), periods_per_year))


def rolling_returns(period_returns: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(period_returns)):
        if i + 1 < window:
            out.append(None)
            continue
        slice_ = period_returns[i + 1 - window : i + 1]
        running = 1.0
        for r in slice_:
            running *= 1.0 + (r or 0)
        out.append(running - 1.0)
    return out


def rolling_sharpe(period_returns: list[float], window: int, periods_per_year: int = 12) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(period_returns)):
        if i + 1 < window:
            out.append(None)
            continue
        out.append(sharpe(period_returns[i + 1 - window : i + 1], periods_per_year=periods_per_year))
    return out


# ────────────────────────────────────────────────────────────────────────────
# Concentration / risk
# ────────────────────────────────────────────────────────────────────────────


def hhi(weights: Iterable[float]) -> float:
    """HHI — delegates to concentration.hhi.

    Caller must pass pre-normalized weights (sum ≈ 1). The principled
    function normalizes internally; with already-normalized input the
    extra normalization is a no-op and the result matches the legacy
    sum-of-squares formula.
    """
    return float(_concentration.hhi(_decimals(weights)))


def top_n_share(weights: list[float], n: int) -> float:
    """Top-n share — delegates to concentration.top_n_share. Same
    pre-normalization caveat as hhi."""
    return float(_concentration.top_n_share(_decimals(weights), n))


def effective_n(weights: Iterable[float]) -> float:
    """Effective number of holdings = 1 / HHI."""
    h = hhi(weights)
    return (1.0 / h) if h > 1e-9 else 0.0


# ────────────────────────────────────────────────────────────────────────────
# P&L attribution
# ────────────────────────────────────────────────────────────────────────────


def realized_pnl_by_ticker(by_ticker: dict) -> list[dict]:
    """Average-cost realized P&L. Matches Sinopac's reporting convention."""
    out = []
    for code, t in by_ticker.items():
        buy_qty = t.get("buy_qty", 0) or 0
        sell_qty = t.get("sell_qty", 0) or 0
        buy_cost = t.get("buy_cost_twd", 0) or 0
        sell_proceeds = t.get("sell_proceeds_twd", 0) or 0
        fees = t.get("fees_twd", 0) or 0
        tax = t.get("tax_twd", 0) or 0
        divs = t.get("dividends_twd", 0) or 0

        avg_buy = (buy_cost / buy_qty) if buy_qty else 0
        cost_of_sold = avg_buy * sell_qty
        realized = sell_proceeds - cost_of_sold - fees - tax

        out.append({
            "code": code,
            "name": t.get("name"),
            "venue": t.get("venue"),
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_cost_twd": buy_cost,
            "sell_proceeds_twd": sell_proceeds,
            "fees_twd": fees,
            "tax_twd": tax,
            "dividends_twd": divs,
            "avg_buy_price_twd": avg_buy,
            "cost_of_sold_twd": cost_of_sold,
            "realized_pnl_twd": realized,
            "realized_pnl_with_div_twd": realized + divs,
            "realized_pnl_pct": (realized / cost_of_sold) if cost_of_sold else None,
            "fully_closed": sell_qty >= buy_qty if buy_qty else False,
        })
    out.sort(key=lambda r: r["realized_pnl_twd"], reverse=True)
    return out


def _venue_for_legacy(raw: Any) -> Venue:
    """Map legacy by_ticker `venue` (str/None) to the Venue enum.
    Unknown / missing values fall back to TW (default for the
    Taiwan-statement code path)."""
    if raw == "US":
        return Venue.US
    if raw == "HK":
        return Venue.HK
    if raw == "JP":
        return Venue.JP
    return Venue.TW


def _trades_from_legacy_ticker(code: str, ticker: dict) -> list[Trade]:
    """Synthesize Trade VOs from a legacy by_ticker entry.

    Per-share price embeds fees/taxes the way the legacy FIFO walk did:
        buy  : px = (gross + fee + tax) / qty
        sell : px = (gross − fee − tax) / qty
    All amounts are TWD (legacy `*_twd` fields). Margin and short sides
    ("資買" / "資賣") are coerced to CASH_* — the legacy walk treated
    them identically for P&L bookkeeping; preserving that semantic here
    avoids a NotImplementedError from Position.apply on margin holdings.
    Trades with non-positive qty or non-buy/non-sell side (e.g.
    "股利") are skipped, matching legacy.
    """
    venue = _venue_for_legacy(ticker.get("venue"))
    out: list[Trade] = []
    for tr in ticker.get("trades") or []:
        qty_raw = tr.get("qty", 0) or 0
        if qty_raw <= 0:
            continue
        side_str = tr.get("side") or ""
        is_buy = "買" in side_str
        is_sell = ("賣" in side_str) and (side_str != "股利")
        if not (is_buy or is_sell):
            continue
        qty = int(round(qty_raw))
        gross = Decimal(str(tr.get("gross_twd", 0) or 0))
        fee = Decimal(str(tr.get("fee_twd", 0) or 0))
        tax = Decimal(str(tr.get("tax_twd", 0) or 0))
        per_share = (gross + fee + tax) / Decimal(qty) if is_buy else (gross - fee - tax) / Decimal(qty)
        out.append(Trade(
            date=_parse_iso_or_slash(tr.get("date")),
            code=code,
            side=Side.CASH_BUY if is_buy else Side.CASH_SELL,
            qty=qty,
            price=Money(per_share, "TWD"),
            venue=venue,
        ))
    return out


def realized_pnl_by_ticker_fifo(by_ticker: dict) -> list[dict]:
    """FIFO realized P&L per ticker — wires through
    `tax_pnl.realized_stats_per_position`.

    Synthesizes Trade VOs from the legacy by_ticker dict, runs the
    principled FIFO walk + bookkeeping, and flattens RealizedStats into
    the dashboard's 16-key shape (merging back per-ticker `name`,
    `venue`, `dividends_twd` which Trade VOs don't carry).
    """
    out: list[dict] = []
    for code, t in by_ticker.items():
        trades = _trades_from_legacy_ticker(code, t)
        stats_map = _tax_pnl.realized_stats_per_position(trades)
        stats = stats_map.get(code)

        if stats is None:
            # Ticker had only dividends or skipped sides — no FIFO matches.
            out.append({
                "code": code,
                "name": t.get("name"),
                "venue": t.get("venue"),
                "realized_pnl_twd": 0.0,
                "sell_proceeds_twd": 0.0,
                "cost_of_sold_twd": 0.0,
                "sell_qty": 0.0,
                "open_qty": 0,
                "open_cost_twd": 0.0,
                "avg_open_cost_twd": None,
                "wins": 0,
                "losses": 0,
                "win_rate": None,
                "profit_factor": None,
                "avg_holding_days": None,
                "dividends_twd": t.get("dividends_twd", 0) or 0,
                "fully_closed": False,
            })
            continue

        out.append({
            "code": code,
            "name": t.get("name"),
            "venue": t.get("venue"),
            "realized_pnl_twd": float(stats.realized_pnl.amount),
            "sell_proceeds_twd": float(stats.sell_proceeds.amount),
            "cost_of_sold_twd": float(stats.cost_of_sold.amount),
            "sell_qty": float(stats.sell_qty),
            "open_qty": stats.open_qty,
            "open_cost_twd": float(stats.open_cost.amount),
            "avg_open_cost_twd": (
                float(stats.avg_open_cost.amount)
                if stats.avg_open_cost is not None else None
            ),
            "wins": stats.wins,
            "losses": stats.losses,
            "win_rate": stats.win_rate,
            "profit_factor": (
                float(stats.profit_factor) if stats.profit_factor is not None else None
            ),
            "avg_holding_days": stats.avg_holding_days,
            "dividends_twd": t.get("dividends_twd", 0) or 0,
            "fully_closed": stats.open_qty == 0,
        })
    out.sort(key=lambda r: r["realized_pnl_twd"], reverse=True)
    return out


def _parse_iso_or_slash(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ────────────────────────────────────────────────────────────────────────────
# Cashflow analysis
# ────────────────────────────────────────────────────────────────────────────


def monthly_flows(months: list[dict], venue_flows: list[dict] | None = None) -> list[dict]:
    """Per-month inflow/outflow breakdown for the cashflow waterfall.

    Pulls TW vs Foreign buy/sell from `venue_flows_twd` (broker-trade ground
    truth) and bank-side categorization from `investment_flows_twd`. Also
    derives three "view" totals so the UI can disambiguate what the user
    means by "net flow":

      * gross_in / gross_out — sum of bank movements tied to broker activity.
      * external_flow         — net broker↔bank cashflow (legacy).
      * deposits_net          — peer transfers + salary (capital actively
                                 moved IN from outside the investing system).
    """
    venue_by_month = {v["month"]: v for v in (venue_flows or [])}
    out = []
    for m in months:
        flows = m.get("investment_flows_twd", {}) or {}
        vf = venue_by_month.get(m["month"], {})
        bank = m.get("bank", {}) or {}

        gross_in = 0.0
        gross_out = 0.0
        for tx in bank.get("tx_twd", []) or []:
            cat = tx.get("category") or ""
            sgn = tx.get("signed_amount", 0) or 0
            if cat in ("stock_settle_tw", "rebate", "tw_dividend", "fx_convert"):
                if sgn > 0:
                    gross_in += sgn
                else:
                    gross_out += -sgn

        usd_rate = (bank.get("fx") or {}).get("USD") or m.get("fx_usd_twd") or 0.0
        for tx in bank.get("tx_foreign", []) or []:
            cat = tx.get("category") or ""
            sgn = tx.get("signed_amount", 0) or 0
            if cat in ("stock_settle_fx", "foreign_dividend", "fx_convert"):
                amt_twd = sgn * usd_rate
                if amt_twd > 0:
                    gross_in += amt_twd
                else:
                    gross_out += -amt_twd

        out.append({
            "month": m["month"],
            "external_flow": m.get("external_flow_twd", 0),
            "gross_in": gross_in,
            "gross_out": gross_out,
            "deposits_net": (
                (flows.get("transfer_net_twd", 0) or 0)
                + (flows.get("salary_in_twd", 0) or 0)
            ),
            # Venue split (broker-side, ground truth):
            "tw_buy": vf.get("tw_buy_twd", 0),
            "tw_sell": vf.get("tw_sell_twd", 0),
            "tw_fee": vf.get("tw_fee_twd", 0),
            "tw_tax": vf.get("tw_tax_twd", 0),
            "foreign_buy": vf.get("foreign_buy_twd", 0),
            "foreign_sell": vf.get("foreign_sell_twd", 0),
            "foreign_fee": vf.get("foreign_fee_twd", 0),
            # Bank-side (the path the cash actually took):
            "stock_buy_bank": flows.get("stock_buy_twd", 0),
            "stock_sell_bank": flows.get("stock_sell_twd", 0),
            "tw_dividend_in": flows.get("tw_dividend_in_twd", 0),
            "rebate_in": flows.get("rebate_in_twd", 0),
            "fx_to_usd": flows.get("fx_to_usd_twd", 0),
            "fx_to_twd": flows.get("fx_to_twd_twd", 0),
            "salary_in": flows.get("salary_in_twd", 0),
            "interest_in": flows.get("interest_in_twd", 0),
            "transfer_net": flows.get("transfer_net_twd", 0),
        })
    return out


# ────────────────────────────────────────────────────────────────────────────
# FX
# ────────────────────────────────────────────────────────────────────────────


def reprice_holdings_with_daily(
    holdings: list[dict],
    get_latest_close: Callable[[str], dict[str, Any] | None],
    current_fx_usd_twd: float | None = None,
) -> list[dict]:
    """Override each holding's ref_price/mv/unrealized with today's close.

    Source-of-truth fields preserved: code, name, qty, avg_cost, cost_*.
    Mutated fields: ref_price, mkt_value_local, mkt_value_twd,
    unrealized_pnl_local, unrealized_pnl_twd, unrealized_pct, repriced_at.

    Tickers with no daily price (e.g. delisted, very thinly-traded) keep
    their month-end values — graceful per-ticker fallback. The function
    is order-preserving and side-effect free (returns new list of new
    dicts).

    Args:
        holdings: rows from `_holdings_for_month(last)` (unified shape).
        get_latest_close: callable(symbol) -> {date, close, currency} or
            None. Pass `daily_store.get_latest_close` directly.
        current_fx_usd_twd: latest USD/TWD rate to convert foreign
            positions. Falls back to the holding's stale fx if None.
    """
    out: list[dict] = []
    for h in holdings:
        code = h.get("code")
        new = dict(h)
        if not code:
            out.append(new)
            continue
        latest = get_latest_close(code)
        if not latest:
            # Foreign holdings sometimes use ".TW" / ".HK" suffixes — daily
            # store keys foreign tickers as their bare Yahoo symbol.
            out.append(new)
            continue
        close = float(latest["close"])
        qty = float(h.get("qty") or 0)
        cost_local = float(h.get("cost_local") or h.get("cost_twd") or 0)
        new["ref_price"] = close
        venue = h.get("venue")
        if venue == "TW":
            mv_local = qty * close
            new["mkt_value_local"] = mv_local
            new["mkt_value_twd"] = mv_local
            upnl = mv_local - cost_local
            new["unrealized_pnl_local"] = upnl
            new["unrealized_pnl_twd"] = upnl
            new["unrealized_pct"] = (upnl / cost_local) if cost_local else 0
        else:  # Foreign — apply current FX
            mv_local = qty * close
            ccy = h.get("ccy") or "USD"
            rate = current_fx_usd_twd if ccy == "USD" and current_fx_usd_twd else 1.0
            new["mkt_value_local"] = mv_local
            new["mkt_value_twd"] = mv_local * rate
            upnl_local = mv_local - cost_local
            new["unrealized_pnl_local"] = upnl_local
            new["unrealized_pnl_twd"] = upnl_local * rate
            new["unrealized_pct"] = (upnl_local / cost_local) if cost_local else 0
        new["repriced_at"] = latest["date"]
        out.append(new)
    return out


def daily_fx_pnl(
    usd_exposure_series: list[dict],
    fx_series: list[dict],
) -> dict:
    """Day-over-day FX P&L using daily rates and daily USD exposure.

    For each priced day d (after the first):
        usd_amount_{d-1} = usd_mv_twd_{d-1} / rate_{d-1}
        fx_pnl_d         = usd_amount_{d-1} × (rate_d − rate_{d-1})

    USD cash from the bank statement is monthly-only and NOT included
    here — the daily layer tracks foreign equity exposure only. The
    /api/fx response calls this out via fx_pnl_resolution: "daily".

    Returns same shape as fx_pnl(): {contribution_twd, daily: [...]}.
    """
    if len(usd_exposure_series) < 2 or len(fx_series) < 2:
        return {"contribution_twd": 0, "daily": []}

    rate_by_date = {r["date"]: float(r["rate_to_twd"]) for r in fx_series}
    usd_by_date = {r["date"]: float(r["usd_mv_twd"]) for r in usd_exposure_series}
    dates = sorted(set(rate_by_date) & set(usd_by_date))
    if len(dates) < 2:
        return {"contribution_twd": 0, "daily": []}

    cumulative = 0.0
    daily = []
    for i in range(1, len(dates)):
        prev_d, curr_d = dates[i - 1], dates[i]
        prev_rate, curr_rate = rate_by_date[prev_d], rate_by_date[curr_d]
        prev_mv_twd = usd_by_date[prev_d]
        usd_amount = prev_mv_twd / prev_rate if prev_rate else 0.0
        delta_twd = usd_amount * (curr_rate - prev_rate)
        cumulative += delta_twd
        daily.append({
            "date": curr_d,
            "fx_usd_twd": curr_rate,
            "usd_amount": usd_amount,
            "fx_pnl_twd": delta_twd,
            "cumulative_fx_pnl_twd": cumulative,
        })
    return {"contribution_twd": cumulative, "daily": daily}


def fx_pnl(months: list[dict]) -> dict:
    """USD/TWD FX P&L on full USD exposure — delegates to
    `attribution.usd_exposure_walk`. Whole-portfolio sequential walk:
    one bar per month for the dashboard's /fx page."""
    return _attribution.usd_exposure_walk(months)


# ────────────────────────────────────────────────────────────────────────────
# Activity / movers
# ────────────────────────────────────────────────────────────────────────────


def top_movers(by_ticker: dict, latest_holdings: list[dict], top_n: int = 5) -> dict:
    enriched = []
    for h in latest_holdings:
        enriched.append({
            "code": h.get("code"),
            "name": h.get("name"),
            "venue": h.get("venue"),
            "unrealized_pnl_twd": h.get("unrealized_pnl_twd", 0),
            "unrealized_pct": h.get("unrealized_pct", 0),
            "mkt_value_twd": h.get("mkt_value_twd", 0),
        })
    enriched.sort(key=lambda r: r["unrealized_pnl_twd"], reverse=True)
    winners = enriched[:top_n]
    losers = enriched[-top_n:][::-1]
    return {"winners": winners, "losers": losers}


def recent_activity(all_trades: list[dict], limit: int = 25) -> list[dict]:
    def sort_key(t):
        try:
            return datetime.strptime(t.get("date", "1970/01/01"), "%Y/%m/%d")
        except ValueError:
            return datetime(1970, 1, 1)
    sorted_trades = sorted(all_trades, key=sort_key, reverse=True)
    return sorted_trades[:limit]


# ────────────────────────────────────────────────────────────────────────────
# Sector mapping (delegates to sectors module)
# ────────────────────────────────────────────────────────────────────────────


def sector_of(code: str, venue: str) -> str:
    """Delegates to sectors.sector_of."""
    return _sectors.sector_of(code, venue)


def sector_breakdown(holdings: list[dict]) -> list[dict]:
    """Delegates to sectors.sector_breakdown."""
    return _sectors.sector_breakdown(holdings)
def month_end_iso(yyyy_mm: str) -> str:
    """'2025-02' → '2025-02-28' (handles leap years)."""
    y, m = (int(p) for p in yyyy_mm.split("-"))
    last_day = monthrange(y, m)[1]
    return f"{y:04d}-{m:02d}-{last_day:02d}"
def anchor_for_daily(
    months: list[dict], monthly_cum: list[float], first_daily_date: str,
) -> float:
    """Monthly cum_twr at the last month-end strictly before first_daily_date.
    Lets the daily TWR chain pick up where the monthly chain left off so a
    portfolio that started before the daily backfill floor doesn't see its
    cumulative reset to 0% on the daily chart. Returns 0.0 when the daily
    window starts before any monthly data (no prior history).
    """
    first_ym = first_daily_date[:7]
    anchor = 0.0
    for i, m in enumerate(months):
        if m["month"] >= first_ym:
            break
        if i < len(monthly_cum):
            anchor = monthly_cum[i]
    return anchor
def monthly_anchored_cum(
    daily_dates: list[str],
    months: list[dict],
    monthly_cum: list[float],
) -> list[float]:
    """Linearly interpolate monthly cum_twr at month-end onto daily dates.
    Daily Modified-Dietz on noisy stocks-only V can drift from the monthly
    chain — TWSE has gaps for some symbols, splits inflate intra-month days,
    and dividends paid into bank look like outflows on the broker side.
    Forcing the chart line through the monthly anchors keeps the chart
    consistent with the headline TWR KPI while equity_twd keeps daily
    resolution.
    For dates past the latest month-end, the line stays flat at the last
    monthly cum — matches Overview's behavior of holding TWR steady until
    the next PDF arrives, even though daily V keeps moving.
    """
    if not months:
        return [0.0] * len(daily_dates)
    anchors: list[tuple[str, float]] = []
    for i, m in enumerate(months):
        if i < len(monthly_cum):
            anchors.append((month_end_iso(m["month"]), monthly_cum[i]))
    anchors.sort()
    if not anchors:
        return [0.0] * len(daily_dates)
    out: list[float] = []
    for d in daily_dates:
        prior = None
        nxt = None
        for a in anchors:
            if a[0] <= d:
                prior = a
            else:
                nxt = a
                break
        if prior is None and nxt is not None:
            d_dt = date.fromisoformat(d)
            n_dt = date.fromisoformat(nxt[0])
            base_dt = date.fromisoformat(daily_dates[0])
            span = max(1, (n_dt - base_dt).days)
            frac = max(0.0, (d_dt - base_dt).days / span)
            out.append(nxt[1] * frac)
        elif prior is not None and nxt is None:
            out.append(prior[1])
        elif prior is not None and nxt is not None:
            d_dt = date.fromisoformat(d)
            p_dt = date.fromisoformat(prior[0])
            n_dt = date.fromisoformat(nxt[0])
            span = max(1, (n_dt - p_dt).days)
            frac = (d_dt - p_dt).days / span
            out.append(prior[1] + (nxt[1] - prior[1]) * frac)
        else:
            out.append(0.0)
    return out
def bank_cash_forward_fill(months: list[dict]):
    """Closure that returns forward-filled bank cash (TWD) for any ISO date.
    Bank cash is monthly-only (parser knows month-end balance). For days
    inside a month we don't have a daily balance, so we carry the most-
    recent month-end forward.
    """
    bank_by_month = {
        m["month"]: (m.get("bank_twd", 0) or 0) + (m.get("bank_usd_in_twd", 0) or 0)
        for m in months
    }
    sorted_months = sorted(bank_by_month.keys())
    def lookup(date_iso: str) -> float:
        ym = date_iso[:7]
        if ym in bank_by_month:
            return bank_by_month[ym]
        prior = [m for m in sorted_months if m <= ym]
        return bank_by_month[prior[-1]] if prior else 0.0
    return lookup
